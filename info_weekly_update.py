# -*- coding: utf-8 -*-
"""
信息模块周增量更新 — info_archive.db (公告/财务摘要/董监高增减持)
(信息模块 Phase1 待办⑤, 2026-07-05)

由 run_weekly_update.sh 作为 Step 6 调用, 也可手动跑。全部写入均为
INSERT OR IGNORE / 按股票整体 REPLACE, 只增不删, 随时可重跑。

三段增量策略:
  A. 公告 announcements — 巨潮全市场查询(不传 stock 参数), **逐日**抓取,
     窗口 = 游标 info_meta.ann_last_ok 往前 3 天 ~ 今天, (code, ann_id) 唯一键去重。
     ⚠️必须逐日: 该接口第 101 页起静默回吐第 1 页(详见常量区 ANN_SOURCE_PAGE_LIMIT
     注释, 2026-08-08 定位), 一次查一整周必然被截断在 3000 条。
     单日超 3000 条(年报季)再按板块 ANN_PLATES 细分; 每天用接口自报 totalAnnouncement
     校验实际抓取数, 不达标即报错, 且游标不越过该日 → 下次运行自动重试。
  B. 财务摘要 fin_abstract — 公告驱动: 只重拉窗口内披露过
     定期报告/业绩预告(含业绩快报)的股票, 新浪接口一次返回该股全历史,
     整股 INSERT OR REPLACE(顺带覆盖财务重述)。窗口起点存 info_meta.fin_last_ok。
  C. 增减持 holder_changes — sse/bse 用接口原生日期窗口参数;
     szse 无日期参数, 按最新在前从第 1 页翻, 整页全重复即停(row_key 去重),
     且失败只警告跳过(129 IP 曾被深交所掐, 不能炸整个周更)。

用法:
  venv/bin/python info_weekly_update.py                 # 全部三段
  venv/bin/python info_weekly_update.py --sections ann  # 只跑指定段 ann/fin/holder
  # 补历史公告(显式模式, 不动游标):
  venv/bin/python info_weekly_update.py --sections ann --ann-days 2026-07-05,2026-07-06
  venv/bin/python info_weekly_update.py --sections ann --ann-start 2026-07-05 --ann-end 2026-07-08
  # 只抓不写, 验证完整性:
  venv/bin/python info_weekly_update.py --sections ann --ann-days 2026-04-29 --dry-run
  # 补跑财务摘要(显式模式, 不动游标; 用于公告补完后重扫/救回失败股):
  venv/bin/python info_weekly_update.py --sections fin --fin-since 2026-07-06
"""
import argparse
import datetime as dt
import os
import random
import re
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import info_backfill_announcements as ann_mod
import info_backfill_fin_abstract as fin_mod
import info_backfill_holder_changes as hc_mod

import requests

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "astock_data", "info_archive.db")

ANN_OVERLAP_DAYS = 3      # 公告窗口重叠, 防上轮收尾当天漏条
HOLDER_OVERLAP_DAYS = 7   # sse/bse 日期窗口重叠(披露有延迟)
FIN_DEFAULT_LOOKBACK = 14 # info_meta 无记录时财报驱动回看天数

# ⚠️ fin 段(新浪 quotes.sina.cn JSON)防封节奏 —— 照搬 download_astock_data.py 中
#   step4/step5 于 2026-04-20 事故(0.4s 节奏致 4699/5017 失败)后定下并实测零失败的方案。
#   fin 原先裸奔 0.35~0.6s, 2026-08-08 夜在 ~98 只后触发 456 封 IP(err=50),
#   且封禁持续 10h+ 呈间歇惩罚态 → 与 step4 同款 JSON 接口, 必须同款节奏。
FIN_INTERVAL_MIN = 6.0
FIN_INTERVAL_MAX = 9.0
FIN_BATCH_PAUSE_EVERY = 50    # 每 50 只停 60s
FIN_BATCH_PAUSE = 60
FIN_LONG_PAUSE_EVERY = 200    # 每 200 只长歇 120~240s
FIN_LONG_PAUSE_MIN = 120
FIN_LONG_PAUSE_MAX = 240
FIN_MAX_TRIES = 2             # 失败快弃(退避链 5 次共 200s 反而持续撞封禁)
FIN_STOP_CONSECUTIVE = 10     # 连续失败熔断: 中止本段, 不推游标 → 下轮自动重试
FIN_BAD_RATE_WINDOW = 30      # 滑窗失败率 >30% → 冷却 600s
FIN_BAD_RATE_THRESHOLD = 0.30
FIN_COOL_DOWN = 600
SZSE_PAGE_CAP = 60        # szse 逐页翻安全上限(正常一周 ~15 页)

# ⚠️⚠️ 巨潮全市场查询(不传 stock)有深翻页硬顶: 第 101 页起静默回吐第 1 页的内容 ——
#   不报错、不返回空、也不改 totalpages/hasMore(照常自报真实总量)。
#   2026-08-08 实证: 窗口 7-22~8-02 自报 totalAnnouncement=11272 / totalpages=375 /
#   hasMore=true, 但 p101 / p102 / p110 / p130 的返回与 p1 字节级一致, 连拉 3 次稳定复现。
#   旧逻辑"连续 30 页零新增即停"恰好被这种回滚完美骗过(回吐的第 1 页本轮开头刚入过库,
#   全是重复 → 凑满 30 页 → 打印 FINISHED), 于是每周只捞回窗口最新 3 天,
#   自 2026-07-05 起静默丢掉 16 个交易日。
#   修法三条: ①按天查(单日 400~1500 条, 远低于上限) ②单日超限再按板块细分
#   ③每天用接口自报总数校验实际抓取数, 对不上就报错而不是装作成功。
ANN_PAGE_ROWS = 30        # 每页硬顶 30 条(传大于 30 会跳条漏数据, 7-2 实证)
ANN_SOURCE_PAGE_LIMIT = 100                              # 源头深翻页硬顶
ANN_BUCKET_CAP = ANN_SOURCE_PAGE_LIMIT * ANN_PAGE_ROWS   # 3000 = 单个查询格子可达上限
ANN_PLATES = ("shmb", "shkcp", "szmb", "szzx", "szcy", "bj")
# ↑ 叶子板块。2026-08-08 实证 Σ叶子 == 全市场总数(取 4-29 极端日: sh 7595 + sz 17599 +
#   bj 872 = 26066 精确吻合; shmb+shkcp==sh, szmb+szzx+szcy==sz)。
#   column 参数实测被服务端忽略(sse/szse/bj 返回完全相同), 不能当细分维度。
ANN_MAX_DAYS_PER_RUN = 45 # 单次运行最多补的天数; 超出部分不丢, 游标只推进到已完成处

CODE_RE = re.compile(r"^\d{6}$")

DRY_RUN = False           # --dry-run: 只抓不写, 用于验证抓取完整性


def init_meta(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS info_meta(key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()


def meta_get(conn, key, default=None):
    row = conn.execute("SELECT value FROM info_meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def meta_set(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO info_meta VALUES(?,?)", (key, value))
    conn.commit()


# ---------- A. 公告增量 (巨潮全市场, 逐日 + 板块细分 + 总数校验) ----------

def _ann_payload(day, page, plate="", sort_type=""):
    return {
        "pageNum": str(page),
        "pageSize": str(ANN_PAGE_ROWS),
        "column": "szse",          # 实测被服务端忽略, 保留原值
        "tabName": "fulltext",
        "plate": plate,            # 空 = 全市场; 叶子板块见 ANN_PLATES
        "stock": "",
        "searchkey": "",
        "secid": "",
        "category": "",
        "trade": "",
        "seDate": f"{day}~{day}",
        "sortName": "time" if sort_type else "",
        "sortType": sort_type,
        "isHLtitle": "false",
    }


def _ann_save_page(conn, items, seen):
    """写一页。seen 收本日全部去重后的 (code, ann_id), 用于和接口自报总数对账。
    返回本页新增行数。"""
    inserted = 0
    for item in items:
        ann_id = str(item.get("announcementId") or "")
        code = str(item.get("secCode") or "").strip()
        if not ann_id:
            continue
        key = (code, ann_id)
        if key in seen:
            continue
        seen.add(key)
        if DRY_RUN:
            continue
        if not CODE_RE.match(code):
            continue
        title = (item.get("announcementTitle") or "").strip()
        if not title:
            continue
        try:
            pub = dt.datetime.fromtimestamp(
                int(item.get("announcementTime")) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        org_id = item.get("orgId") or ""
        url = (f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode={code}"
               f"&announcementId={ann_id}&orgId={org_id}&announcementTime={pub[:10]}")
        cat, impact = ann_mod.classify(title)
        cur = conn.execute(
            "INSERT OR IGNORE INTO announcements(code,name,title,publish_time,ann_id,url,category,impact) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (code, item.get("secName") or "", title, pub, ann_id, url, cat, impact))
        inserted += cur.rowcount
    conn.commit()
    return inserted


def _ann_bucket(conn, session, day, plate, sort_type, seen):
    """翻一个查询格子(某天 × 某板块 × 某排序)。
    ⚠️终止条件只认"本页返回 0 条"和"到达源头 100 页硬顶" —— hasMore 恒真、
    totalpages 虚报, 二者都不可信(见文件头常量区说明)。"""
    inserted = 0
    for page in range(1, ANN_SOURCE_PAGE_LIMIT + 1):
        j = ann_mod.post_with_retry(session, _ann_payload(day, page, plate, sort_type))
        items = j.get("announcements") or []
        if not items:
            break
        inserted += _ann_save_page(conn, items, seen)
    return inserted


def _ann_total(session, day, plate=""):
    j = ann_mod.post_with_retry(session, _ann_payload(day, 1, plate))
    return int(j.get("totalAnnouncement") or 0)


def _ann_per_stock_day(conn, session, day, seen):
    """兜底: 逐股查该日。逐股查询走 stock 参数, 不受全市场查询的深翻页硬顶影响
    (2011~2026 那 633 万条全量回填就是这么跑完的), 慢但可证完整。
    只在"按板块细分 + 正反双向"仍覆盖不住时触发 —— 历史上仅年报季少数几天。"""
    org_map = ann_mod.get_orgid_map(session)
    codes = sorted(c for c in org_map if CODE_RE.match(c))
    print(f"    [{day}] 逐股兜底: {len(codes)} 只 (约 {len(codes) * 0.35 / 60:.0f} 分钟)",
          flush=True)
    inserted, err = 0, 0
    for i, code in enumerate(codes, 1):
        try:
            items = ann_mod.fetch_stock_range(session, code, org_map[code], day, day)
        except Exception as e:
            err += 1
            print(f"      [{day} {code}] ERR {type(e).__name__}: {e}", flush=True)
            continue
        if items:
            inserted += _ann_save_page(conn, items, seen)
        if i % 1000 == 0:
            print(f"      [{day}] 逐股 {i}/{len(codes)} fetched={len(seen)}", flush=True)
    print(f"    [{day}] 逐股兜底完成 +{inserted} err={err}", flush=True)
    return inserted


def _ann_one_day(conn, session, day):
    """抓一天, 返回 (inserted, claimed, fetched, ok)。
    ok = 实际去重抓到的条数 >= 接口自报总数 —— 这是防"静默截断"复发的唯一保险。"""
    claimed = _ann_total(session, day)
    if claimed == 0:
        return 0, 0, 0, True

    seen = set()
    inserted = 0
    exhausted = False

    if claimed <= ANN_BUCKET_CAP:
        inserted += _ann_bucket(conn, session, day, "", "", seen)
    else:
        # 先探各板块总数, 决定细分够不够用 (探 6 次, 比闷头翻页便宜)
        subs = {pl: _ann_total(session, day, pl) for pl in ANN_PLATES}
        over = {pl: n for pl, n in subs.items() if n > ANN_BUCKET_CAP}
        print(f"  [ann {day}] claimed={claimed} > {ANN_BUCKET_CAP}, 板块分布 "
              f"{ {k: v for k, v in subs.items() if v} }", flush=True)
        if over:
            # ⚠️不要再试"时间正序+倒序各取 3000"分两半: 2026-08-08 压力样本(4-29)实测,
            #   叠加 plate 参数后 sortType 被服务端忽略, 正序拉回来的和倒序一模一样,
            #   多打 300 个请求换回 0 条新数据。超限就直接转逐股, 那才是可证完整的路。
            print(f"    [{day}] 板块 {list(over)} 仍超上限 → 直接转逐股兜底", flush=True)
            inserted += _ann_per_stock_day(conn, session, day, seen)
            exhausted = True
        else:
            for pl, n in subs.items():
                if n:
                    inserted += _ann_bucket(conn, session, day, pl, "", seen)

    fetched = len(seen)
    if fetched < claimed and not exhausted:
        print(f"  [ann {day}] 抓取数不足(缺 {claimed - fetched} 条) → 转逐股兜底", flush=True)
        inserted += _ann_per_stock_day(conn, session, day, seen)
        fetched = len(seen)
        exhausted = True

    # ok=False 会卡住游标下次重排, 所以只有"手段没用尽"才判失败;
    # 逐股兜底已是可证最完整的手段, 即便仍差几条(非股票发行人/已退市代码不在 org_map)
    # 也要放行游标, 但差额必须打出来, 绝不静默。
    ok = fetched >= claimed or exhausted
    tag = "OK" if fetched >= claimed else (
        f"⚠️逐股兜底后仍差 {claimed - fetched} 条(疑非股票发行人代码)"
        if exhausted else "❌ 不完整")
    print(f"  [ann {day}] claimed={claimed} fetched={fetched} +{inserted} {tag}", flush=True)
    return inserted, claimed, fetched, ok


def update_announcements(conn, days=None, start=None, end=None):
    """逐日抓取公告。

    默认窗口 = 游标 ann_last_ok(退化时用库内最新 publish_time)往前 ANN_OVERLAP_DAYS
    ~ 今天。游标只推进到"已校验完整"的那一天, 所以任何一天抓漏都会在下次运行
    自动重试, 不会像旧版那样把缺口永久留在库里。

    days/start/end 为人工补历史用(显式模式), 不读也不写游标。
    """
    manual = bool(days or start or end)
    today = dt.date.today()

    if days:
        day_list = sorted(dt.date.fromisoformat(d.strip()) for d in days)
    else:
        if start:
            d0 = dt.date.fromisoformat(start)
        else:
            cur = meta_get(conn, "ann_last_ok")
            if not cur:
                row = conn.execute("SELECT MAX(publish_time) FROM announcements").fetchone()
                cur = row[0][:10] if row and row[0] else None
            d0 = ((dt.date.fromisoformat(cur[:10]) - dt.timedelta(days=ANN_OVERLAP_DAYS))
                  if cur else today - dt.timedelta(days=FIN_DEFAULT_LOOKBACK))
        d1 = dt.date.fromisoformat(end) if end else today
        day_list = [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]

    if not day_list:
        print("[ann] 无待抓日期", flush=True)
        return 0
    truncated = 0
    if len(day_list) > ANN_MAX_DAYS_PER_RUN:
        truncated = len(day_list) - ANN_MAX_DAYS_PER_RUN
        day_list = day_list[:ANN_MAX_DAYS_PER_RUN]

    print(f"[ann] {len(day_list)} 天: {day_list[0]} ~ {day_list[-1]}"
          f"{' (manual)' if manual else ''}", flush=True)

    session = requests.Session()
    inserted, bad_days, last_ok = 0, [], None
    for d in day_list:
        ds = d.isoformat()
        try:
            n, _claimed, _fetched, ok = _ann_one_day(conn, session, ds)
            inserted += n
        except Exception as e:
            ok = False
            print(f"  [ann {ds}] ERROR {type(e).__name__}: {e}", flush=True)
        if ok:
            if not bad_days:
                last_ok = ds          # 游标只能推到第一个坏日之前
        else:
            bad_days.append(ds)

    if not manual and last_ok and not DRY_RUN:
        meta_set(conn, "ann_last_ok", last_ok)
    if truncated:
        print(f"[ann] WARN 还剩 {truncated} 天未补(单次上限 {ANN_MAX_DAYS_PER_RUN}), "
              f"游标已存, 下次运行自动接着补", flush=True)
    if bad_days:
        print(f"[ann] DONE inserted={inserted} ⚠️不完整: {','.join(bad_days)}", flush=True)
        raise RuntimeError(f"{len(bad_days)} 天抓取不完整(游标停在 {last_ok}, "
                           f"下次自动重试): {','.join(bad_days[:8])}")
    print(f"[ann] DONE days={len(day_list)} inserted={inserted} 逐日总数校验全部通过",
          flush=True)
    return inserted


# ---------- B. 财务摘要增量 (定期报告公告驱动) ----------

FIN_RESUME_FILE = os.path.join(BASE, "fin_resume_after.txt")


def _fin_note_resume(codes, i, consecutive_fail, after):
    """记录断点(最后一只成功股, 失败连串之前), 供 --fin-after 续跑。

    i 为 1-based 当前进度; 若从头就全失败则回退到本轮入参 after(整轮重来)。
    """
    idx = i - consecutive_fail - 1
    resume = codes[idx] if idx >= 0 else (after or "")
    with open(FIN_RESUME_FILE, "w") as f:
        f.write(resume + "\n")
    print(f"[fin] 断点已记录: 续跑用 --fin-after {resume}", flush=True)
    return resume


def update_fin_abstract(conn, since=None, sleep_s=0.0, after="", limit=0):
    """公告驱动: 重拉窗口内披露过定期报告/业绩预告的股票。

    since 为人工补跑用(显式模式), 不读也不写游标 fin_last_ok —— 用于公告补完后
    重扫漏掉的股票, 或救回某轮 err>0 但游标已推进的失败股。
    sleep_s: 在内置 FIN_* 防封节奏之上追加的逐股延时(秒), 一般不需要。
    after/limit 仅显式模式可用(CLI 层校验): after=断点续跑(跳过 code<=after),
    limit=本轮最多处理 N 只后优雅停; 到量或熔断时都会把断点写进
    fin_resume_after.txt, 下轮 --fin-after $(cat ...) 即可续。
    两级熔断(连续失败中止 / 滑窗失败率冷却)在正常与手动模式均生效;
    熔断中止时抛异常 → 不推游标, 下轮自动重试整个窗口。
    """
    manual = bool(since)
    if not manual:
        last_ok = meta_get(conn, "fin_last_ok")
        if last_ok:
            since = (dt.date.fromisoformat(last_ok[:10]) - dt.timedelta(days=1)).isoformat()
        else:
            since = (dt.date.today() - dt.timedelta(days=FIN_DEFAULT_LOOKBACK)).isoformat()
    codes = [r[0] for r in conn.execute(
        "SELECT DISTINCT code FROM announcements "
        "WHERE publish_time >= ? AND category IN ('定期报告','业绩预告') "
        "ORDER BY code", (since,))]
    if after:
        codes = [c for c in codes if c > after]
    print(f"[fin] since {since}: {len(codes)} stocks with new reports"
          f"{' (manual)' if manual else ''}"
          f"{f' (after {after})' if after else ''}"
          f"{f' (limit {limit})' if limit else ''}", flush=True)

    n_stocks, n_rows, n_err = 0, 0, 0
    consecutive_fail = 0
    recent = []               # 滑窗: 1=成功 0=失败
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        try:
            df = fin_mod.fetch_with_retry(code, max_tries=FIN_MAX_TRIES)
            if df is not None and not df.empty:
                n_rows += fin_mod.save_stock(conn, code, df)
            n_stocks += 1
            consecutive_fail = 0
            recent.append(1)
        except Exception as e:
            n_err += 1
            consecutive_fail += 1
            recent.append(0)
            print(f"  [fin ERR] {code}: {type(e).__name__}: {e}", flush=True)
        recent = recent[-FIN_BAD_RATE_WINDOW:]

        # 熔断: 连续失败 → 中止本段(不推游标, 下轮重试), 别再撞封禁
        if consecutive_fail >= FIN_STOP_CONSECUTIVE:
            _fin_note_resume(codes, i, consecutive_fail, after)
            raise RuntimeError(
                f"连续 {FIN_STOP_CONSECUTIVE} 只失败, 疑似 456 封禁, 中止 fin "
                f"(进度 {i}/{len(codes)}, 成功 {n_stocks})")
        # 限量: 到量优雅停(只在显式模式传入), 断点写文件供下轮续跑
        if limit and i >= limit:
            _fin_note_resume(codes, i, consecutive_fail, after)
            print(f"[fin] 到量停 (limit={limit}, 进度 {i}/{len(codes)}, "
                  f"成功 {n_stocks})", flush=True)
            break
        # 冷却: 滑窗失败率过高 → 停 FIN_COOL_DOWN 再继续
        if (len(recent) == FIN_BAD_RATE_WINDOW
                and 1 - sum(recent) / FIN_BAD_RATE_WINDOW > FIN_BAD_RATE_THRESHOLD):
            print(f"  ⚠ 最近 {FIN_BAD_RATE_WINDOW} 只失败率过阈, "
                  f"冷却 {FIN_COOL_DOWN}s", flush=True)
            time.sleep(FIN_COOL_DOWN)
            recent = []

        # 防封节奏(照搬 step4 实测方案)
        if i % FIN_LONG_PAUSE_EVERY == 0:
            lp = random.uniform(FIN_LONG_PAUSE_MIN, FIN_LONG_PAUSE_MAX)
            print(f"  ... 长歇 {lp:.0f}s (每 {FIN_LONG_PAUSE_EVERY} 只)", flush=True)
            time.sleep(lp)
        elif i % FIN_BATCH_PAUSE_EVERY == 0:
            print(f"  ... 批次停 {FIN_BATCH_PAUSE}s", flush=True)
            time.sleep(FIN_BATCH_PAUSE)
        else:
            time.sleep(random.uniform(FIN_INTERVAL_MIN, FIN_INTERVAL_MAX) + sleep_s)

        if i % 100 == 0:
            rate = i / (time.time() - t0) * 3600
            print(f"  [fin {i}/{len(codes)}] rate={rate:.0f}/h", flush=True)
    if not manual and not DRY_RUN and (n_err == 0 or n_stocks > 0):
        meta_set(conn, "fin_last_ok", dt.date.today().isoformat())
    print(f"[fin] DONE stocks={n_stocks} rows_upserted={n_rows} err={n_err}", flush=True)
    return n_stocks, n_err


# ---------- C. 增减持增量 (三所) ----------

def _holder_window(conn, exchange):
    row = conn.execute(
        "SELECT MAX(COALESCE(disclosure_date, change_date)) FROM holder_changes "
        "WHERE exchange=?", (exchange,)).fetchone()
    if row and row[0]:
        return (dt.date.fromisoformat(row[0][:10])
                - dt.timedelta(days=HOLDER_OVERLAP_DAYS)).isoformat()
    return (dt.date.today() - dt.timedelta(days=30)).isoformat()


def update_holder_sse(conn):
    begin = _holder_window(conn, "sse")
    end = "2050-01-01"
    session = requests.Session()
    # 复用回填模块的解析: 临时替换其参数函数, 注入日期窗口
    orig = hc_mod.sse_params

    def windowed(page):
        p = orig(page)
        p["BEGIN_DATE"] = begin
        p["END_DATE"] = end
        return p

    hc_mod.sse_params = windowed
    try:
        total = hc_mod.sse_total(session)
        inserted = 0
        for page in range(1, total + 1):
            inserted += hc_mod.save_rows(conn, hc_mod.sse_page(session, page))
        print(f"[sse] window>={begin} pages={total} inserted={inserted}", flush=True)
        return inserted
    finally:
        hc_mod.sse_params = orig


def update_holder_bse(conn):
    begin = _holder_window(conn, "bse")
    session = requests.Session()
    orig = hc_mod.bse_params

    def windowed(page):
        p = orig(page)
        p["startTime"] = begin
        p["endTime"] = dt.date.today().isoformat()
        return p

    hc_mod.bse_params = windowed
    try:
        total = hc_mod.bse_total(session)
        inserted = 0
        for page in range(0, total):   # bse 页码从 0 起
            inserted += hc_mod.save_rows(conn, hc_mod.bse_page(session, page))
        print(f"[bse] window>={begin} pages={total} inserted={inserted}", flush=True)
        return inserted
    finally:
        hc_mod.bse_params = orig


def update_holder_szse(conn):
    """szse 无日期参数: 列表最新在前, 从第 1 页翻, 整页 0 新增即停。
    129 IP 曾被深交所掐(7-4), 失败只警告跳过, 不炸周更。"""
    session = requests.Session()
    try:
        inserted = 0
        for page in range(1, SZSE_PAGE_CAP + 1):
            rows = hc_mod.szse_page(session, page)
            if not rows:
                break
            if page == 1:
                newest = max((r.get("change_date") or "") for r in rows)
                stale = (dt.date.today() - dt.timedelta(days=30)).isoformat()
                if newest < stale:
                    print(f"  [szse] WARN page1 newest={newest} 非最新在前排序, 中止防漏", flush=True)
                    return -1
            n = hc_mod.save_rows(conn, rows)
            inserted += n
            if n == 0:
                break
        print(f"[szse] pages={page} inserted={inserted}", flush=True)
        return inserted
    except Exception as e:
        print(f"[szse] SKIP {type(e).__name__}: {e} (IP 可能仍被深交所限制, 下周自动重试)", flush=True)
        return -1


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", type=str, default="ann,fin,holder",
                    help="逗号分隔: ann/fin/holder")
    ap.add_argument("--ann-days", type=str, default="",
                    help="补历史: 逗号分隔的日期 2026-07-05,2026-07-06 (不动游标)")
    ap.add_argument("--ann-start", type=str, default="", help="补历史: 起始日 (不动游标)")
    ap.add_argument("--ann-end", type=str, default="", help="补历史: 结束日 (不动游标)")
    ap.add_argument("--fin-since", type=str, default="",
                    help="补跑 fin: 重拉该日起披露过定期报告/业绩预告的股票 (不动游标)")
    ap.add_argument("--fin-after", type=str, default="",
                    help="断点续跑: 跳过 code<=此值 (需与 --fin-since 同用)")
    ap.add_argument("--fin-limit", type=int, default=0,
                    help="本轮最多处理 N 只后优雅停 (需与 --fin-since 同用)")
    ap.add_argument("--fin-sleep", type=float, default=0.0,
                    help="fin 在内置防封节奏之上追加的逐股延时秒数(一般不需要)")
    ap.add_argument("--dry-run", action="store_true",
                    help="只抓不写库(事务最后回滚), 用于验证抓取完整性")
    args = ap.parse_args()
    if (args.fin_after or args.fin_limit) and not args.fin_since:
        ap.error("--fin-after/--fin-limit 仅限显式模式, 必须与 --fin-since 同用"
                 " (正常模式游标语义不兼容断点/限量)")
    sections = {s.strip() for s in args.sections.split(",") if s.strip()}
    ann_days = [d for d in args.ann_days.split(",") if d.strip()]

    global DRY_RUN
    DRY_RUN = args.dry_run
    if DRY_RUN:
        print("*** DRY-RUN: 只抓取校验, 不写库 ***", flush=True)

    conn = sqlite3.connect(DB_PATH)
    init_meta(conn)
    failed = 0
    stats = []

    if "ann" in sections:
        try:
            n = update_announcements(conn, days=ann_days,
                                     start=args.ann_start, end=args.ann_end)
            stats.append(f"ann=+{n}")
        except Exception as e:
            failed += 1
            print(f"[ann] FAILED {type(e).__name__}: {e}", flush=True)

    if "fin" in sections:
        try:
            n_stocks, n_err = update_fin_abstract(conn, since=args.fin_since or None,
                                                  sleep_s=args.fin_sleep,
                                                  after=args.fin_after,
                                                  limit=args.fin_limit)
            stats.append(f"fin_stocks={n_stocks}(err={n_err})")
        except Exception as e:
            failed += 1
            print(f"[fin] FAILED {type(e).__name__}: {e}", flush=True)

    if "holder" in sections:
        for name, fn in [("sse", update_holder_sse), ("bse", update_holder_bse)]:
            try:
                n = fn(conn)
                stats.append(f"{name}=+{n}")
            except Exception as e:
                failed += 1
                print(f"[{name}] FAILED {type(e).__name__}: {e}", flush=True)
        n = update_holder_szse(conn)   # 自带容错, 不计 failed
        stats.append(f"szse=+{n}" if n >= 0 else "szse=SKIPPED")

    print(f"INFO_WEEKLY FINISHED {' '.join(stats)} failed_sections={failed}", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
