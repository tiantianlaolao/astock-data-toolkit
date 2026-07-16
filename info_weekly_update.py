# -*- coding: utf-8 -*-
"""
信息模块周增量更新 — info_archive.db (公告/财务摘要/董监高增减持)
(信息模块 Phase1 待办⑤, 2026-07-05)

由 run_weekly_update.sh 作为 Step 6 调用, 也可手动跑。全部写入均为
INSERT OR IGNORE / 按股票整体 REPLACE, 只增不删, 随时可重跑。

三段增量策略:
  A. 公告 announcements — 巨潮全市场按日期段查询(不传 stock 参数),
     窗口 = 库内最新 publish_time 往前 3 天 ~ 今天, (code, ann_id) 唯一键去重。
  B. 财务摘要 fin_abstract — 公告驱动: 只重拉窗口内披露过
     定期报告/业绩预告(含业绩快报)的股票, 新浪接口一次返回该股全历史,
     整股 INSERT OR REPLACE(顺带覆盖财务重述)。窗口起点存 info_meta.fin_last_ok。
  C. 增减持 holder_changes — sse/bse 用接口原生日期窗口参数;
     szse 无日期参数, 按最新在前从第 1 页翻, 整页全重复即停(row_key 去重),
     且失败只警告跳过(129 IP 曾被深交所掐, 不能炸整个周更)。

用法:
  venv/bin/python info_weekly_update.py                 # 全部三段
  venv/bin/python info_weekly_update.py --sections ann  # 只跑指定段 ann/fin/holder
"""
import argparse
import datetime as dt
import os
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
ANN_PAGE_CAP = 3000       # 公告翻页安全上限(正常一周 ~300 页)
ANN_DUP_PAGE_STOP = 30    # 连续 N 页零新增即停(结果按时间倒序, 进入重叠区后全是旧数据;
                          # 巨潮全市场查询 hasMore 恒真不可信, 7-5 实测翻到 3000 页不止)
SZSE_PAGE_CAP = 60        # szse 逐页翻安全上限(正常一周 ~15 页)

CODE_RE = re.compile(r"^\d{6}$")


def init_meta(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS info_meta(key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()


def meta_get(conn, key, default=None):
    row = conn.execute("SELECT value FROM info_meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def meta_set(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO info_meta VALUES(?,?)", (key, value))
    conn.commit()


# ---------- A. 公告增量 (巨潮全市场日期段) ----------

def update_announcements(conn):
    today = dt.date.today().isoformat()
    row = conn.execute("SELECT MAX(publish_time) FROM announcements").fetchone()
    if row and row[0]:
        start = (dt.date.fromisoformat(row[0][:10])
                 - dt.timedelta(days=ANN_OVERLAP_DAYS)).isoformat()
    else:
        start = (dt.date.today() - dt.timedelta(days=FIN_DEFAULT_LOOKBACK)).isoformat()
    print(f"[ann] window {start}~{today}", flush=True)

    session = requests.Session()
    payload = {
        "pageNum": "1",
        "pageSize": "30",          # 巨潮硬顶 30, 传大会静默漏数据(7-2 实证)
        "column": "szse",          # akshare 同参: szse = 沪深京全市场
        "tabName": "fulltext",
        "plate": "",
        "stock": "",               # 空 = 全市场
        "searchkey": "",
        "secid": "",
        "category": "",
        "trade": "",
        "seDate": f"{start}~{today}",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "false",
    }
    inserted = 0
    dup_streak = 0
    page = 1
    while page <= ANN_PAGE_CAP:
        payload["pageNum"] = str(page)
        j = ann_mod.post_with_retry(session, payload)
        got = j.get("announcements") or []
        page_inserted = 0
        for item in got:
            code = str(item.get("secCode") or "").strip()
            if not CODE_RE.match(code):
                continue
            ann_id = str(item.get("announcementId") or "")
            title = (item.get("announcementTitle") or "").strip()
            if not ann_id or not title:
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
            page_inserted += cur.rowcount
        conn.commit()
        inserted += page_inserted
        dup_streak = dup_streak + 1 if page_inserted == 0 else 0
        if not j.get("hasMore") or not got:
            break
        if dup_streak >= ANN_DUP_PAGE_STOP:
            print(f"  [ann] {ANN_DUP_PAGE_STOP} 连续重复页, 已进入重叠区尾部, 停止翻页", flush=True)
            break
        if page % 50 == 0:
            print(f"  [ann page {page}] inserted={inserted}", flush=True)
        page += 1
    if page > ANN_PAGE_CAP:
        print(f"  [ann] WARN hit page cap {ANN_PAGE_CAP}, window may be incomplete", flush=True)
    print(f"[ann] DONE pages={page} inserted={inserted}", flush=True)
    return inserted


# ---------- B. 财务摘要增量 (定期报告公告驱动) ----------

def update_fin_abstract(conn):
    last_ok = meta_get(conn, "fin_last_ok")
    if last_ok:
        since = (dt.date.fromisoformat(last_ok[:10]) - dt.timedelta(days=1)).isoformat()
    else:
        since = (dt.date.today() - dt.timedelta(days=FIN_DEFAULT_LOOKBACK)).isoformat()
    codes = [r[0] for r in conn.execute(
        "SELECT DISTINCT code FROM announcements "
        "WHERE publish_time >= ? AND category IN ('定期报告','业绩预告') "
        "ORDER BY code", (since,))]
    print(f"[fin] since {since}: {len(codes)} stocks with new reports", flush=True)

    n_stocks, n_rows, n_err = 0, 0, 0
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        try:
            df = fin_mod.fetch_with_retry(code)
            if df is not None and not df.empty:
                n_rows += fin_mod.save_stock(conn, code, df)
            n_stocks += 1
        except Exception as e:
            n_err += 1
            print(f"  [fin ERR] {code}: {type(e).__name__}: {e}", flush=True)
        if i % 100 == 0:
            rate = i / (time.time() - t0) * 3600
            print(f"  [fin {i}/{len(codes)}] rate={rate:.0f}/h", flush=True)
    if n_err == 0 or n_stocks > 0:
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
    args = ap.parse_args()
    sections = {s.strip() for s in args.sections.split(",") if s.strip()}

    conn = sqlite3.connect(DB_PATH)
    init_meta(conn)
    failed = 0
    stats = []

    if "ann" in sections:
        try:
            n = update_announcements(conn)
            stats.append(f"ann=+{n}")
        except Exception as e:
            failed += 1
            print(f"[ann] FAILED {type(e).__name__}: {e}", flush=True)

    if "fin" in sections:
        try:
            n_stocks, n_err = update_fin_abstract(conn)
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
