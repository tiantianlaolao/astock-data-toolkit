# -*- coding: utf-8 -*-
"""开盘/收盘集合竞价每日采集器 (2026-08-11)

依据 8-10(收盘)/8-11(开盘) 两次探测的实证结论:
- 开盘: 09:25 定价后至 09:30 开盘, 全市场 量/额 冻结 = 竞价成交量/额, 现价=开盘价
  → 09:25:30~09:29:40 窗口内单次全市场扫描直读(盘前累计为零, 无需差值)
- 收盘: 14:57~15:00 竞价期间 量/额 冻结, 15:00:1x 跳变
  → 14:57~15:00 前扫(A) + 15:01:30 后扫(B), B−A 差值 = 收盘竞价量/额, B 现价=收盘价
- 通道: clist_delay(push2delay) — 实测"延迟源"几乎不延迟、不限流零失败;
  push2 实时的 clist 路径在我们的服务器上被按路径封, 换机器未必, 但延迟源已够用
- 两次扫描均落在数值静止段 → 翻页期间排序不漂移, 不漏不重

产出 $ASTOCK_HOME/auction_data/ :
  open_YYYYMMDD.csv  : code,name,open,prev_close,auction_vol,auction_amt,suspect
  close_YYYYMMDD.csv : code,name,close,auction_vol,auction_amt,vol_pre,vol_post,suspect
对账(claimed total vs 实抓行数)与告警一律写 stdout(由 run.sh 收进日志)。

🔴 旧快照闸门 (2026-08-19 加, 见 guard_fresh): 东财会成片返回上一时刻的旧副本,
   每次扫描按 f124 逐只校验时刻, 旧页整页重拉, 两轮仍旧则 suspect=1 并告警。
   suspect 只是标记, 数值照写不删——判不判用它由下游决定。
   suspect 取值: 0=新鲜 / 1=名单内旧快照(真问题, 告警) / 2=名单外壳股旧快照
   (8-20 加, 退市/PT/长停股的 f124 恒为 08:00 占位值, 无害, 不重拉不告警)。

用法:
  python3 auction_collector.py open        # 由 cron 09:25 拉起
  python3 auction_collector.py close       # 由 cron 14:57 拉起
  python3 auction_collector.py test-sweep  # 盘后自测翻页机制, 不写正式文件

⚠️ 开盘竞价不可回溯: 它混在 1min 的第一根里, 事后无法从任何历史数据还原。
   今天没采, 这一天就永久没有。收盘竞价则可用 backfill_close_auction.py 从 1min 反推。
"""
import csv
import datetime as dt
import json
import os
import sys
import time

import requests

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTDIR = os.path.join(BASE, "auction_data")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"),
    "Referer": "https://quote.eastmoney.com/",
}
CLIST_URL = "http://push2delay.eastmoney.com/api/qt/clist/get"
FS_HSA = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"   # 沪深A, 与数据管线口径一致
FIELDS = "f2,f5,f6,f12,f14,f17,f18,f124"        # 现价/量/额/代码/名称/今开/昨收/行情时间戳


def _num(v):
    """东财空值统一 '-' → 0.0"""
    return 0.0 if v in ("-", None, "") else float(v)


def fetch_page(session_box, tag, pn):
    """取单页, 返回 data(可能为 None); 三次重试后仍失败则抛出"""
    params = {"pn": str(pn), "pz": "100", "po": "1", "np": "1",
              "fltt": "2", "invt": "2", "fid": "f12", "fs": FS_HSA,
              "fields": FIELDS}
    for attempt in (1, 2, 3):
        try:
            r = session_box[0].get(CLIST_URL, params=params,
                                   headers=HEADERS, timeout=8)
            return r.json().get("data")
        except Exception as e:
            print(f"  [{tag}] p{pn} attempt{attempt} {type(e).__name__}: {e}",
                  flush=True)
            session_box[0].close()
            session_box[0] = requests.Session()
            if attempt == 3:
                raise
            time.sleep(2)


def sweep(session_box, tag):
    """全市场翻页扫描一遍, 返回 {code: row}, claimed_total, pages, {code: 页号}"""
    rows, page_of, total, pn = {}, {}, None, 1
    while True:
        data = fetch_page(session_box, tag, pn)
        if not data or not data.get("diff"):
            break
        total = data.get("total")
        for it in data["diff"]:
            rows[it["f12"]] = it
            page_of[it["f12"]] = pn
        if total and len(rows) >= total:
            break
        pn += 1
        time.sleep(0.15)
    print(f"  [{tag}] pages={pn} claimed={total} fetched={len(rows)}", flush=True)
    if total and len(rows) < total:
        print(f"  ALERT [{tag}] 抓取数 {len(rows)} < claimed {total}, 差 "
              f"{total - len(rows)}", flush=True)
    return rows, total, pn, page_of


def rows_quote_date(rows):
    """交易日守卫(走主通道): 取扫描结果里 f124 行情时间戳的众数日期。
    节假日全市场 f124 停在上个交易日 → 众数日期 != 今天 即非交易日/陈旧行情。
    (长期停牌股 f124 是老日期, 所以用众数不用 min/max)"""
    from collections import Counter
    c = Counter()
    for it in rows.values():
        v = it.get("f124")
        if v in ("-", None, "", 0):
            continue
        c[dt.datetime.fromtimestamp(int(v)).date()] += 1
    if not c:
        print("  WARN f124 全空, 无法判交易日", flush=True)
        return None
    qd, n = c.most_common(1)[0]
    print(f"  行情日期众数 {qd} ({n}/{len(rows)} 只)", flush=True)
    return qd


MINUTE_DIR = os.path.join(BASE, "minute_data", "tdx_1min")
_UNIVERSE = []          # 惰性缓存: [] 未读 / [None] 不可用 / [set] 可用


def minute_universe():
    """分钟线名单(= tdx_1min 目录里的股票), 与 auction_reconcile 同一口径。

    名单外 = refresh_codes_weekly 已排掉的退市/PT/长停壳股(国华退/PT金田A 之类)。
    ⚠️8-20 实证: 这类股全天无成交, 东财给的 f124 是当天 08:00:00 占位值 ——
    正好落进旧快照判据("日期是今天但时刻偏早"), 天天被误判成旧快照, 且重拉
    一万轮也不会变(8-20 open 腿 342→342→342 就是它们)。它们无当日行情、量为零,
    不影响差值, 也没有任何下游消费者 → 不重拉、不告警, 只标记。
    返回 None = 名单不可用(目录缺失或异常少), 此时闸门退回"全部按名单内处理"。"""
    if not _UNIVERSE:
        codes = None
        try:
            found = {fn[:-7] for fn in os.listdir(MINUTE_DIR)
                     if fn.endswith(".csv.gz")}
        except OSError as e:
            print(f"  WARN 分钟线名单不可读({e}), 闸门按全市场处理", flush=True)
        else:
            if len(found) < 1000:
                print(f"  WARN 分钟线名单只有 {len(found)} 只, 疑似异常, "
                      f"闸门按全市场处理", flush=True)
            else:
                codes = found
        _UNIVERSE.append(codes)
    return _UNIVERSE[0]


def quote_ts(it):
    """单只股票的行情时间戳(f124, unix秒) → datetime; 空值返回 None"""
    v = it.get("f124")
    if v in ("-", None, "", 0):
        return None
    try:
        return dt.datetime.fromtimestamp(int(v))
    except (TypeError, ValueError):
        return None


def guard_fresh(session_box, tag, rows, page_of, hh, mm, rounds=2):
    """旧快照闸门 (2026-08-19 立): 东财分页会成片返回上一时刻的旧副本 ——
    8-19 收盘前扫 205 只(命中 14 页)的 f124 停在 14:57 之前, 差值把收盘前最后
    一分钟的连续成交也算进了竞价量; 8-18 已有 2 只同样症状但量小被容差吞掉。
    判据: f124 日期==今天 且 时刻 < 截止线 → 旧快照。整页重拉, 仍旧则标 suspect。
    ⚠️只认"日期是今天但时刻偏早"的; 长期停牌股 f124 停在老日期, 不算旧快照
    (它本就无当日行情, 量为零不影响差值), 重拉也救不回来。
    ⚠️8-20 加: 名单外壳股(见 minute_universe)不参与重拉、不进告警, 只标 suspect=2
    —— 它们的 f124 是 08:00 占位值, 永远"旧", 天天报红把真信号淹了。
    返回: (名单内仍旧的 code 集合, 名单外旧快照的 code 集合)"""
    cutoff = dt.datetime.now().replace(hour=hh, minute=mm, second=0,
                                       microsecond=0)
    today = dt.date.today()
    uni = minute_universe()
    stale, shell = [], []
    for rd in range(rounds + 1):
        stale, shell = [], []
        for c, it in rows.items():
            ts = quote_ts(it)
            if ts is not None and ts.date() == today and ts < cutoff:
                (stale if (uni is None or c in uni) else shell).append(c)
        if rd == 0 and shell:
            print(f"  [{tag}] 名单外旧快照 {len(shell)} 只 "
                  f"(退市/PT/长停壳股, 无当日行情, 不重拉不告警)", flush=True)
        if not stale:
            if rd:
                print(f"  [{tag}] 旧快照已重拉干净", flush=True)
            return set(), set(shell)
        pages = sorted({page_of[c] for c in stale if c in page_of})
        oldest = min(quote_ts(rows[c]) for c in stale)
        if rd == rounds:
            break
        print(f"  WARN [{tag}] 旧快照 {len(stale)} 只 命中 {len(pages)} 页 "
              f"(最旧 {oldest:%H:%M:%S} < 截止 {cutoff:%H:%M:%S}), "
              f"重拉第 {rd + 1} 轮: {','.join('p%d' % p for p in pages[:12])}"
              f"{'...' if len(pages) > 12 else ''}", flush=True)
        for pn in pages:
            data = fetch_page(session_box, tag, pn)
            if not data or not data.get("diff"):
                continue
            for it in data["diff"]:
                rows[it["f12"]] = it
                page_of[it["f12"]] = pn
            time.sleep(0.15)
        time.sleep(1.0)
    print(f"  ALERT [{tag}] 重拉 {rounds} 轮后仍有 {len(stale)} 只旧快照 "
          f"(最旧 {oldest:%H:%M:%S}), 已标 suspect=1", flush=True)
    return set(stale), set(shell)


def wait_until(hh, mm, ss):
    target = dt.datetime.now().replace(hour=hh, minute=mm, second=ss,
                                       microsecond=0)
    gap = (target - dt.datetime.now()).total_seconds()
    if gap > 0:
        print(f"  等待 {gap:.0f}s 至 {target.strftime('%H:%M:%S')}", flush=True)
        time.sleep(gap)


def write_csv(path, header, lines):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(lines)
    os.replace(tmp, path)
    print(f"  写出 {path} ({len(lines)} 行)", flush=True)


def mode_open():
    """开盘竞价: 09:25:30~09:29:40 窗口单次扫描直读"""
    now = dt.datetime.now()
    hard_end = now.replace(hour=9, minute=29, second=40, microsecond=0)
    if now > hard_end:
        print("ALERT open 模式启动过晚(>09:29:40), 窗口已污染, 放弃", flush=True)
        return 2
    wait_until(9, 25, 30)
    sb = [requests.Session()]
    rows, total, _, page_of = sweep(sb, "open")
    qd = rows_quote_date(rows)
    if qd is not None and qd != dt.date.today():
        print("非交易日(行情日期不是今天), 不写文件", flush=True)
        return 0
    # 09:25 定价后量额才冻结; 早于 09:25 的快照读到的是竞价前的零/旧值。
    # 这一路无第二源(开盘竞价混在 1min 09:31 首根里分不出), 漏了不可回溯。
    suspect, shell = guard_fresh(sb, "open", rows, page_of, 9, 25)
    if dt.datetime.now() > dt.datetime.now().replace(hour=9, minute=30,
                                                    second=0, microsecond=0):
        print("ALERT 扫描收尾越过 09:30 开盘线, 尾部页数值可能已含连续竞价",
              flush=True)
    day = dt.date.today().strftime("%Y%m%d")
    lines, n_traded, n_open_mismatch = [], 0, 0
    for code in sorted(rows):
        it = rows[code]
        vol, amt = _num(it.get("f5")), _num(it.get("f6"))
        px, opn, pc = _num(it.get("f2")), _num(it.get("f17")), _num(it.get("f18"))
        if vol > 0:
            n_traded += 1
        if vol > 0 and px and opn and abs(px - opn) > 1e-6:
            n_open_mismatch += 1
        lines.append([code, it.get("f14"), opn or px, pc, int(vol), amt,
                      1 if code in suspect else (2 if code in shell else 0)])
    write_csv(os.path.join(OUTDIR, f"open_{day}.csv"),
              ["code", "name", "open", "prev_close", "auction_vol",
               "auction_amt", "suspect"], lines)
    print(f"  有竞价成交 {n_traded}/{len(lines)}; 现价≠今开 {n_open_mismatch} 只"
          f"(应为0, >0 说明窗口读数可疑); suspect {len(suspect)} 只"
          f"(另名单外壳股 {len(shell)} 只标 2, 不告警)", flush=True)
    return 0


def mode_close():
    """收盘竞价: 14:57~15:00 前扫 A + 15:01:30 后扫 B, 差值"""
    now = dt.datetime.now()
    if now > now.replace(hour=14, minute=59, second=30, microsecond=0):
        print("ALERT close 模式启动过晚(>14:59:30), 前扫窗口不够, 放弃", flush=True)
        return 2
    wait_until(14, 57, 10)
    sb = [requests.Session()]
    pre, total_a, _, page_a = sweep(sb, "close-pre")
    qd = rows_quote_date(pre)
    if qd is not None and qd != dt.date.today():
        print("非交易日(行情日期不是今天), 不写文件", flush=True)
        return 0
    # 前扫必须已含 14:57 收盘竞价开始前的全部连续成交, 否则 B−A 多算最后一分钟
    suspect_a, shell_a = guard_fresh(sb, "close-pre", pre, page_a, 14, 57)
    t_done = dt.datetime.now()
    if t_done > t_done.replace(hour=15, minute=0, second=0, microsecond=0):
        print("ALERT 前扫收尾越过 15:00, 差值将偏小, 结果可疑", flush=True)
    wait_until(15, 1, 30)
    post, total_b, _, page_b = sweep(sb, "close-post")
    # 后扫必须已含 15:00:1x 的竞价跳变, 停在 15:00 前的旧副本会让差值偏小
    suspect_b, shell_b = guard_fresh(sb, "close-post", post, page_b, 15, 0)
    suspect = suspect_a | suspect_b
    shell = (shell_a | shell_b) - suspect
    day = dt.date.today().strftime("%Y%m%d")
    lines, n_traded, n_neg, n_nopre = [], 0, 0, 0
    for code in sorted(post):
        it = post[code]
        vol_b, amt_b = _num(it.get("f5")), _num(it.get("f6"))
        a = pre.get(code)
        if a is None:
            n_nopre += 1
            continue
        vol_a, amt_a = _num(a.get("f5")), _num(a.get("f6"))
        dv, da = vol_b - vol_a, amt_b - amt_a
        if dv < 0 or da < -1e-6:
            n_neg += 1
        if dv > 0:
            n_traded += 1
        lines.append([code, it.get("f14"), _num(it.get("f2")), int(dv),
                      round(da, 2), int(vol_a), int(vol_b),
                      1 if code in suspect else (2 if code in shell else 0)])
    write_csv(os.path.join(OUTDIR, f"close_{day}.csv"),
              ["code", "name", "close", "auction_vol", "auction_amt",
               "vol_pre", "vol_post", "suspect"], lines)
    print(f"  有竞价成交 {n_traded}/{len(lines)}; 差值为负 {n_neg} 只(应为0); "
          f"后扫有前扫无 {n_nopre} 只; suspect {len(suspect)} 只"
          f"(前扫 {len(suspect_a)} / 后扫 {len(suspect_b)}); "
          f"另名单外壳股 {len(shell)} 只标 2, 不告警", flush=True)
    return 0


def mode_test_sweep():
    """盘后自测: 只验翻页机制(claimed==fetched, 无漏无重), 写测试文件"""
    sb = [requests.Session()]
    rows, total, pages, _ = sweep(sb, "test")
    rows_quote_date(rows)
    day = dt.date.today().strftime("%Y%m%d")
    lines = [[c, rows[c].get("f14"), _num(rows[c].get("f2")),
              int(_num(rows[c].get("f5"))), _num(rows[c].get("f6"))]
             for c in sorted(rows)]
    write_csv(os.path.join(OUTDIR, f"test_sweep_{day}.csv"),
              ["code", "name", "price", "vol", "amt"], lines)
    print(f"  test-sweep: claimed={total} fetched={len(rows)} pages={pages} "
          f"{'OK' if total == len(rows) else 'MISMATCH'}", flush=True)
    return 0 if total == len(rows) else 1


def main():
    os.makedirs(OUTDIR, exist_ok=True)
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    print(f"auction_collector {mode} start {dt.datetime.now():%F %T}", flush=True)
    if mode == "open":
        rc = mode_open()
    elif mode == "close":
        rc = mode_close()
    elif mode == "test-sweep":
        rc = mode_test_sweep()
    else:
        print("用法: auction_collector.py open|close|test-sweep")
        rc = 2
    print(f"auction_collector {mode} end {dt.datetime.now():%F %T} rc={rc}",
          flush=True)
    sys.exit(rc)


if __name__ == "__main__":
    main()
