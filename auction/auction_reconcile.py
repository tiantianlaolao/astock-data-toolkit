# -*- coding: utf-8 -*-
"""收盘竞价双源对账 (2026-08-12 立项)

两条完全独立的链路采到同一件事, 每天比一次:
  A 源 = auction_collector (东方财富, 14:57 前扫 / 15:01 后扫差值)
  B 源 = 通达信 1min 标记 15:00 的那根
不一致就告警。目的不是修数据, 是当哨兵——防"日志一切正常、数据静默出错"
(公告静默截断事故的教训, 见 feedback_crawler_total_reconciliation)。

用法: venv/bin/python3 auction_reconcile.py [YYYYMMDD]   # 缺省=今天
排点: 30 17 * * 1-5  (分钟线日增 15:30 起跑约 84 分, 17:00 前跑完)
退出码: 0=通过 1=告警 2=前置缺失
"""
import os, csv, gzip, sys
import datetime as dt

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BASE, "minute_data", "tdx_1min")
OUTDIR = os.path.join(BASE, "auction_data")

TOL = 0.0005          # 相对容差 0.05%
MAX_MISMATCH = 20     # 与分钟线日增同一告警线(只数"意外的"不一致, 见下)
MAX_MISSING_PCT = 2.0 # 真缺口占比上限(分母=分钟线名单内的股票)
MAX_SUSPECT_PCT = 10.0  # 旧快照占比上限: 已知病因不告警, 但爆量了要知道
ZERO = 1.0

# ⚠️ 2026-08-20 加: 认 collector 的 suspect 列。
# suspect=1 的股票, collector 那一侧的前扫快照被东财冻结在 14:56~14:57 之间某一秒
# (8-20 实证: vol_pre 落在 14:57 那根内部, 对不上任何一根边界), B−A 把快照瞬间到
# 连续竞价结束那几十秒的成交也算进了竞价量 → 必然不一致, 且**病因已知、重拉无效**
# (东财服务端缓存, 我方只能挡不能治)。这类不一致再计进告警线, 就是天天报红把真信号
# 淹掉。所以: 单独归类、只报数、不告警; 只有占比爆掉(>MAX_SUSPECT_PCT)才升级。
# 🔴 非 suspect 的不一致仍按原告警线 —— 那才是"没预料到"的, 一只都不该放过。
# (8-20 全天 76 只不一致 100% 命中 suspect, 非 suspect 的 0 只)

# ⚠️ 两个源的股票池本来就不一样, 不是缺口:
#   A源(东财 clist_delay) 5548 只, 含 PT/退市/*ST 壳股
#   B源(分钟线 codes.csv) 5024 只, refresh_codes_weekly 已按设计排掉退市壳和未上市
#   2026-08-12 实测差额 524 只 全部是壳股(00*226/60*193/30*89/68*16, 无北交所)
# 所以"没有 gz 文件"= 不在名单, 只记录; "有 gz 但当日无 15:00 根"= 真缺口, 才告警。


def main():
    day = sys.argv[1] if len(sys.argv) > 1 else dt.date.today().strftime("%Y%m%d")
    iso = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    src = os.path.join(OUTDIR, f"close_{day}.csv")
    print(f"auction_reconcile {iso} start {dt.datetime.now():%F %T}", flush=True)

    if not os.path.exists(src):
        print(f"⚠️ RECONCILE ALERT: 缺 collector 文件 {src}")
        return 2
    with open(src, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  A源(collector) {len(rows)} 只", flush=True)

    hit = mism = mism_sus = missing = both_zero = not_in_universe = 0
    bad = []
    for r in rows:
        code = r["code"]
        path = os.path.join(SRC, code + ".csv.gz")
        if not os.path.exists(path):
            not_in_universe += 1      # 不在分钟线名单(退市壳等), 预期内
            continue
        bar = None
        try:
            with gzip.open(path, "rt") as f:
                key = f"{iso} 15:00"
                for ln in f:
                    if ln.startswith(key):
                        bar = ln.rstrip("\n").split(",")
                        break
        except Exception as e:
            missing += 1
            if len(bad) < 8:
                bad.append(f"{code} 读取失败 {e}")
            continue
        if bar is None:
            missing += 1
            continue

        bv = float(bar[5]); ba = float(bar[6])
        if bv < ZERO:
            bv = 0.0
        if ba < ZERO:
            ba = 0.0
        av = float(r["auction_vol"] or 0) * 100.0   # 手 -> 股
        aa = float(r["auction_amt"] or 0)

        if av == 0 and bv == 0:
            both_zero += 1
            continue
        ok = (abs(bv - av) <= max(1.0, av * TOL)
              and abs(ba - aa) <= max(1.0, aa * TOL))
        if ok:
            hit += 1
        elif r.get("suspect") == "1":
            mism_sus += 1          # 病因已知(东财冻结副本), 只报数不告警
        else:
            mism += 1
            if len(bad) < 8:
                bad.append(f"{code} {r.get('name','')} "
                           f"1min={bv:.0f}股/{ba:.0f}元 "
                           f"collector={av:.0f}股/{aa:.0f}元")

    cmp_n = hit + mism + mism_sus
    rate = hit / cmp_n * 100 if cmp_n else 0.0
    sus_pct = mism_sus / cmp_n * 100 if cmp_n else 0.0
    in_universe = cmp_n + both_zero + missing      # 分钟线名单内的股票数
    miss_pct = missing / in_universe * 100 if in_universe else 0.0
    print(f"  可比 {cmp_n} 只: 一致 {hit}, 不一致 {mism + mism_sus}, "
          f"一致率 {rate:.3f}%")
    print(f"    其中 旧快照(suspect=1)引起 {mism_sus} 只 ({sus_pct:.2f}%) "
          f"病因已知, 不告警")
    print(f"    其中 意外不一致 {mism} 只 ← 只有这个计告警线 {MAX_MISMATCH}")
    print(f"  双方均无竞价 {both_zero} 只")
    print(f"  不在分钟线名单 {not_in_universe} 只 (退市壳等, 预期内不告警)")
    print(f"  名单内但缺当日 15:00 根 {missing} 只 ({miss_pct:.2f}%) ← 真缺口")
    for b in bad:
        print(f"    - {b}")

    alert = []
    if mism > MAX_MISMATCH:
        alert.append(f"意外不一致 {mism} 只 > 告警线 {MAX_MISMATCH}")
    if sus_pct > MAX_SUSPECT_PCT:
        alert.append(f"旧快照不一致 {mism_sus} 只 = {sus_pct:.2f}% "
                     f"> {MAX_SUSPECT_PCT}% (东财缓存病情加重, 不是我方故障)")
    if miss_pct > MAX_MISSING_PCT:
        alert.append(f"名单内真缺口 {missing} 只 = {miss_pct:.2f}% "
                     f"> {MAX_MISSING_PCT}% (分钟线日增可能没跑完或失败)")
    if cmp_n == 0:
        alert.append("零条可比, 两源之一整体失效")

    if alert:
        for a in alert:
            print(f"⚠️ RECONCILE ALERT: {a}")
        return 1
    print("  对账通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
