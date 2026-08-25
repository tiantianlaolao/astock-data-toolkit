# -*- coding: utf-8 -*-
"""从已有 1min 数据反推历史收盘竞价 (2026-08-12 立项)

依据: 收盘集合竞价 14:57-15:00 期间无成交, 成交集中在 15:00:0x,
      通达信 1min 标记为 15:00 的那根 == 收盘竞价。
实证: 2026-08-12 随机 272 只与 collector 东财口径逐只比对, 一致率 100%
      (容差 0.05%); 且 5min 15:00 根 == 1min 14:56~15:00 五根之和, 加总自洽。
⚠️ 5min 不能用: 其 15:00 根覆盖 14:55-15:00, 混入连续竞价的量, 分不出来。

输出: auction_data/close_auction_history.parquet
      列 = date, code, name, close, auction_vol(手), auction_amt,
           day_vol(手), day_amt, ratio(竞价量占全日), full_coverage

🔴 full_coverage 这一列必须看: 1min 是"最新往回数 N 根"的滚动窗口, 长期停牌股
   停牌期间不产生 bar, 同样根数能往前够到更早的日历日 → 早期日期只有零星几只
   有数据。2026-08-12 实测: 跨度 138 天, 但满覆盖(>=5000只)只有最近 90 天,
   最早的 2026-01-16 只有 1 只股票。
   **做横截面统计前必须 df[df.full_coverage]**, 否则"全市场竞价总额"会是 1 只
   股票的数, 且不报错。数据一行不删, 只打标志。
用法: venv/bin/python3 backfill_close_auction.py
"""
import os, csv, gzip, sys, time
import pandas as pd

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BASE, "minute_data", "tdx_1min")
OUTDIR = os.path.join(BASE, "auction_data")
OUT = os.path.join(OUTDIR, "close_auction_history.parquet")

# 当日入表股票数 >= 此值才算满覆盖(全市场约 5020 只)
FULL_COVERAGE_MIN = 5000

# 通达信空量根是 float32 反规格化数(如 5.877e-39), 不是 0
ZERO = 1.0


def load_names():
    """名称从最近一份 collector close_*.csv 取, 取不到就留空"""
    names = {}
    try:
        files = sorted(f for f in os.listdir(OUTDIR)
                       if f.startswith("close_2") and f.endswith(".csv"))
        if files:
            with open(os.path.join(OUTDIR, files[-1]), encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    names[r["code"]] = r.get("name", "")
            print(f"  名称表取自 {files[-1]} ({len(names)} 只)", flush=True)
    except Exception as e:
        print(f"  ⚠️ 名称表读取失败, 留空: {e}", flush=True)
    return names


def scan_one(path):
    """单只: 逐日汇总全日量额, 并摘出 15:00 那根"""
    days = {}   # date -> [day_vol, day_amt, auc_vol, auc_amt, close]
    with gzip.open(path, "rt") as f:
        f.readline()  # header
        for ln in f:
            # datetime,open,high,low,close,vol,amount
            p = ln.rstrip("\n").split(",")
            if len(p) < 7:
                continue
            stamp = p[0]
            d = stamp[:10]
            hm = stamp[11:16]
            try:
                v = float(p[5]); a = float(p[6])
            except ValueError:
                continue
            if v < ZERO:
                v = 0.0
            if a < ZERO:
                a = 0.0
            rec = days.get(d)
            if rec is None:
                rec = days[d] = [0.0, 0.0, 0.0, 0.0, 0.0]
            rec[0] += v
            rec[1] += a
            if hm == "15:00":
                rec[2] = v
                rec[3] = a
                try:
                    rec[4] = float(p[4])
                except ValueError:
                    pass
    return days


def main():
    if not os.path.isdir(SRC):
        print(f"找不到 1min 目录: {SRC}")
        return 2
    os.makedirs(OUTDIR, exist_ok=True)
    names = load_names()
    files = sorted(f for f in os.listdir(SRC) if f.endswith(".csv.gz"))
    print(f"待扫 {len(files)} 只", flush=True)

    rows = []
    t0 = time.time()
    bad = 0
    for i, fn in enumerate(files, 1):
        code = fn[:-7]
        try:
            days = scan_one(os.path.join(SRC, fn))
        except Exception as e:
            bad += 1
            if bad <= 5:
                print(f"  ⚠️ {code} 读取失败: {e}", flush=True)
            continue
        nm = names.get(code, "")
        for d, (dv, da, av, aa, cl) in days.items():
            if dv <= 0:          # 当日无成交(停牌) 不入表
                continue
            rows.append((d, code, nm, cl, av / 100.0, aa,
                         dv / 100.0, da, av / dv if dv else 0.0))
        if i % 500 == 0:
            el = time.time() - t0
            print(f"  {i}/{len(files)}  {i/el:.0f} 只/秒  已收 {len(rows)} 行",
                  flush=True)

    df = pd.DataFrame(rows, columns=["date", "code", "name", "close",
                                     "auction_vol", "auction_amt",
                                     "day_vol", "day_amt", "ratio"])
    df = df.sort_values(["date", "code"]).reset_index(drop=True)

    # 满覆盖标志: 滚动窗口导致早期日期只有零星停牌股, 打标志不删数据
    cnt = df.groupby("date")["code"].transform("size")
    df["full_coverage"] = cnt >= FULL_COVERAGE_MIN
    df.to_parquet(OUT, index=False)

    per_day = df.groupby("date").size()
    full_days = per_day[per_day >= FULL_COVERAGE_MIN]

    print(f"\n写出 {OUT}")
    print(f"  行数 {len(df):,}  股票 {df['code'].nunique()}  "
          f"交易日 {df['date'].nunique()}")
    print(f"  日期范围 {df['date'].min()} ~ {df['date'].max()}")
    print(f"  读取失败 {bad} 只")
    print(f"  🔴 满覆盖(>={FULL_COVERAGE_MIN}只) {len(full_days)} 天: "
          f"{full_days.index.min()} ~ {full_days.index.max()}")
    print(f"     稀疏(不可做横截面) {len(per_day) - len(full_days)} 天, "
          f"占 {len(df) - int(df['full_coverage'].sum()):,} 行")
    # 逐日对账表: 每天多少只有竞价, 全市场竞价额, 占比
    g = df.groupby("date").agg(
        stocks=("code", "size"),
        with_auc=("auction_vol", lambda s: int((s > 0).sum())),
        auc_amt_yi=("auction_amt", lambda s: round(s.sum() / 1e8, 2)),
        ratio_mkt=("auction_vol", "sum"))
    dayvol = df.groupby("date")["day_vol"].sum()
    g["ratio_mkt"] = (g["ratio_mkt"] / dayvol * 100).round(2)
    print("\n逐日概览(尾 10 日):")
    print(g.tail(10).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
