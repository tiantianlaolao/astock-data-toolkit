# -*- coding: utf-8 -*-
"""
财务摘要回填 — 新浪 getFinanceReport2022 (经 akshare stock_financial_abstract)
(信息模块 Phase 1 剩余项④, 2026-07-04)

- 覆盖 stock_list.parquet 全部股票, 每股一次调用返回全历史(1998~今, 80指标×季度)
- 精选 28 个核心指标, 宽表落 SQLite: astock_data/info_archive.db
    fin_abstract(code, report_date, ...28列, UNIQUE(code, report_date))
    fin_progress(code, status, n_quarters, err, updated_at)
- 断点续传: status='done' 跳过; 限速 0.35~0.6s/股, 指数退避 5 次

用法:
  venv/bin/python info_backfill_fin_abstract.py                  # 全量(断点续传)
  venv/bin/python info_backfill_fin_abstract.py --limit 3        # 烟测
  venv/bin/python info_backfill_fin_abstract.py --codes 600519
"""
import argparse
import datetime as dt
import math
import os
import random
import sqlite3
import time

import pandas as pd
import akshare as ak

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "astock_data")
DB_PATH = os.path.join(DATA_DIR, "info_archive.db")

# (选项, 指标) -> SQL 列名。选项+指标联合匹配, 避免跨组重名(如毛利率/ROE 多组出现)
METRICS = [
    ("常用指标", "归母净利润", "net_profit_attr"),
    ("常用指标", "营业总收入", "revenue"),
    ("常用指标", "营业成本", "op_cost"),
    ("常用指标", "净利润", "net_profit"),
    ("常用指标", "扣非净利润", "net_profit_deducted"),
    ("常用指标", "股东权益合计(净资产)", "equity"),
    ("常用指标", "商誉", "goodwill"),
    ("常用指标", "经营现金流量净额", "ocf"),
    ("常用指标", "基本每股收益", "eps_basic"),
    ("常用指标", "每股净资产", "bps"),
    ("常用指标", "每股现金流", "cfps"),
    ("常用指标", "净资产收益率(ROE)", "roe"),
    ("常用指标", "总资产报酬率(ROA)", "roa"),
    ("常用指标", "毛利率", "gross_margin"),
    ("常用指标", "销售净利率", "net_margin"),
    ("常用指标", "期间费用率", "expense_ratio"),
    ("常用指标", "资产负债率", "debt_ratio"),
    ("成长能力", "营业总收入增长率", "revenue_yoy"),
    ("成长能力", "归属母公司净利润增长率", "np_attr_yoy"),
    ("收益质量", "经营活动净现金/归属母公司的净利润", "ocf_to_np"),
    ("财务风险", "流动比率", "current_ratio"),
    ("财务风险", "速动比率", "quick_ratio"),
    ("财务风险", "权益乘数", "equity_multiplier"),
    ("营运能力", "存货周转率", "inventory_turnover"),
    ("营运能力", "应收账款周转天数", "ar_days"),
    ("营运能力", "总资产周转率", "asset_turnover"),
    ("每股指标", "每股未分配利润", "retained_eps"),
    ("每股指标", "每股经营现金流", "ocf_ps"),
]
COLS = [m[2] for m in METRICS]


def init_db():
    conn = sqlite3.connect(DB_PATH)
    col_defs = ",\n        ".join(f"{c} REAL" for c in COLS)
    conn.execute(f"""CREATE TABLE IF NOT EXISTS fin_abstract(
        code TEXT NOT NULL,
        report_date TEXT NOT NULL,
        {col_defs},
        UNIQUE(code, report_date))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fin_code ON fin_abstract(code, report_date)")
    conn.execute("""CREATE TABLE IF NOT EXISTS fin_progress(
        code TEXT PRIMARY KEY,
        status TEXT,
        n_quarters INTEGER DEFAULT 0,
        err TEXT,
        updated_at TEXT)""")
    conn.commit()
    return conn


def fetch_with_retry(code, max_tries=5):
    delay = 5
    for attempt in range(max_tries):
        try:
            time.sleep(random.uniform(0.35, 0.6))
            return ak.stock_financial_abstract(symbol=code)
        except Exception as e:
            if attempt == max_tries - 1:
                raise
            print(f"    retry {attempt + 1} after {delay}s: {type(e).__name__}: {e}", flush=True)
            time.sleep(delay)
            delay = min(delay * 3, 300)


def save_stock(conn, code, df):
    """df: 列 = [选项, 指标, 20260331, 20251231, ...]"""
    date_cols = [c for c in df.columns if str(c).isdigit() and len(str(c)) == 8]
    picked = {}  # sql_col -> Series indexed by date col
    for sec, ind, col in METRICS:
        m = df[(df["选项"] == sec) & (df["指标"] == ind)]
        if not m.empty:
            picked[col] = m.iloc[0]
    n = 0
    for d in date_cols:
        rd = f"{str(d)[:4]}-{str(d)[4:6]}-{str(d)[6:8]}"
        vals = []
        has_any = False
        for col in COLS:
            v = picked.get(col, {}).get(d) if col in picked else None
            try:
                v = float(v)
                if math.isnan(v) or math.isinf(v):
                    v = None
            except (TypeError, ValueError):
                v = None
            if v is not None:
                has_any = True
            vals.append(v)
        if not has_any:
            continue
        conn.execute(
            f"INSERT OR REPLACE INTO fin_abstract(code, report_date, {','.join(COLS)}) "
            f"VALUES(?,?,{','.join('?' * len(COLS))})",
            [code, rd] + vals)
        n += 1
    conn.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--codes", type=str, default="")
    args = ap.parse_args()

    conn = init_db()
    stock_list = pd.read_parquet(os.path.join(DATA_DIR, "stock_list.parquet"))
    codes = [str(c).zfill(6) for c in stock_list["stock_code"].tolist()]
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    if args.limit:
        codes = codes[: args.limit]

    done = {r[0] for r in conn.execute("SELECT code FROM fin_progress WHERE status='done'")}
    todo = [c for c in codes if c not in done]
    print(f"total={len(codes)} done={len(codes) - len(todo)} todo={len(todo)}", flush=True)

    t0 = time.time()
    for i, code in enumerate(todo, 1):
        try:
            df = fetch_with_retry(code)
            n = save_stock(conn, code, df) if df is not None and not df.empty else 0
            conn.execute("INSERT OR REPLACE INTO fin_progress VALUES(?,?,?,?,?)",
                         (code, "done", n, "", dt.datetime.now().isoformat()))
            conn.commit()
        except Exception as e:
            conn.execute("INSERT OR REPLACE INTO fin_progress VALUES(?,?,?,?,?)",
                         (code, "error", 0, f"{type(e).__name__}: {e}",
                          dt.datetime.now().isoformat()))
            conn.commit()
            print(f"  [ERR] {code}: {type(e).__name__}: {e}", flush=True)
        if i % 25 == 0 or i == len(todo):
            elapsed = time.time() - t0
            rate = i / elapsed * 3600
            eta_h = (len(todo) - i) / max(rate, 1)
            n_rows = conn.execute("SELECT COUNT(*) FROM fin_abstract").fetchone()[0]
            print(f"[{i}/{len(todo)}] {code} fin_rows={n_rows} "
                  f"rate={rate:.0f}/h eta={eta_h:.1f}h", flush=True)

    n_rows = conn.execute("SELECT COUNT(*) FROM fin_abstract").fetchone()[0]
    n_err = conn.execute("SELECT COUNT(*) FROM fin_progress WHERE status='error'").fetchone()[0]
    print(f"FINISHED fin_rows={n_rows} err_stocks={n_err}", flush=True)


if __name__ == "__main__":
    main()
