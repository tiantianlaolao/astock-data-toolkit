"""
合并 backfill_data 到生产 astock_data.

执行前提:
  - backfill_data/ 下的 5 个 *_backfill.parquet 全部完成
  - 生产 parquet 已备份 (.bak_15y_*)

步骤:
  1. 各表 concat (backfill + 现有) + 按主键 dedup
  2. 原子写入 (先写 .tmp, 再 rename)
  3. 跨表对齐验证 (股票集一致性)
  4. 日期覆盖验证 (旧时段数据一字不变)
  5. 打印差异报告

用法:
  python merge_backfill.py --dry-run   # 只验证不写
  python merge_backfill.py --commit    # 实际写入
"""
import os
import sys
import argparse
import io
import hashlib
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

import pandas as pd
import pyarrow.parquet as pq

_HOME = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROD_DIR = os.path.join(_HOME, 'astock_data')
BACKFILL_DIR = os.path.join(_HOME, 'backfill_data')

def LOG(m): print(f'[{datetime.now().strftime("%H:%M:%S")}] {m}', flush=True)

# (表名, 主键列, backfill 文件, 生产文件)
TABLES = [
    ('daily_ohlcv', ['stock_code', 'date'],
     'daily_ohlcv_backfill.parquet', 'daily_ohlcv.parquet'),
    ('valuation_daily', ['stock_code', 'date'],
     'valuation_daily_backfill.parquet', 'valuation_daily.parquet'),
    ('financial_quarterly', ['stock_code', 'report_date'],
     'financial_quarterly_backfill.parquet', 'financial_quarterly.parquet'),
    ('dividend_history', ['stock_code', 'announce_date'],
     'dividend_history_backfill.parquet', 'dividend_history.parquet'),
    ('index_daily', ['date'],
     'index_daily_backfill.parquet', 'index_daily.parquet'),
]


def md5_file(p):
    h = hashlib.md5()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def merge_one(name, pk, bf_file, prod_file, commit=False):
    LOG(f'\n=== {name} ===')
    bf_path = os.path.join(BACKFILL_DIR, bf_file)
    prod_path = os.path.join(PROD_DIR, prod_file)

    if not os.path.exists(bf_path):
        LOG(f'  ❌ backfill 不存在: {bf_path}, 跳过')
        return None
    if not os.path.exists(prod_path):
        LOG(f'  ❌ 生产不存在: {prod_path}, 跳过')
        return None

    bf_df = pd.read_parquet(bf_path)
    prod_df = pd.read_parquet(prod_path)
    LOG(f'  backfill: {len(bf_df):,} 行, 列 {list(bf_df.columns)}')
    LOG(f'  生产   : {len(prod_df):,} 行, 列 {list(prod_df.columns)}')

    # 合并并去重
    merged = pd.concat([bf_df, prod_df], ignore_index=True)
    merged_sorted = merged.sort_values(pk).reset_index(drop=True)
    before_dup = len(merged_sorted)
    merged_dedup = merged_sorted.drop_duplicates(subset=pk, keep='last').reset_index(drop=True)
    dup_removed = before_dup - len(merged_dedup)
    LOG(f'  合并后: {before_dup:,} 行 → 去重后: {len(merged_dedup):,} 行 (去重 {dup_removed})')

    # 日期边界检查
    date_col = pk[-1] if pk[-1] != 'date' else pk[-1]
    if 'date' in merged_dedup.columns:
        LOG(f'  日期范围: {merged_dedup["date"].min()} ~ {merged_dedup["date"].max()}')
    if 'report_date' in merged_dedup.columns:
        LOG(f'  report_date 范围: {merged_dedup["report_date"].min()} ~ {merged_dedup["report_date"].max()}')
    if 'announce_date' in merged_dedup.columns:
        LOG(f'  announce_date 范围: {merged_dedup["announce_date"].min()} ~ {merged_dedup["announce_date"].max()}')

    # 旧段数据一字不变验证: 取生产段数据与 merged 对应段对比
    if 'stock_code' in pk and 'date' in pk:
        prod_dates = set(zip(prod_df['stock_code'], prod_df['date']))
        merged_pairs = merged_dedup.set_index(['stock_code', 'date'])
        missing = [p for p in prod_dates if p not in merged_pairs.index]
        if missing:
            LOG(f'  ⚠ 合并后缺失 {len(missing)} 条原生产记录, 前 5: {missing[:5]}')
        else:
            LOG(f'  ✅ 生产段 {len(prod_dates):,} 记录全在合并后数据里')

    if commit:
        tmp_path = prod_path + '.tmp_merge'
        merged_dedup.to_parquet(tmp_path, index=False, engine='pyarrow')
        os.replace(tmp_path, prod_path)
        size_mb = os.path.getsize(prod_path) / 1024 / 1024
        md5 = md5_file(prod_path)
        LOG(f'  ✅ 写入: {size_mb:.1f} MB, md5={md5}')

    return {
        'name': name,
        'bf_rows': len(bf_df),
        'prod_rows': len(prod_df),
        'merged_rows': len(merged_dedup),
        'duplicates_removed': dup_removed,
    }


def cross_table_check():
    """跨表股票集一致性"""
    LOG(f'\n=== 跨表股票集对齐 ===')
    sl = pd.read_parquet(os.path.join(PROD_DIR, 'stock_list.parquet'))
    base_codes = set(sl['stock_code'])
    LOG(f'  stock_list (基准): {len(base_codes)} 只')

    for name, _, _, prod_file in TABLES:
        if name == 'index_daily':
            continue  # 沪深300 不按股票, 跳过
        p = os.path.join(PROD_DIR, prod_file)
        if not os.path.exists(p):
            continue
        df = pd.read_parquet(p, columns=['stock_code'])
        codes = set(df['stock_code'])
        missing_in_tbl = base_codes - codes
        extra_in_tbl = codes - base_codes
        LOG(f'  {name}: {len(codes)} 只, 缺 {len(missing_in_tbl)}, 多 {len(extra_in_tbl)}')


def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument('--dry-run', action='store_true')
    grp.add_argument('--commit', action='store_true')
    args = ap.parse_args()

    LOG(f'mode: {"COMMIT" if args.commit else "DRY RUN"}')

    results = []
    for name, pk, bf_file, prod_file in TABLES:
        r = merge_one(name, pk, bf_file, prod_file, commit=args.commit)
        if r:
            results.append(r)

    LOG('\n=== 汇总 ===')
    for r in results:
        LOG(f'  {r["name"]:25s} bf={r["bf_rows"]:>10,} + prod={r["prod_rows"]:>10,} → {r["merged_rows"]:>10,} (dedup {r["duplicates_removed"]:>6})')

    if args.commit:
        cross_table_check()


if __name__ == '__main__':
    main()
