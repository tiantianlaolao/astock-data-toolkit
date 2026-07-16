"""
重拉 cninfo 全市场股本变动 (扩展到每只股票上市日起)

当前 cninfo_share_change_full.parquet 只到 2020, 15 年 backfill 需要更早.
用 akshare stock_share_change_cninfo 逐只拉, start_date=19900101.

输出: <数据主目录>/cninfo_share_change_full_15y.parquet (新文件)
  - 跑完后用户决定是否替换生产 cninfo_share_change_full.parquet
"""
import os
import sys
import time
import json
import io
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
for var in ['http_proxy', 'https_proxy', 'all_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']:
    if var in os.environ:
        del os.environ[var]
os.environ['NO_PROXY'] = '*'

import akshare as ak
import pandas as pd

SCRIPT_DIR = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STOCK_LIST = os.path.join(SCRIPT_DIR, 'astock_data', 'stock_list.parquet')
OUTPUT = os.path.join(SCRIPT_DIR, 'cninfo_share_change_full_15y.parquet')
PROGRESS = os.path.join(SCRIPT_DIR, 'cninfo_refetch_progress.json')
REQUEST_INTERVAL = 0.3

def LOG(m): print(f'[{datetime.now().strftime("%H:%M:%S")}] {m}', flush=True)

def main():
    stock_list = pd.read_parquet(STOCK_LIST)
    codes = stock_list['stock_code'].tolist()
    total = len(codes)
    LOG(f'重拉 cninfo 全市场 (start=1990-01-01), {total} 只股')

    done = set()
    if os.path.exists(PROGRESS):
        with open(PROGRESS) as f:
            done = set(json.load(f))
        LOG(f'断点续传: {len(done)}/{total}')

    all_rows = []
    # 如果 output 存在, 先读进来 (resume 模式)
    if os.path.exists(OUTPUT):
        all_rows.append(pd.read_parquet(OUTPUT))
        LOG(f'已有 output: {sum(len(x) for x in all_rows)} 行')

    ok_n = len(done)
    fail_n = 0
    none_n = 0

    batch_buffer = []
    BATCH_FLUSH = 100   # 每 100 只写一次盘

    for i, code in enumerate(codes):
        if code in done:
            continue

        for attempt in range(3):
            try:
                df = ak.stock_share_change_cninfo(
                    symbol=code, start_date='19900101', end_date='20260419'
                )
                if df is None or df.empty:
                    none_n += 1
                    break

                # 提取需要的 4 列: stock_code, change_date, reason, total_share
                # 原始列名: 变动日期/变动原因/总股本/证券代码
                out_rows = []
                for _, row in df.iterrows():
                    change_date = pd.to_datetime(row.get('变动日期'), errors='coerce')
                    if pd.isna(change_date):
                        continue
                    total_share = pd.to_numeric(row.get('总股本'), errors='coerce')
                    if pd.isna(total_share):
                        continue
                    reason = str(row.get('变动原因', ''))
                    out_rows.append({
                        'stock_code': code,
                        'change_date': change_date,
                        'reason': reason,
                        'total_share': total_share,
                    })
                if out_rows:
                    batch_buffer.append(pd.DataFrame(out_rows))
                ok_n += 1
                done.add(code)
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    fail_n += 1
                    LOG(f'  ✗ {code}: {str(e)[:80]}')

        if (i + 1) % 50 == 0:
            LOG(f'  [{i+1}/{total}] ok={ok_n} none={none_n} fail={fail_n}')

        # 定期存盘
        if len(batch_buffer) >= BATCH_FLUSH:
            all_rows.extend(batch_buffer)
            batch_buffer = []
            merged = pd.concat(all_rows, ignore_index=True)
            merged = merged.drop_duplicates(
                subset=['stock_code', 'change_date'], keep='last'
            ).sort_values(['stock_code', 'change_date']).reset_index(drop=True)
            merged.to_parquet(OUTPUT, index=False)
            with open(PROGRESS, 'w') as f:
                json.dump(sorted(done), f)
            all_rows = [merged]  # 合成一个 frame 避免重复
            LOG(f'  💾 flush: {len(merged)} 行')

        time.sleep(REQUEST_INTERVAL)

    # 最后一次 flush
    if batch_buffer:
        all_rows.extend(batch_buffer)
    if all_rows:
        merged = pd.concat(all_rows, ignore_index=True)
        merged = merged.drop_duplicates(
            subset=['stock_code', 'change_date'], keep='last'
        ).sort_values(['stock_code', 'change_date']).reset_index(drop=True)
        merged.to_parquet(OUTPUT, index=False)

    with open(PROGRESS, 'w') as f:
        json.dump(sorted(done), f)

    LOG('=' * 60)
    LOG(f'完成: ok={ok_n} none={none_n} fail={fail_n}')
    LOG(f'输出: {OUTPUT}')
    if os.path.exists(OUTPUT):
        final = pd.read_parquet(OUTPUT)
        LOG(f'  最终 {len(final)} 行, {final["stock_code"].nunique()} 只')
        LOG(f'  最早: {final["change_date"].min()}  最晚: {final["change_date"].max()}')

if __name__ == '__main__':
    main()
