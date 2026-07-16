"""
全市场 cninfo 股本变动下载 (5016 只 A 股)
- 4 线程并发
- 每只股 retry 3 次
- 清洗成 [stock_code, change_date, reason, total_share] 四列
输出: 脚本目录下 cninfo_share_change_full.parquet (可用环境变量 ASTOCK_HOME 覆盖目录)
"""
import os, sys, io, time, threading
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
def LOG(m): print(m, flush=True)

SCRIPT_DIR = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.abspath(__file__))

import akshare as ak
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. 拿全市场 A 股清单
LOG('=== 1. 拿全 A 股清单 ===')
try:
    df_list = ak.stock_info_a_code_name()
    LOG(f'  stock_info_a_code_name 返回 {len(df_list)} 行, 列: {df_list.columns.tolist()}')
    codes = df_list['code'].astype(str).tolist()
except Exception as e:
    LOG(f'  ✗ {e}')
    sys.exit(1)

# 去重 + 排序
codes = sorted(set(codes))
LOG(f'  唯一股票数: {len(codes)}')
LOG(f'  样例: {codes[:5]} ... {codes[-5:]}')

# 2. 下载函数
def fetch_one(code):
    for retry in range(3):
        try:
            df = ak.stock_share_change_cninfo(symbol=code, start_date='20200101', end_date='20260501')
            if df is None or len(df) == 0:
                return code, None, 'empty'
            df = df[['证券代码', '变动日期', '变动原因', '总股本']].copy()
            df.columns = ['stock_code', 'change_date', 'reason', 'total_share_wan']
            df['stock_code'] = code
            df['total_share'] = pd.to_numeric(df['total_share_wan'], errors='coerce') * 10000
            df = df[['stock_code', 'change_date', 'reason', 'total_share']]
            return code, df, None
        except Exception as e:
            if retry < 2:
                time.sleep(1)
            else:
                return code, None, str(e)[:80]

# 3. 并发下载
LOG(f'\n=== 2. 开始下载 (4 线程) ===')
t0 = time.time()
results = []
failures = []
empties = []
lock = threading.Lock()
done_count = [0]
total = len(codes)

def on_done(fut):
    with lock:
        done_count[0] += 1
        if done_count[0] % 100 == 0 or done_count[0] == total:
            elapsed = time.time() - t0
            rate = done_count[0] / elapsed
            eta = (total - done_count[0]) / rate if rate > 0 else 0
            LOG(f'  [{done_count[0]:4}/{total}]  ok={len(results):4}  fail={len(failures):3}  empty={len(empties):3}  用时 {elapsed:.0f}s  速度 {rate:.1f}/s  ETA {eta:.0f}s')

with ThreadPoolExecutor(max_workers=4) as ex:
    futs = {ex.submit(fetch_one, c): c for c in codes}
    for fut in as_completed(futs):
        code, df, err = fut.result()
        with lock:
            if df is not None:
                results.append(df)
            elif err == 'empty':
                empties.append(code)
            else:
                failures.append((code, err))
        on_done(fut)

elapsed = time.time() - t0
LOG(f'\n=== 3. 下载完成 总用时 {elapsed:.0f}s ({elapsed/60:.1f} min) ===')
LOG(f'  成功: {len(results)}  空: {len(empties)}  失败: {len(failures)}')

# 4. 合并保存
LOG(f'\n=== 4. 合并保存 ===')
if results:
    full = pd.concat(results, ignore_index=True)
    full['change_date'] = pd.to_datetime(full['change_date'], errors='coerce')
    full = full.dropna(subset=['change_date', 'total_share'])
    full = full.sort_values(['stock_code', 'change_date']).reset_index(drop=True)
    LOG(f'  合计 {len(full)} 行, {full["stock_code"].nunique()} 只股')
    LOG(f'  日期范围: {full["change_date"].min()} ~ {full["change_date"].max()}')
    out_path = os.path.join(SCRIPT_DIR, 'cninfo_share_change_full.parquet')
    full.to_parquet(out_path, engine='pyarrow', index=False)
    LOG(f'  ✓ 已保存 {out_path}')
else:
    LOG('  ⚠ 无数据')

# 5. 失败清单
if failures:
    LOG(f'\n=== 失败 {len(failures)} 只 (前 30) ===')
    for c, e in failures[:30]:
        LOG(f'  {c}: {e}')
    pd.DataFrame(failures, columns=['code','error']).to_csv(os.path.join(SCRIPT_DIR, 'cninfo_full_failures.csv'), index=False)
if empties:
    LOG(f'\n=== 空数据 {len(empties)} 只 (前 20) ===')
    LOG(f'  {empties[:20]}')

LOG('\n✓ DONE')
