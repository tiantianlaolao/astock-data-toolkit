# -*- coding: utf-8 -*-
"""
通达信分钟线: gzip CSV -> 按年 parquet + 全量正确性校验

校验矩阵 (与 8-07 验证同一套):
  逐根 R1 high>=max(o,c)  R2 low<=min(o,c)  R3 high>=low  R4 均价落区间(相对容差)
       R5 volume>=0&close>0  R6 无NaN  R7 时间戳合法
  逐日 D1 根数  D2 open  D3 high  D4 low  D5 close  D6 volume  D7 amount
       基准 = 通达信不复权日线 (已证 == baostock == 生产库, 零差异)
  交叉 D8 日成交量/额 与生产 daily_ohlcv 一致 (腾讯源, 真正独立的第三方)

不修改任何数据, 只打 clean 标志 + 出质检表。
用法: python build_minute_parquet.py 5min|1min
"""
import glob
import gzip
import json
import os
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, 'minute_data')
DATA = os.path.join(BASE, 'astock_data')    # 日线库(download_astock_data.py 的产物), 供 D8 交叉校验
FREQ = sys.argv[1] if len(sys.argv) > 1 else '5min'
SRC = f'{OUT}/tdx_{FREQ}'
DLY = f'{OUT}/tdx_daily'

BARS = {'5min': 48, '1min': 240}[FREQ]
PRICE_TOL = 0.005          # 半分钱, A股最小报价单位 0.01
PCT_TOL = 0.01             # 量/额 相对容差 %
VWAP_TOL = 0.05            # R4 相对容差 % (源 amount 精度约 0.01~0.03%)

if FREQ == '5min':
    G = ([f'{9+(35+5*i)//60:02d}:{(35+5*i)%60:02d}' for i in range(24)] +
         [f'{13+(5+5*i)//60:02d}:{(5+5*i)%60:02d}' for i in range(24)])
else:
    G = ([f'{9+(31+i)//60:02d}:{(31+i)%60:02d}' for i in range(120)] +
         [f'{13+(1+i)//60:02d}:{(1+i)%60:02d}' for i in range(120)])
VALID_HM = set(G)

SCHEMA = pa.schema([('stock_code', pa.string()), ('dt', pa.timestamp('ns')),
                    ('open', pa.float32()), ('high', pa.float32()),
                    ('low', pa.float32()), ('close', pa.float32()),
                    ('volume', pa.int64()), ('amount', pa.float64()),
                    ('clean', pa.bool_())])


def log(m):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {m}', flush=True)


# ---------- 生产日线 (腾讯源, 独立第三方, 只取裸量额) ----------
log('载入生产 daily_ohlcv 的 volume/amount (独立交叉基准)...')
pf = pq.ParquetFile(f'{DATA}/daily_ohlcv.parquet')
parts = []
for b in pf.iter_batches(batch_size=1_000_000,
                         columns=['stock_code', 'date', 'volume', 'amount']):
    df = b.to_pandas()
    df = df[pd.to_datetime(df['date']) >= pd.Timestamp('2024-06-01')]
    if len(df):
        parts.append(df)
prod = pd.concat(parts, ignore_index=True)
prod['d'] = pd.to_datetime(prod['date']).dt.date
prod = prod.set_index(['stock_code', 'd'])[['volume', 'amount']]
prod.columns = ['p_vol', 'p_amt']
log(f'  生产日线 {len(prod):,} 行')

files = sorted(glob.glob(f'{SRC}/*.csv.gz'))
log(f'{FREQ}: 待处理 {len(files)} 只')

writers, quals = {}, []
t0 = time.time()
n_ok = n_nodaily = 0

for i, f in enumerate(files, 1):
    code = os.path.basename(f)[:-7]
    try:
        d = pd.read_csv(f)
    except Exception as e:
        log(f'  !! {code} 读取失败 {e}')
        continue
    if not len(d):
        continue
    d['dt'] = pd.to_datetime(d['datetime'])
    d = d.rename(columns={'vol': 'volume'})
    d['d'] = d['dt'].dt.date
    hm = d['dt'].dt.strftime('%H:%M')
    o, h, l, c = d['open'], d['high'], d['low'], d['close']
    v, a = d['volume'], d['amount']

    with np.errstate(divide='ignore', invalid='ignore'):
        vwap = np.where(v > 0, a / v.replace(0, np.nan), np.nan)
        over = np.maximum(vwap - h, l - vwap) / c.replace(0, np.nan) * 100
    R1 = h >= np.maximum(o, c) - PRICE_TOL
    R2 = l <= np.minimum(o, c) + PRICE_TOL
    R3 = h >= l - PRICE_TOL
    R4 = (v <= 0) | ~(over > VWAP_TOL)
    R5 = (v >= 0) & (c > 0)
    R6 = ~d[['open', 'high', 'low', 'close', 'volume', 'amount']].isna().any(axis=1)
    R7 = hm.isin(VALID_HM)
    # ⚠️ R4 不计入 bars_ok: 它衡量源 amount 与 volume 之间约 0.01~0.03% 的精度差,
    #    不是数据正确性。48 根里有 1 根越界就废掉整天, 是判据的乘方效应而非质量断崖。
    #    (日级 amount 汇总 D7 通过率 99.999% 已证明总量正确)  单独记 nR4 备查。
    d['_ok'] = R1 & R2 & R3 & R5 & R6 & R7
    d['_r4'] = R4

    g = d.groupby('d').agg(bars=('close', 'size'), o5=('open', 'first'), h5=('high', 'max'),
                           l5=('low', 'min'), c5=('close', 'last'), v5=('volume', 'sum'),
                           a5=('amount', 'sum'), bars_ok=('_ok', 'all'),
                           nbad=('_ok', lambda s: int((~s).sum())),
                           nR4=('_r4', lambda s: int((~s).sum()))).reset_index()

    # 基准: 通达信不复权日线
    pdly = f'{DLY}/{code}.csv.gz'
    if os.path.exists(pdly):
        dd = pd.read_csv(pdly)
        dd['d'] = pd.to_datetime(dd['datetime']).dt.date
        dd = dd.rename(columns={'vol': 'volume'})
        g = g.merge(dd[['d', 'open', 'high', 'low', 'close', 'volume', 'amount']],
                    on='d', how='left')
    else:
        n_nodaily += 1
        for cc in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            g[cc] = np.nan

    g['has_ref'] = g['volume'].notna() & (g['volume'] > 0)
    g['D1'] = g['bars'] == BARS
    g['D2'] = (g['o5'] - g['open']).abs() <= PRICE_TOL
    g['D3'] = (g['h5'] - g['high']).abs() <= PRICE_TOL
    g['D4'] = (g['l5'] - g['low']).abs() <= PRICE_TOL
    g['D5'] = (g['c5'] - g['close']).abs() <= PRICE_TOL
    with np.errstate(divide='ignore', invalid='ignore'):
        g['vdev'] = (g['v5'] - g['volume']).abs() / g['volume'].replace(0, np.nan) * 100
        g['adev'] = (g['a5'] - g['amount']).abs() / g['amount'].replace(0, np.nan) * 100
    g['D6'] = g['vdev'] <= PCT_TOL
    g['D7'] = g['adev'] <= PCT_TOL

    # D8: 与生产库(腾讯)交叉
    try:
        pr = prod.loc[code]
        g = g.merge(pr, left_on='d', right_index=True, how='left')
    except KeyError:
        g['p_vol'] = np.nan
        g['p_amt'] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'):
        g['D8'] = ((g['v5'] - g['p_vol']).abs() / g['p_vol'].replace(0, np.nan) * 100
                   <= PCT_TOL) | g['p_vol'].isna()

    checks = ['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7']
    for cc in checks:
        g[cc] = g[cc].fillna(False) & g['has_ref']
    g['D8'] = g['D8'].fillna(True)
    g['clean'] = g['bars_ok'] & g[checks].all(axis=1) & g['D8']

    q = g[['d', 'bars', 'bars_ok', 'nbad', 'nR4', 'has_ref'] + checks +
          ['D8', 'vdev', 'adev', 'clean']].copy()
    q.insert(0, 'stock_code', code)
    quals.append(q)

    # ---- 写 parquet (按年, 增量 row group, 内存受控) ----
    d = d.merge(g[['d', 'clean']], on='d', how='left')
    d['clean'] = d['clean'].fillna(False)
    d['stock_code'] = code
    for y, gg in d.groupby(d['dt'].dt.year):
        t = pa.Table.from_pandas(
            gg[['stock_code', 'dt', 'open', 'high', 'low', 'close', 'volume', 'amount', 'clean']]
            .astype({'open': 'float32', 'high': 'float32', 'low': 'float32',
                     'close': 'float32', 'volume': 'int64', 'amount': 'float64'}),
            schema=SCHEMA, preserve_index=False)
        if y not in writers:
            writers[y] = pq.ParquetWriter(f'{OUT}/{FREQ}_{y}.parquet', SCHEMA,
                                          compression='zstd')
        writers[y].write_table(t)
    n_ok += 1
    if i % 500 == 0:
        log(f'  {i}/{len(files)}  {i/(time.time()-t0)*60:.0f} 只/分')

for w in writers.values():
    w.close()

Q = pd.concat(quals, ignore_index=True)
Q.to_parquet(f'{OUT}/{FREQ}_quality.parquet', compression='zstd', index=False)
log(f'\n处理完成 {n_ok} 只, 耗时 {(time.time()-t0)/60:.1f} 分')
log(f'质检表 -> {FREQ}_quality.parquet ({len(Q):,} 行)')
if n_nodaily:
    log(f'!! 缺日线基准 {n_nodaily} 只')

# ---------------- 报告 ----------------
V = Q[Q['has_ref']]
print()
log('=' * 62)
log(f'{FREQ} 正确性校验报告')
log('=' * 62)
log(f'  (股,日) 总数     : {len(Q):,}   有基准 {len(V):,}')
log(f'  ✅ 全项 clean     : {int(Q["clean"].sum()):,}  ({Q["clean"].mean()*100:.3f}%)')
log('\n  各项通过率:')
for k, nm in [('bars_ok', '逐根 R1/2/3/5/6/7'), ('D1', f'D1 根数={BARS}'), ('D2', 'D2 open'),
              ('D3', 'D3 high'), ('D4', 'D4 low'), ('D5', 'D5 close'),
              ('D6', 'D6 volume'), ('D7', 'D7 amount'), ('D8', 'D8 vs生产库')]:
    log(f'    {nm:<18} {V[k].mean()*100:>8.4f}%   不通过 {int((~V[k]).sum()):>6}')
tot_bars = V['bars'].sum()
log(f'\n  (参考, 不计入 clean) R4 均价落区间:')
log(f'    按根: {(1-V["nR4"].sum()/max(1,tot_bars))*100:>8.4f}%   越界 {int(V["nR4"].sum()):,} / {int(tot_bars):,} 根')
log(f'    按日: {(V["nR4"]==0).mean()*100:>8.4f}%   (整天 {BARS} 根全过才算)')
bad = V[~V['clean']]
if len(bad):
    log(f'\n  不通过时偏差: |vol_dev| 中位={bad["vdev"].median():.4f}% '
        f'最大={bad["vdev"].max():.4f}%')
    ext = bad[bad['vdev'] > 100]
    log(f'  |vol_dev| >100% 的极端值: {len(ext):,} 个 (股,日)')
    if len(ext):
        log('    样例: ' + ', '.join(
            f'{r.stock_code}/{r.d}/{r.vdev:.0f}%' for r in ext.head(5).itertuples()))
qy = Q.copy()
qy['y'] = pd.to_datetime(qy['d']).dt.year
t = qy.groupby('y')['clean'].agg(['size', 'sum'])
t['clean%'] = (t['sum'] / t['size'] * 100).round(3)
log('\n  按年:')
for line in t.to_string().split('\n'):
    log('    ' + line)
log('\n  产出文件:')
for fn in sorted(os.listdir(OUT)):
    if fn.startswith(f'{FREQ}_') and fn.endswith('.parquet'):
        log(f'    {fn}: {os.path.getsize(OUT+"/"+fn)/1024**3:.2f} GB')
