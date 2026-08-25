# -*- coding: utf-8 -*-
"""
通达信分钟线全市场下载器 (历史首刷)

数据源: 通达信行情服务器 (pytdx), 不复权
深度  : 5min ≈ 488 交易日(2.01年) / 1min ≈ 90 交易日
        ⚠️ 服务端只保留这么多, 再往前是滚动丢弃的 —— 想要更长的历史,
        只能靠 tdx_increment.py 每个交易日跑一次, 自己往后攒。
输出  : 每股一个 .csv.gz (无 pyarrow 依赖), 后续由 build_minute_parquet.py 转 parquet
安全  : 断点续传 / 硬停 / 多节点轮换 / 原子落盘 / 只写 minute_data 目录

用法:
  python tdx_download.py 5min            # 全市场
  python tdx_download.py 5min --limit 20 # 小批
  python tdx_download.py 1min
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
from pytdx.hq import TdxHq_API

BASE = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, 'minute_data')
CODES_CSV = f'{OUT}/codes.csv'

CAT = {'5min': 0, '1min': 8}
DEPTH = {'5min': 24800, '1min': 24800}      # 略大于实测上限, 拉满为止
MAXCNT = 800
SERVERS = [('180.153.18.170', 7709), ('180.153.18.172', 80),
           ('202.108.253.139', 80), ('60.191.117.167', 7709)]

ap = argparse.ArgumentParser()
ap.add_argument('freq', choices=['5min', '1min'])
ap.add_argument('--limit', type=int, default=0)
ap.add_argument('--deadline', default='01:00')
a = ap.parse_args()

FREQ = a.freq
DDIR = f'{OUT}/tdx_{FREQ}'
os.makedirs(DDIR, exist_ok=True)


def log(m):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {m}', flush=True)


hh, mm = map(int, a.deadline.split(':'))
now = datetime.now()
dl = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
if dl <= now:
    dl += timedelta(days=1)
log(f'硬停 {dl}  (还有 {(dl-now).total_seconds()/3600:.1f} 小时)')

codes = sorted(pd.read_csv(CODES_CSV, dtype={'stock_code': str})['stock_code'])
if a.limit:
    codes = codes[:a.limit]
todo = [c for c in codes if not os.path.exists(f'{DDIR}/{c}.csv.gz')]
log(f'{FREQ}: 共 {len(codes)} 只, 已有 {len(codes)-len(todo)} 只, 待抓 {len(todo)} 只')

api = TdxHq_API(heartbeat=True)
si = 0


def connect():
    global si
    for k in range(len(SERVERS) * 2):
        ip, port = SERVERS[(si + k) % len(SERVERS)]
        try:
            if api.connect(ip, port, time_out=8):
                si = (si + k) % len(SERVERS)
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


if not connect():
    log('!! 无可用服务器, 退出')
    sys.exit(1)
log(f'连接 {SERVERS[si]}')

t0 = time.time()
ok = fail = 0
errors = []
KEEP = ['datetime', 'open', 'high', 'low', 'close', 'vol', 'amount']

for i, code in enumerate(todo, 1):
    if datetime.now() >= dl:
        log(f'!! 到达硬停时刻, 剩余 {len(todo)-i+1} 只未抓 (可续跑)')
        break
    mkt = 1 if code.startswith(('6', '9')) else 0
    got = []
    bad = False
    for start in range(0, DEPTH[FREQ], MAXCNT):
        d = None
        for attempt in range(4):
            try:
                d = api.get_security_bars(CAT[FREQ], mkt, code, start, MAXCNT)
                break
            except Exception:
                time.sleep(0.4 * (attempt + 1))
                try:
                    api.disconnect()
                except Exception:
                    pass
                if not connect():
                    time.sleep(5)
                    connect()
        if d is None:
            bad = True
            break
        if not d:
            break
        got.extend(d)
    if bad or not got:
        fail += 1
        errors.append(code)
        log(f'  !! {code} 抓取失败')
        continue
    df = pd.DataFrame(got).drop_duplicates(subset=['datetime']).sort_values('datetime')
    cols = [c for c in KEEP if c in df.columns]
    tmp = f'{DDIR}/{code}.csv.gz.writing'
    df[cols].to_csv(tmp, index=False, compression='gzip')
    os.replace(tmp, f'{DDIR}/{code}.csv.gz')
    ok += 1
    if ok % 100 == 0:
        el = time.time() - t0
        r = ok / el * 60
        left = (len(todo) - i) / r if r else 0
        log(f'  {i}/{len(todo)}  {r:.0f} 只/分  已用 {el/60:.0f} 分  预计还需 {left:.0f} 分  '
            f'失败 {fail}')

api.disconnect()
el = time.time() - t0
log(f'\n{FREQ} 结束: 成功 {ok} 失败 {fail} 耗时 {el/60:.1f} 分')
if errors:
    json.dump(errors, open(f'{OUT}/tdx_{FREQ}_errors.json', 'w'), indent=1)
    log(f'失败清单 -> tdx_{FREQ}_errors.json  前10: {errors[:10]}')
n = len([f for f in os.listdir(DDIR) if f.endswith('.csv.gz')])
sz = sum(os.path.getsize(f'{DDIR}/{f}') for f in os.listdir(DDIR) if f.endswith('.csv.gz'))
log(f'目录现有 {n} 个文件, 合计 {sz/1024**3:.2f} GB')
