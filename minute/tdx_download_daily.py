# -*- coding: utf-8 -*-
"""通达信不复权日线全市场下载 (作分钟线校验的金标准)
已验证: 通达信日线 == baostock 日线 == 生产 daily_ohlcv, 104,948 个股日零差异
"""
import json, os, sys, time
from datetime import datetime, timedelta
import pandas as pd
from pytdx.hq import TdxHq_API

BASE = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, 'minute_data')
DDIR = f'{OUT}/tdx_daily'
CAT_DAY, MAXCNT, NEED = 4, 800, 1600      # 1600 根 ≈ 6.5 年, 覆盖 2 年分钟区间绰绰有余
SERVERS = [('180.153.18.170', 7709), ('180.153.18.172', 80),
           ('202.108.253.139', 80), ('60.191.117.167', 7709)]
os.makedirs(DDIR, exist_ok=True)


def log(m):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {m}', flush=True)


dl = datetime.now().replace(hour=1, minute=0, second=0, microsecond=0)
if dl <= datetime.now():
    dl += timedelta(days=1)

codes = sorted(pd.read_csv(f'{OUT}/codes.csv', dtype={'stock_code': str})['stock_code'])
todo = [c for c in codes if not os.path.exists(f'{DDIR}/{c}.csv.gz')]
log(f'日线: 共 {len(codes)} 只, 待抓 {len(todo)} 只')

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
    sys.exit('无可用服务器')

t0, ok, fail, errs = time.time(), 0, 0, []
KEEP = ['datetime', 'open', 'high', 'low', 'close', 'vol', 'amount']
for i, code in enumerate(todo, 1):
    if datetime.now() >= dl:
        log('!! 硬停')
        break
    mkt = 1 if code.startswith(('6', '9')) else 0
    got, bad = [], False
    for start in range(0, NEED, MAXCNT):
        d = None
        for attempt in range(4):
            try:
                d = api.get_security_bars(CAT_DAY, mkt, code, start, MAXCNT)
                break
            except Exception:
                time.sleep(0.4 * (attempt + 1))
                try:
                    api.disconnect()
                except Exception:
                    pass
                connect()
        if d is None:
            bad = True
            break
        if not d:
            break
        got.extend(d)
    if bad or not got:
        fail += 1
        errs.append(code)
        continue
    df = pd.DataFrame(got).drop_duplicates(subset=['datetime']).sort_values('datetime')
    tmp = f'{DDIR}/{code}.csv.gz.writing'
    df[[c for c in KEEP if c in df.columns]].to_csv(tmp, index=False, compression='gzip')
    os.replace(tmp, f'{DDIR}/{code}.csv.gz')
    ok += 1
    if ok % 500 == 0:
        el = time.time() - t0
        log(f'  {i}/{len(todo)}  {ok/el*60:.0f} 只/分  预计还需 {(len(todo)-i)/(ok/el*60):.0f} 分')
api.disconnect()
log(f'日线结束: 成功 {ok} 失败 {fail} 耗时 {(time.time()-t0)/60:.1f} 分')
if errs:
    json.dump(errs, open(f'{OUT}/tdx_daily_errors.json', 'w'), indent=1)
    log(f'失败 {len(errs)} 只 -> tdx_daily_errors.json')
