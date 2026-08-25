# -*- coding: utf-8 -*-
"""通达信分钟线每日增量

每交易日收盘后跑一次: 对每只股票拉 日线(校验基准)/5min/1min 三个频率,
只追加比库里最后一根更新的 bar, 原子重写 .csv.gz(与首批同格式)。
当场校验新增交易日: 根数(48/240) + 分钟聚合 OHLC/量额 vs 通达信日线
(容差与 build_minute_parquet.py 相同; D8 生产库交叉留给周度重建)。

窗口每交易日滑一格, gz 是唯一权威源; parquet 重建另行安排, 不在本脚本。

用法:
  python3 tdx_increment.py                            # 正常增量
  python3 tdx_increment.py --limit 5 --dry-run        # 冒烟自测

目录: 数据根目录取 ASTOCK_HOME(默认=本仓所在目录), 分钟线落 $ASTOCK_HOME/minute_data/,
      股票名单读 $ASTOCK_HOME/minute_data/codes.csv (由 refresh_codes_weekly.py 维护)。
退出码: 0=正常(含非交易日全市场零新增), 1=失败/不一致超阈值(供 cron 告警)
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
STATUS = f'{OUT}/increment_status.json'

# (频率名, pytdx category, 子目录, 每次请求根数, 深挖上限根数)
FREQS = [('daily', 4, 'tdx_daily', 100, 1600),
         ('5min', 0, 'tdx_5min', 800, 24800),
         ('1min', 8, 'tdx_1min', 800, 24800)]
BARS_PER_DAY = {'5min': 48, '1min': 240}
PRICE_TOL = 0.005          # 半分钱
PCT_TOL = 0.01             # 量/额相对容差 %
# 绝对容差: 分钟线 vol 以"手"为单位存储、日线精确到股 -> 聚合后恒差不足 1 手的零头,
# 低量股相对误差压不到 0.01% (实测 8-14 五只小盘股 ±0.010~0.011%)。取宽者放过取整零头。
ABS_TOL_VOL = 100.0        # 1 手
ABS_TOL_AMT = 1.0          # 1 元
# 通达信把"零成交"编码成 float32 denormal (5.877e-39) 而非干净的 0, `> 0` 判真会拿 0 除 0,
# 使全天停牌股恒定报出 48x / 240x (= 每日根数) 假不一致。零值判据必须带下限。
ZERO_EPS = 1.0             # 小于此值一律视为零成交, 跳过量/额对账
KEEP = ['datetime', 'open', 'high', 'low', 'close', 'vol', 'amount']

SERVERS = [('180.153.18.170', 7709), ('180.153.18.172', 80),
           ('202.108.253.139', 80), ('60.191.117.167', 7709)]

ALERT_FAIL = 50            # 抓取失败股数阈值
ALERT_MISM = 20            # 对账不一致(股,日)阈值

ap = argparse.ArgumentParser()
ap.add_argument('--limit', type=int, default=0)
ap.add_argument('--dry-run', action='store_true')
ap.add_argument('--deadline', default='23:00')
a = ap.parse_args()


def log(m):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {m}', flush=True)


hh, mm = map(int, a.deadline.split(':'))
dl = datetime.now().replace(hour=hh, minute=mm, second=0, microsecond=0)
if dl <= datetime.now():
    dl += timedelta(days=1)

codes = sorted(pd.read_csv(CODES_CSV, dtype={'stock_code': str})['stock_code'])
if a.limit:
    codes = codes[:a.limit]
log(f'增量: {len(codes)} 只 × {len(FREQS)} 频率  dry_run={a.dry_run}  硬停 {dl}')

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


def fetch_bars(cat, mkt, code, start, cnt):
    for attempt in range(4):
        try:
            return api.get_security_bars(cat, mkt, code, start, cnt)
        except Exception:
            time.sleep(0.4 * (attempt + 1))
            try:
                api.disconnect()
            except Exception:
                pass
            if not connect():
                time.sleep(5)
                connect()
    return None


def fetch_new(cat, mkt, code, last_dt, chunk, cap):
    """从最新往回翻块, 翻到与库存重叠即止。返回新 bar 列表(升序), 失败返回 None。"""
    got = []
    for start in range(0, cap, chunk):
        d = fetch_bars(cat, mkt, code, start, chunk)
        if d is None:
            return None
        if not d:
            break
        got = list(d) + got
        if d[0]['datetime'] <= last_dt:
            break
    return [x for x in got if x['datetime'] > last_dt]


if not connect():
    log('!! 无可用服务器, 退出')
    sys.exit(1)
log(f'连接 {SERVERS[si]}')

t0 = time.time()
stat = {f: {'upd': 0, 'no_new': 0, 'fail': 0, 'new_bars': 0} for f, _, _, _, _ in FREQS}
new_days = set()           # (freq, code, date) 汇总用
incomplete = []            # (freq, code, date, bars)
mism = []                  # (freq, code, date, 项)
fail_codes = set()

for i, code in enumerate(codes, 1):
    if datetime.now() >= dl:
        log(f'!! 硬停, 已处理 {i-1}/{len(codes)} (明日增量自动补)')
        break
    mkt = 1 if code.startswith(('6', '9')) else 0
    daily_df = None

    for freq, cat, sub, chunk, cap in FREQS:
        path = f'{OUT}/{sub}/{code}.csv.gz'
        try:
            old = pd.read_csv(path, dtype={'datetime': str}) if os.path.exists(path) \
                else pd.DataFrame(columns=KEEP)
        except Exception as e:
            log(f'  !! {code} {freq} 读库失败 {e}')
            stat[freq]['fail'] += 1
            fail_codes.add(code)
            continue
        last_dt = old['datetime'].max() if len(old) else ''

        new = fetch_new(cat, mkt, code, last_dt, chunk, cap)
        if new is None:
            stat[freq]['fail'] += 1
            fail_codes.add(code)
            log(f'  !! {code} {freq} 抓取失败')
            continue
        if not new:
            stat[freq]['no_new'] += 1
            if freq == 'daily':
                daily_df = old
            continue

        nd = pd.DataFrame(new).drop_duplicates(subset=['datetime']).sort_values('datetime')
        nd = nd[[c for c in KEEP if c in nd.columns]]
        merged = pd.concat([old, nd], ignore_index=True) \
                   .drop_duplicates(subset=['datetime']).sort_values('datetime')
        if not a.dry_run:
            tmp = path + '.writing'
            merged.to_csv(tmp, index=False, compression='gzip')
            os.replace(tmp, path)
        stat[freq]['upd'] += 1
        stat[freq]['new_bars'] += len(nd)

        if freq == 'daily':
            daily_df = merged
            continue

        # ---- 校验新增交易日: 根数 + 聚合 vs 日线 ----
        nd['dt'] = pd.to_datetime(nd['datetime'])
        nd['d'] = nd['dt'].dt.date
        ref = None
        if daily_df is not None and len(daily_df):
            ref = daily_df.copy()
            ref['d'] = pd.to_datetime(ref['datetime']).dt.date
            ref = ref.set_index('d')
        for d_, g in nd.groupby('d'):
            new_days.add((freq, code, str(d_)))
            if len(g) != BARS_PER_DAY[freq]:
                incomplete.append((freq, code, str(d_), len(g)))
                continue
            if ref is None or d_ not in ref.index:
                mism.append((freq, code, str(d_), 'no_daily_ref'))
                continue
            r = ref.loc[d_]
            bad = []
            if abs(g['open'].iloc[0] - r['open']) > PRICE_TOL: bad.append('open')
            if abs(g['high'].max() - r['high']) > PRICE_TOL: bad.append('high')
            if abs(g['low'].min() - r['low']) > PRICE_TOL: bad.append('low')
            if abs(g['close'].iloc[-1] - r['close']) > PRICE_TOL: bad.append('close')
            if r['vol'] >= ZERO_EPS:
                dv = abs(g['vol'].sum() - r['vol'])
                if dv > ABS_TOL_VOL and dv / r['vol'] * 100 > PCT_TOL:
                    bad.append('vol')
            elif g['vol'].sum() >= ZERO_EPS:
                bad.append('vol0')      # 日线零成交而分钟有量 -> 真异常, 不能被零值判据放过
            if r['amount'] >= ZERO_EPS:
                da = abs(g['amount'].sum() - r['amount'])
                if da > ABS_TOL_AMT and da / r['amount'] * 100 > PCT_TOL:
                    bad.append('amt')
            elif g['amount'].sum() >= ZERO_EPS:
                bad.append('amt0')
            if bad:
                mism.append((freq, code, str(d_), '+'.join(bad)))

    if i % 250 == 0:
        el = time.time() - t0
        log(f'  {i}/{len(codes)}  {i/el*60:.0f} 只/分  预计还需 {(len(codes)-i)/(i/el)/60:.0f} 分')

api.disconnect()

# ---------------- 汇总 ----------------
el = time.time() - t0
log('=' * 60)
for freq, _, _, _, _ in FREQS:
    s = stat[freq]
    log(f'{freq:>5}: 更新 {s["upd"]}  无新增 {s["no_new"]}  失败 {s["fail"]}  '
        f'新增 {s["new_bars"]:,} 根')
days_by_freq = {}
for freq, code, d_ in new_days:
    days_by_freq.setdefault(freq, set()).add(d_)
for freq, ds in sorted(days_by_freq.items()):
    log(f'{freq} 新增交易日: {sorted(ds)}')
if incomplete:
    log(f'⚠️ 根数不足 {len(incomplete)} 个(股,日), 前5: {incomplete[:5]}')
if mism:
    log(f'⚠️ 对账不一致 {len(mism)} 个(股,日), 前5: {mism[:5]}')

total_new = sum(s['new_bars'] for s in stat.values())
n_fail = len(fail_codes)
alert = (n_fail > ALERT_FAIL) or (len(mism) > ALERT_MISM)
if total_new == 0 and n_fail == 0:
    log('全市场零新增 → 非交易日, 正常退出')

if not a.dry_run:
    json.dump({'ts': datetime.now().strftime('%F %T'), 'total_new_bars': total_new,
               'fail': n_fail, 'incomplete': len(incomplete), 'mism': len(mism),
               'alert': alert,
               'days': {f: sorted(ds) for f, ds in days_by_freq.items()}},
              open(STATUS, 'w'), indent=1, ensure_ascii=False)

log(f'增量结束, 耗时 {el/60:.1f} 分  {"⚠️ ALERT" if alert else "OK"}')
sys.exit(1 if alert else 0)
