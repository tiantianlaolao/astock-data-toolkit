"""
A股 15 年历史数据 backfill 脚本
================================
补齐 2011-04-01 ~ 2021-04-11 这 10 年历史数据, 不动现有 5 年 parquet.

用法:
  python backfill_15y.py --table ohlcv    [--resume] [--limit N]
  python backfill_15y.py --table valuation [--resume] [--limit N]
  python backfill_15y.py --table financial [--resume] [--limit N]
  python backfill_15y.py --table dividend  [--resume] [--limit N]
  python backfill_15y.py --table index     [--resume]

输出 (全部写到 backfill_data/, 不碰生产):
  backfill_data/daily_ohlcv_backfill.parquet
  backfill_data/valuation_daily_backfill.parquet
  backfill_data/financial_quarterly_backfill.parquet
  backfill_data/dividend_history_backfill.parquet
  backfill_data/index_daily_backfill.parquet
  backfill_data/progress_backfill.json
  backfill_data/errors_backfill.json

合并到生产 parquet 由单独的 merge_backfill.py 处理.
"""

import os
import sys
import json
import time
import argparse
import warnings
import io
import signal
from datetime import datetime, timedelta

# ============ 编码/代理 ============
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
for var in ['http_proxy', 'https_proxy', 'all_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']:
    if var in os.environ:
        del os.environ[var]
os.environ['NO_PROXY'] = '*'

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

warnings.filterwarnings('ignore')

# ============ 配置 ============
SCRIPT_DIR = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROD_DIR = os.path.join(SCRIPT_DIR, 'astock_data')   # 生产 parquet 目录 (只读)
BACKFILL_DIR = os.path.join(SCRIPT_DIR, 'backfill_data')
CNINFO_PATH = os.path.join(SCRIPT_DIR, 'cninfo_share_change_full.parquet')
STOCK_LIST_PATH = os.path.join(PROD_DIR, 'stock_list.parquet')
PROGRESS_FILE = os.path.join(BACKFILL_DIR, 'progress_backfill.json')
ERRORS_FILE = os.path.join(BACKFILL_DIR, 'errors_backfill.json')
HEARTBEAT_FILE = os.path.join(BACKFILL_DIR, 'heartbeat.json')

# Backfill 时间范围 (15 年 = 现在-15年 到 现有数据起点前一天)
START_DATE = '2011-04-01'
END_DATE = '2021-04-11'       # 现有 parquet 从 2021-04-12 开始, backfill 到 4-11 结束

# 限频 / 重试
REQUEST_INTERVAL = 0.4
MAX_RETRIES = 3
MERGE_BATCH_SIZE = 50   # 比原来 200 小, 压内存峰值

os.makedirs(BACKFILL_DIR, exist_ok=True)


# ============ 工具函数 ============
def LOG(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def heartbeat(phase, **extra):
    data = {
        'ts': datetime.now().isoformat(),
        'phase': phase,
        'pid': os.getpid(),
        **extra,
    }
    try:
        with open(HEARTBEAT_FILE, 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def code_to_tx_symbol(code):
    return f'sh{code}' if code.startswith('6') else f'sz{code}'


def code_to_bs_symbol(code):
    if code.startswith(('6', '5', '9')):
        return f'sh.{code}'
    return f'sz.{code}'


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_progress(progress):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def load_errors():
    if os.path.exists(ERRORS_FILE):
        with open(ERRORS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_errors(errors):
    with open(ERRORS_FILE, 'w', encoding='utf-8') as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)


def mark_done(progress, key, code):
    progress.setdefault(key, []).append(code)
    save_progress(progress)


def record_err(errors, key, code, msg):
    errors.setdefault(key, {})[code] = str(msg)[:200]
    save_errors(errors)


def flexible_rename(df, mapping):
    rename_dict = {}
    for target, cands in mapping.items():
        if isinstance(cands, str):
            cands = [cands]
        for c in cands:
            if c in df.columns:
                rename_dict[c] = target
                break
    return df.rename(columns=rename_dict)


def merge_temp_files(temp_dir, output_path, sort_columns):
    """分批合并 temp parquet → 单 output parquet"""
    temp_files = sorted([
        os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.parquet')
    ])
    if not temp_files:
        LOG(f'  ⚠ 无 temp 文件可合并 ({temp_dir})')
        return
    total = len(temp_files)
    LOG(f'  合并 {total} 个 temp 文件 (每批 {MERGE_BATCH_SIZE})...')

    all_tables = []
    for i in range(0, total, MERGE_BATCH_SIZE):
        batch = temp_files[i:i + MERGE_BATCH_SIZE]
        batch_tables = []
        for tf in batch:
            try:
                batch_tables.append(pq.read_table(tf))
            except Exception as e:
                LOG(f'    ⚠ 损坏: {os.path.basename(tf)}: {e}')
        if batch_tables:
            all_tables.append(pa.concat_tables(batch_tables, promote_options='permissive'))
        LOG(f'    批 {i+1}-{min(i+MERGE_BATCH_SIZE, total)}/{total} done')

    if not all_tables:
        LOG(f'  ❌ 无合并结果')
        return

    final = pa.concat_tables(all_tables, promote_options='permissive')
    df = final.to_pandas()
    df = df.sort_values(sort_columns).drop_duplicates(subset=sort_columns, keep='last').reset_index(drop=True)
    df.to_parquet(output_path, index=False, engine='pyarrow')
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    LOG(f'  ✅ {len(df)} 行, {size_mb:.1f} MB → {output_path}')


# ============ OHLCV Backfill ============
def backfill_ohlcv(resume=False, limit=0):
    import akshare as ak
    LOG('=' * 60)
    LOG(f'OHLCV backfill  {START_DATE} ~ {END_DATE}')
    LOG('=' * 60)

    stock_list = pd.read_parquet(STOCK_LIST_PATH)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    temp_dir = os.path.join(BACKFILL_DIR, 'temp_ohlcv')
    output_path = os.path.join(BACKFILL_DIR, 'daily_ohlcv_backfill.parquet')
    os.makedirs(temp_dir, exist_ok=True)

    progress = load_progress()
    errors = load_errors()
    completed = set(progress.get('ohlcv', [])) if resume else set()
    if completed:
        LOG(f'  断点续传: 已完成 {len(completed)}/{total}')

    ok_n = len(completed)
    fail_n = 0
    pre_ipo_n = 0

    start_yyyymmdd = START_DATE.replace('-', '')
    end_yyyymmdd = END_DATE.replace('-', '')

    for i, code in enumerate(codes):
        if code in completed:
            continue

        heartbeat('ohlcv', i=i+1, total=total, code=code, ok=ok_n, fail=fail_n, pre_ipo=pre_ipo_n)

        tx = code_to_tx_symbol(code)
        ok = False
        is_pre_ipo = False
        for attempt in range(MAX_RETRIES):
            try:
                df = ak.stock_zh_a_daily(
                    symbol=tx,
                    start_date=start_yyyymmdd, end_date=end_yyyymmdd,
                    adjust='qfq'
                )
                if df is None or df.empty:
                    # 该股票在这段时间尚未上市 (pre-IPO) — 正常情况
                    is_pre_ipo = True
                    ok = True
                    break
                df['stock_code'] = code
                df['date'] = pd.to_datetime(df['date'])
                df['turnover_rate'] = df['turnover'] * 100 if 'turnover' in df.columns else None
                df = df.sort_values('date').reset_index(drop=True)
                df['pct_change'] = df['close'].pct_change() * 100
                df = df[['stock_code', 'date', 'open', 'high', 'low', 'close',
                         'volume', 'amount', 'turnover_rate', 'pct_change']]
                df.to_parquet(os.path.join(temp_dir, f'{code}.parquet'), index=False)
                ok = True
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                else:
                    record_err(errors, 'ohlcv', code, e)

        if ok:
            if is_pre_ipo:
                pre_ipo_n += 1
            ok_n += 1
            mark_done(progress, 'ohlcv', code)
        else:
            fail_n += 1

        if (i + 1) % 50 == 0:
            LOG(f'  [{i+1}/{total}] ok={ok_n} fail={fail_n} pre_ipo={pre_ipo_n}')

        time.sleep(REQUEST_INTERVAL)

    LOG(f'OHLCV 下载阶段完成: ok={ok_n} fail={fail_n} pre_ipo={pre_ipo_n}')
    heartbeat('ohlcv_merging', total=total, ok=ok_n, fail=fail_n)
    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'date'])
    heartbeat('ohlcv_done', total=total, ok=ok_n, fail=fail_n)


# ============ Valuation Backfill (V4 逻辑) ============
def _fetch_daily_bs(bs, bs_code):
    """baostock 拉 close+pe/pb/ps/pcf"""
    rs = bs.query_history_k_data_plus(
        bs_code, 'date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM',
        start_date=START_DATE, end_date=END_DATE,
        frequency='d', adjustflag='3'
    )
    if rs.error_code != '0':
        return None, f'{rs.error_code}: {rs.error_msg}'
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame(), None
    df = pd.DataFrame(rows, columns=rs.fields)
    df['date'] = pd.to_datetime(df['date'])
    for c in ['close', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df, None


def _fetch_shares_bs(bs, bs_code, start_year=2011):
    """baostock 季报拉 totalShare"""
    rows = []
    for year in range(start_year, 2022):  # backfill 到 2021 够了
        for q in range(1, 5):
            try:
                rs = bs.query_profit_data(code=bs_code, year=year, quarter=q)
                if rs.error_code != '0':
                    continue
                while rs.next():
                    rows.append(dict(zip(rs.fields, rs.get_row_data())))
            except Exception:
                continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['pubDate'] = pd.to_datetime(df['pubDate'], errors='coerce')
    df['totalShare'] = pd.to_numeric(df['totalShare'], errors='coerce')
    df = df.dropna(subset=['pubDate', 'totalShare']).sort_values('pubDate').reset_index(drop=True)
    return df


def _fix_cninfo_div(cninfo_df, div_events):
    if len(cninfo_df) == 0 or len(div_events) == 0:
        return cninfo_df
    cninfo_df = cninfo_df.copy().sort_values('change_date').reset_index(drop=True)
    for _, ev in div_events.iterrows():
        bonus = float(ev.get('bonus_shares', 0) or 0)
        transfer = float(ev.get('transfer_shares', 0) or 0)
        split_ratio = (bonus + transfer) / 10.0
        if split_ratio <= 0:
            continue
        ex_date = pd.Timestamp(ev['ex_dividend_date'])
        after = cninfo_df[cninfo_df['change_date'] >= ex_date]
        if len(after) == 0:
            continue
        post_share = after['total_share'].iloc[0]
        before_mask = cninfo_df['change_date'] < ex_date
        before_diff = before_mask & (np.abs(cninfo_df['total_share'] - post_share) / post_share > 0.01)
        if not before_diff.any():
            pre_share = post_share / (1 + split_ratio)
        else:
            pre_share = cninfo_df.loc[before_diff, 'total_share'].iloc[-1]
        mask_early_post = (
            (cninfo_df['change_date'] < ex_date) &
            (cninfo_df['change_date'] >= ex_date - pd.Timedelta(days=120)) &
            (np.abs(cninfo_df['total_share'] - post_share) / post_share < 0.01)
        )
        if mask_early_post.any():
            cninfo_df.loc[mask_early_post, 'change_date'] = ex_date
        first_after_date = after['change_date'].iloc[0]
        if first_after_date > ex_date:
            ratio = post_share / pre_share if pre_share > 0 else 0
            expected = 1 + split_ratio
            if abs(ratio - expected) / expected < 0.15:
                idx_list = cninfo_df.index[cninfo_df['change_date'] == first_after_date].tolist()
                if idx_list:
                    cninfo_df.loc[idx_list[0], 'change_date'] = ex_date
    cninfo_df = cninfo_df.sort_values('change_date').reset_index(drop=True)
    cninfo_df = cninfo_df.drop_duplicates(subset='change_date', keep='last').reset_index(drop=True)
    return cninfo_df


def _merge_timeline(cninfo_df, bao_df):
    cninfo_df = cninfo_df[['change_date', 'total_share']].rename(columns={'change_date': 'date'})
    cninfo_df['date'] = pd.to_datetime(cninfo_df['date'])
    cninfo_df = cninfo_df.dropna().sort_values('date').reset_index(drop=True)
    if bao_df.empty and cninfo_df.empty:
        return pd.DataFrame(columns=['date', 'total_share'])
    if bao_df.empty:
        return cninfo_df
    bao = bao_df[['pubDate', 'totalShare']].rename(columns={'pubDate': 'date', 'totalShare': 'total_share'})
    if cninfo_df.empty:
        return bao.sort_values('date').reset_index(drop=True)
    cninfo_last = cninfo_df['date'].max()
    bao_after = bao[bao['date'] > cninfo_last].copy()
    merged = pd.concat([cninfo_df, bao_after], ignore_index=True)
    merged = merged.sort_values('date').drop_duplicates(subset='date', keep='last').reset_index(drop=True)
    return merged


def _compute_cap(daily, shares_tl):
    if daily.empty:
        return daily.assign(total_share=None, total_mv=None)
    d = daily.sort_values('date').reset_index(drop=True)
    if shares_tl.empty:
        return d.assign(total_share=None, total_mv=None)
    s = shares_tl.sort_values('date').reset_index(drop=True)
    first_share = s.iloc[0]['total_share']
    backfill = pd.DataFrame([{'date': pd.Timestamp('2000-01-01'), 'total_share': first_share}])
    s2 = pd.concat([backfill, s], ignore_index=True)
    merged = pd.merge_asof(d, s2, on='date', direction='backward')
    # total_mv 单位: 元 (与生产 parquet 一致)
    merged['total_mv'] = merged['close'] * merged['total_share']
    return merged


def backfill_valuation(resume=False, limit=0):
    import baostock as bs
    LOG('=' * 60)
    LOG(f'Valuation backfill  {START_DATE} ~ {END_DATE}  (单进程, V4 逻辑)')
    LOG('=' * 60)

    if not os.path.exists(CNINFO_PATH):
        LOG(f'❌ cninfo 不存在: {CNINFO_PATH}')
        return

    stock_list = pd.read_parquet(STOCK_LIST_PATH)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    cninfo_all = pd.read_parquet(CNINFO_PATH)
    LOG(f'  cninfo: {len(cninfo_all)} 行, {cninfo_all["stock_code"].nunique()} 只')
    cninfo_dict = {c: sub for c, sub in cninfo_all.groupby('stock_code')}

    div_path = os.path.join(PROD_DIR, 'dividend_history.parquet')
    backfill_div_path = os.path.join(BACKFILL_DIR, 'dividend_history_backfill.parquet')
    div_frames = []
    if os.path.exists(div_path):
        div_frames.append(pd.read_parquet(div_path))
    if os.path.exists(backfill_div_path):
        div_frames.append(pd.read_parquet(backfill_div_path))
    if div_frames:
        div_all = pd.concat(div_frames, ignore_index=True)
        div_all['ex_dividend_date'] = pd.to_datetime(div_all['ex_dividend_date'], errors='coerce')
        div_all = div_all.dropna(subset=['ex_dividend_date'])
        div_all['bonus_shares'] = pd.to_numeric(div_all.get('bonus_shares'), errors='coerce').fillna(0)
        div_all['transfer_shares'] = pd.to_numeric(div_all.get('transfer_shares'), errors='coerce').fillna(0)
        div_dict = {c: sub for c, sub in div_all.groupby('stock_code')}
        LOG(f'  div: {len(div_all)} 行 (现生产+backfill), {len(div_dict)} 只')
    else:
        div_dict = {}
        LOG('  div: (empty)')

    temp_dir = os.path.join(BACKFILL_DIR, 'temp_valuation')
    output_path = os.path.join(BACKFILL_DIR, 'valuation_daily_backfill.parquet')
    os.makedirs(temp_dir, exist_ok=True)

    progress = load_progress()
    errors = load_errors()
    completed = set(progress.get('valuation', [])) if resume else set()
    if completed:
        LOG(f'  断点续传: {len(completed)}/{total}')

    lg = bs.login()
    if lg.error_code != '0':
        LOG(f'❌ baostock 登录失败: {lg.error_msg}')
        return

    ok_n = len(completed)
    fail_n = 0
    pre_ipo_n = 0

    try:
        for i, code in enumerate(codes):
            if code in completed:
                continue
            heartbeat('valuation', i=i+1, total=total, code=code, ok=ok_n, fail=fail_n, pre_ipo=pre_ipo_n)
            bs_code = code_to_bs_symbol(code)

            for attempt in range(MAX_RETRIES):
                try:
                    daily, err = _fetch_daily_bs(bs, bs_code)
                    if err:
                        if attempt < MAX_RETRIES - 1:
                            try:
                                bs.logout(); bs.login()
                            except Exception:
                                pass
                            time.sleep(1)
                            continue
                        record_err(errors, 'valuation', code, err)
                        fail_n += 1
                        break
                    if daily is None or len(daily) == 0:
                        pre_ipo_n += 1
                        ok_n += 1
                        mark_done(progress, 'valuation', code)
                        break

                    cninfo_sh = cninfo_dict.get(code, pd.DataFrame())
                    if len(cninfo_sh) > 0:
                        cninfo_last_year = int(pd.to_datetime(cninfo_sh['change_date']).max().year)
                        bao_start = max(2011, cninfo_last_year)
                    else:
                        bao_start = 2011
                    bao_sh = _fetch_shares_bs(bs, bs_code, start_year=bao_start)
                    div_ev = div_dict.get(code, pd.DataFrame())
                    cninfo_fixed = _fix_cninfo_div(cninfo_sh, div_ev) if len(cninfo_sh) > 0 else cninfo_sh
                    timeline = _merge_timeline(cninfo_fixed, bao_sh)
                    merged = _compute_cap(daily, timeline)
                    merged['stock_code'] = code
                    merged = merged.rename(columns={
                        'peTTM': 'pe_ttm', 'pbMRQ': 'pb', 'psTTM': 'ps_ttm', 'pcfNcfTTM': 'pcf_ttm',
                    })
                    out = merged[['stock_code', 'date', 'pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm',
                                  'total_share', 'total_mv']]
                    out.to_parquet(os.path.join(temp_dir, f'{code}.parquet'), index=False)
                    ok_n += 1
                    mark_done(progress, 'valuation', code)
                    break
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(1)
                    else:
                        record_err(errors, 'valuation', code, e)
                        fail_n += 1

            if (i + 1) % 50 == 0:
                LOG(f'  [{i+1}/{total}] ok={ok_n} fail={fail_n} pre_ipo={pre_ipo_n}')
    finally:
        try:
            bs.logout()
        except Exception:
            pass

    LOG(f'Valuation 下载完成: ok={ok_n} fail={fail_n} pre_ipo={pre_ipo_n}')
    heartbeat('valuation_merging', total=total, ok=ok_n, fail=fail_n)
    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'date'])
    heartbeat('valuation_done', total=total, ok=ok_n, fail=fail_n)


# ============ Financial Backfill ============
FINANCIAL_ROW_MAPPING = {
    '净资产收益率(ROE)': 'roe',
    '营业总收入': 'revenue',
    '归母净利润': 'net_profit',
    '归属母公司净利润': 'net_profit',
    '营业总收入增长率': 'revenue_growth_yoy',
    '营业总收入同比增长率': 'revenue_growth_yoy',
    '归属母公司净利润增长率': 'profit_growth_yoy',
    '归属母公司净利润同比增长率': 'profit_growth_yoy',
}


def calc_publish_deadline(report_date_str):
    month = int(report_date_str[4:6])
    year = int(report_date_str[:4])
    if month == 3:
        return f'{year}0501'
    elif month == 6:
        return f'{year}0901'
    elif month == 9:
        return f'{year}1101'
    elif month == 12:
        return f'{year+1}0501'
    return f'{year}0501'


def backfill_financial(resume=False, limit=0):
    import akshare as ak
    LOG('=' * 60)
    LOG(f'Financial backfill  ~{START_DATE} 前的全部季度')
    LOG('=' * 60)

    stock_list = pd.read_parquet(STOCK_LIST_PATH)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    temp_dir = os.path.join(BACKFILL_DIR, 'temp_financial')
    output_path = os.path.join(BACKFILL_DIR, 'financial_quarterly_backfill.parquet')
    os.makedirs(temp_dir, exist_ok=True)

    progress = load_progress()
    errors = load_errors()
    completed = set(progress.get('financial', [])) if resume else set()
    if completed:
        LOG(f'  断点续传: {len(completed)}/{total}')

    # 现有 financial_quarterly 的各 stock_code 已有最早 report_date, 避免重复
    existing_min = {}
    prod_fin = os.path.join(PROD_DIR, 'financial_quarterly.parquet')
    if os.path.exists(prod_fin):
        df_fin = pd.read_parquet(prod_fin, columns=['stock_code', 'report_date'])
        existing_min = df_fin.groupby('stock_code')['report_date'].min().to_dict()
        del df_fin

    start_year = int(START_DATE[:4])
    ok_n = len(completed)
    fail_n = 0
    none_n = 0

    for i, code in enumerate(codes):
        if code in completed:
            continue
        heartbeat('financial', i=i+1, total=total, code=code, ok=ok_n, fail=fail_n)

        ok = False
        for attempt in range(MAX_RETRIES):
            try:
                df_raw = ak.stock_financial_abstract(symbol=code)
                if df_raw is None or df_raw.empty:
                    none_n += 1
                    ok = True
                    break

                indicator_col = df_raw.columns[1]
                quarter_cols = [c for c in df_raw.columns[2:]
                                if str(c).isdigit() and len(str(c)) == 8]
                # 只要 start_year <= 年份 < 2021 (backfill 段)
                quarter_cols = [c for c in quarter_cols
                                if start_year <= int(str(c)[:4]) < 2021]
                # 去掉已存在的 report_date (防重)
                if code in existing_min:
                    min_existing = existing_min[code].strftime('%Y%m%d')
                    quarter_cols = [c for c in quarter_cols if str(c) < min_existing]
                if not quarter_cols:
                    ok = True
                    break

                row_indices = {}
                for idx, row in df_raw.iterrows():
                    row_name = str(row[indicator_col]).strip()
                    if row_name in FINANCIAL_ROW_MAPPING:
                        target = FINANCIAL_ROW_MAPPING[row_name]
                        row_indices.setdefault(target, idx)

                records = []
                for qcol in quarter_cols:
                    rec = {
                        'stock_code': code,
                        'report_date': pd.to_datetime(str(qcol)),
                        'publish_deadline': pd.to_datetime(calc_publish_deadline(str(qcol))),
                    }
                    for target, ridx in row_indices.items():
                        val = df_raw.at[ridx, qcol]
                        try:
                            rec[target] = float(val)
                        except (ValueError, TypeError):
                            rec[target] = None
                    records.append(rec)

                if records:
                    df_result = pd.DataFrame(records)
                    for col in ['roe', 'revenue', 'net_profit',
                                'revenue_growth_yoy', 'profit_growth_yoy']:
                        if col not in df_result.columns:
                            df_result[col] = None
                    df_result = df_result[['stock_code', 'report_date', 'publish_deadline',
                                           'roe', 'revenue', 'net_profit',
                                           'revenue_growth_yoy', 'profit_growth_yoy']]
                    df_result.to_parquet(os.path.join(temp_dir, f'{code}.parquet'), index=False)
                ok = True
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                else:
                    record_err(errors, 'financial', code, e)

        if ok:
            ok_n += 1
            mark_done(progress, 'financial', code)
        else:
            fail_n += 1

        if (i + 1) % 50 == 0:
            LOG(f'  [{i+1}/{total}] ok={ok_n} fail={fail_n} none={none_n}')

        time.sleep(REQUEST_INTERVAL)

    LOG(f'Financial 下载完成: ok={ok_n} fail={fail_n} none={none_n}')
    heartbeat('financial_merging', total=total, ok=ok_n, fail=fail_n)
    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'report_date'])
    heartbeat('financial_done', total=total, ok=ok_n, fail=fail_n)


# ============ Dividend Backfill ============
DIVIDEND_COLUMN_MAPPING = {
    'announce_date': ['公告日期', '报告日期', '公告时间'],
    'dividend_per_share': ['派息', '每股派息', '派息(税前)', '每股分红'],
    'bonus_shares': ['送股', '送股(股)', '每10股送股'],
    'transfer_shares': ['转增', '转增(股)', '每10股转增'],
    'ex_dividend_date': ['除权除息日', '除息日', '除权日'],
    'progress': ['进度', '方案进度', '实施进度'],
}


def backfill_dividend(resume=False, limit=0):
    import akshare as ak
    LOG('=' * 60)
    LOG(f'Dividend backfill  ~{START_DATE} 前的全部分红')
    LOG('=' * 60)

    stock_list = pd.read_parquet(STOCK_LIST_PATH)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    temp_dir = os.path.join(BACKFILL_DIR, 'temp_dividend')
    output_path = os.path.join(BACKFILL_DIR, 'dividend_history_backfill.parquet')
    os.makedirs(temp_dir, exist_ok=True)

    progress = load_progress()
    errors = load_errors()
    completed = set(progress.get('dividend', [])) if resume else set()

    start_ts = pd.to_datetime(START_DATE)
    end_ts = pd.to_datetime(END_DATE)

    ok_n = len(completed)
    fail_n = 0
    none_n = 0

    required_cols = ['announce_date', 'dividend_per_share', 'bonus_shares',
                     'transfer_shares', 'ex_dividend_date', 'progress']

    for i, code in enumerate(codes):
        if code in completed:
            continue
        heartbeat('dividend', i=i+1, total=total, code=code, ok=ok_n, fail=fail_n)

        ok = False
        for attempt in range(MAX_RETRIES):
            try:
                df = ak.stock_history_dividend_detail(symbol=code, indicator='分红')
                if df is None or df.empty:
                    none_n += 1
                    ok = True
                    break
                df = flexible_rename(df, DIVIDEND_COLUMN_MAPPING)
                df['stock_code'] = code
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = None
                for dc in ['announce_date', 'ex_dividend_date']:
                    df[dc] = pd.to_datetime(df[dc], errors='coerce')
                for nc in ['dividend_per_share', 'bonus_shares', 'transfer_shares']:
                    df[nc] = pd.to_numeric(df[nc], errors='coerce')
                df['dividend_per_share'] = df['dividend_per_share'] / 10.0
                # 只保留 backfill 段 (start_ts <= announce < end_ts + 1day) — 不重叠生产
                if df['announce_date'].notna().any():
                    df = df[(df['announce_date'] >= start_ts) &
                            (df['announce_date'] <= end_ts)].copy()
                if not df.empty:
                    df = df[['stock_code'] + required_cols]
                    df.to_parquet(os.path.join(temp_dir, f'{code}.parquet'), index=False)
                else:
                    none_n += 1
                ok = True
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                else:
                    record_err(errors, 'dividend', code, e)

        if ok:
            ok_n += 1
            mark_done(progress, 'dividend', code)
        else:
            fail_n += 1

        if (i + 1) % 50 == 0:
            LOG(f'  [{i+1}/{total}] ok={ok_n} fail={fail_n} none={none_n}')

        time.sleep(REQUEST_INTERVAL)

    LOG(f'Dividend 下载完成: ok={ok_n} fail={fail_n} none={none_n}')
    heartbeat('dividend_merging', total=total, ok=ok_n, fail=fail_n)
    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'announce_date'])
    heartbeat('dividend_done', total=total, ok=ok_n, fail=fail_n)


# ============ Index (沪深300) Backfill ============
def backfill_index(resume=False, limit=0):
    import baostock as bs
    LOG('=' * 60)
    LOG(f'Index backfill  沪深300  {START_DATE} ~ {END_DATE}')
    LOG('=' * 60)

    output_path = os.path.join(BACKFILL_DIR, 'index_daily_backfill.parquet')

    lg = bs.login()
    if lg.error_code != '0':
        LOG(f'❌ baostock 登录失败: {lg.error_msg}')
        return

    try:
        rs = bs.query_history_k_data_plus(
            'sh.000300',
            'date,open,high,low,close,volume',
            start_date=START_DATE, end_date=END_DATE,
            frequency='d'
        )
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            LOG('❌ 无数据')
            return
        df = pd.DataFrame(rows, columns=rs.fields)
        df['date'] = pd.to_datetime(df['date'])
        for c in ['open', 'high', 'low', 'close', 'volume']:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
        df = df.sort_values('date').reset_index(drop=True)
        df.to_parquet(output_path, index=False)
        size_kb = os.path.getsize(output_path) / 1024
        LOG(f'✅ {len(df)} 行, {size_kb:.1f} KB → {output_path}')
        heartbeat('index_done', rows=len(df))
    finally:
        try:
            bs.logout()
        except Exception:
            pass


# ============ Main ============
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--table', required=True,
                    choices=['ohlcv', 'valuation', 'financial', 'dividend', 'index'])
    ap.add_argument('--resume', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()

    LOG(f'=== backfill_15y 启动 table={args.table} resume={args.resume} limit={args.limit} ===')
    LOG(f'  PID: {os.getpid()}')
    LOG(f'  BACKFILL_DIR: {BACKFILL_DIR}')
    t0 = time.time()

    dispatch = {
        'ohlcv': backfill_ohlcv,
        'valuation': backfill_valuation,
        'financial': backfill_financial,
        'dividend': backfill_dividend,
        'index': backfill_index,
    }
    dispatch[args.table](resume=args.resume, limit=args.limit)

    LOG(f'=== 总耗时 {(time.time()-t0)/60:.1f} min ===')


if __name__ == '__main__':
    main()
