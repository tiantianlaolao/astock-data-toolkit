"""
V4 生产版 — 全市场 valuation_daily 重建 (5321 只)

数据来源:
  - daily prices: baostock query_history_k_data_plus (adjustflag='3' 未复权)
  - share timeline:
      1) cninfo_share_change_full.parquet (全市场新鲜版, 110K 行)
      2) dividend_history.parquet (除权日校正, 修 cninfo 标日偏差)
      3) baostock query_profit_data (季报 pubDate 兜底, cninfo 截止日之后)
  - total_mv = close × total_share / 1e8 (亿元)

输出:
  - valuation_daily_baostock_v4.parquet (字段同 v3)
  - v4_failures.csv (失败列表)
  - v4_progress.log (实时进度)

预估: 4 进程 × ~3.3 小时
"""
import baostock as bs
import pandas as pd
import polars as pl
import multiprocessing as mp
import time
import os
import sys
import io
import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
def LOG(m): print(m, flush=True)

SCRIPT_DIR = os.environ.get('ASTOCK_HOME') or os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'astock_data')
CNINFO_PARQUET = os.path.join(SCRIPT_DIR, 'cninfo_share_change_full.parquet')
DIV_PARQUET = os.path.join(OUTPUT_DIR, 'dividend_history.parquet')
STOCK_LIST_PARQUET = os.path.join(OUTPUT_DIR, 'stock_list.parquet')
OUT_PATH = os.path.join(OUTPUT_DIR, 'valuation_daily_baostock_v4.parquet')
FAIL_PATH = os.path.join(SCRIPT_DIR, 'v4_failures.csv')
START_DATE = '2021-04-12'
END_DATE   = '2026-04-11'
N_PROC = 4
MAX_RETRY = 3
CHUNK_SIZE = 30  # 每批 30 只, 流式收集

def to_bs_code(code):
    if code.startswith(('6','5','9')): return f'sh.{code}'
    return f'sz.{code}'

def retry_call(fn, *args, **kwargs):
    last_err = None
    for attempt in range(MAX_RETRY):
        try:
            result = fn(*args, **kwargs)
            if hasattr(result, 'error_code') and result.error_code != '0':
                last_err = f'{result.error_code}: {result.error_msg}'
                time.sleep(1 + attempt)
                bs.logout(); bs.login()
                continue
            return result, None
        except Exception as e:
            last_err = str(e)
            time.sleep(1 + attempt)
            try: bs.logout()
            except Exception: pass
            try: bs.login()
            except Exception: pass
    return None, last_err

def fetch_daily(bs_code):
    fields = 'date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM'
    rs, err = retry_call(
        bs.query_history_k_data_plus,
        bs_code, fields,
        start_date=START_DATE, end_date=END_DATE,
        frequency='d', adjustflag='3'
    )
    if err: return None, err
    rows = []
    try:
        while rs.next(): rows.append(rs.get_row_data())
    except Exception as e:
        return None, f'read: {e}'
    if not rows: return pd.DataFrame(), None
    df = pd.DataFrame(rows, columns=rs.fields)
    df['date'] = pd.to_datetime(df['date'])
    for c in ['close','peTTM','pbMRQ','psTTM','pcfNcfTTM']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df, None

def fetch_shares_baostock(bs_code, start_year=2021):
    rows = []
    for year in range(start_year, 2027):
        for q in range(1, 5):
            rs, err = retry_call(bs.query_profit_data, code=bs_code, year=year, quarter=q)
            if err or rs is None: continue
            try:
                while rs.next():
                    rows.append(dict(zip(rs.fields, rs.get_row_data())))
            except Exception:
                continue
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['pubDate'] = pd.to_datetime(df['pubDate'], errors='coerce')
    df['totalShare'] = pd.to_numeric(df['totalShare'], errors='coerce')
    df = df.dropna(subset=['pubDate','totalShare']).sort_values('pubDate').reset_index(drop=True)
    return df

def fix_cninfo_with_divhistory(cninfo_df, div_events):
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
        # Fix 1: 预披露 - change_date < ex_date 但值 ≈ post_share → 改到 ex_date
        mask_early_post = (
            (cninfo_df['change_date'] < ex_date) &
            (cninfo_df['change_date'] >= ex_date - pd.Timedelta(days=120)) &
            (np.abs(cninfo_df['total_share'] - post_share) / post_share < 0.01)
        )
        if mask_early_post.any():
            cninfo_df.loc[mask_early_post, 'change_date'] = ex_date
        # Fix 2: 延后 - first_after_date > ex_date 且 跳变比例匹配 → 改到 ex_date
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

def merge_share_timeline(cninfo_df, bao_df):
    cninfo_df = cninfo_df[['change_date','total_share']].rename(columns={'change_date':'date'})
    cninfo_df['date'] = pd.to_datetime(cninfo_df['date'])
    cninfo_df = cninfo_df.dropna().sort_values('date').reset_index(drop=True)
    if bao_df.empty and cninfo_df.empty:
        return pd.DataFrame(columns=['date','total_share'])
    if bao_df.empty:
        return cninfo_df
    bao_df2 = bao_df[['pubDate','totalShare']].rename(columns={'pubDate':'date','totalShare':'total_share'})
    if cninfo_df.empty:
        return bao_df2.sort_values('date').reset_index(drop=True)
    cninfo_last = cninfo_df['date'].max()
    bao_after = bao_df2[bao_df2['date'] > cninfo_last].copy()
    merged = pd.concat([cninfo_df, bao_after], ignore_index=True).sort_values('date').reset_index(drop=True)
    merged = merged.drop_duplicates(subset='date', keep='last').reset_index(drop=True)
    return merged

def compute_cap(daily, shares_tl):
    if daily.empty:
        daily = daily.copy()
        daily['total_share'] = None
        daily['total_mv'] = None
        return daily
    d = daily.sort_values('date').reset_index(drop=True)
    if shares_tl.empty:
        d['total_share'] = None
        d['total_mv'] = None
        return d
    s = shares_tl.sort_values('date').reset_index(drop=True)
    first_share = s.iloc[0]['total_share']
    backfill = pd.DataFrame([{'date': pd.Timestamp('2000-01-01'), 'total_share': first_share}])
    s2 = pd.concat([backfill, s], ignore_index=True)
    merged = pd.merge_asof(d, s2, on='date', direction='backward')
    merged['total_mv'] = merged['close'] * merged['total_share'] / 1e8
    return merged

def worker(args):
    chunk, cninfo_dict, div_dict = args
    lg = bs.login()
    if lg.error_code != '0':
        return [(c, None, 'login fail') for c in chunk]
    results = []
    for code in chunk:
        t0 = time.time()
        bs_code = to_bs_code(code)
        try:
            daily, err1 = fetch_daily(bs_code)
            if err1 or daily is None or len(daily) == 0:
                results.append((code, None, f'daily err: {err1}'))
                continue
            cninfo_sh = cninfo_dict.get(code, pd.DataFrame())
            if len(cninfo_sh) > 0:
                cninfo_last_year = int(pd.to_datetime(cninfo_sh['change_date']).max().year)
                bao_start_year = max(2021, cninfo_last_year)
            else:
                bao_start_year = 2021
            bao_sh = fetch_shares_baostock(bs_code, start_year=bao_start_year)
            div_ev = div_dict.get(code, pd.DataFrame())
            cninfo_sh_fixed = fix_cninfo_with_divhistory(cninfo_sh, div_ev) if len(cninfo_sh) > 0 else cninfo_sh
            timeline = merge_share_timeline(cninfo_sh_fixed, bao_sh)
            merged = compute_cap(daily, timeline)
            merged['stock_code'] = code
            merged = merged.rename(columns={
                'peTTM':'pe_ttm','pbMRQ':'pb','psTTM':'ps_ttm','pcfNcfTTM':'pcf_ttm',
            })
            out = merged[['stock_code','date','pe_ttm','pb','ps_ttm','pcf_ttm','total_share','total_mv']]
            elapsed = time.time() - t0
            results.append((code, out, f'ok t={elapsed:.1f}s'))
        except Exception as e:
            results.append((code, None, f'except: {e}'))
    try: bs.logout()
    except Exception: pass
    return results

def main():
    LOG(f'=== V4 全市场 valuation 重建 ===')
    LOG(f'  cninfo: {CNINFO_PARQUET}')
    LOG(f'  div:    {DIV_PARQUET}')
    LOG(f'  list:   {STOCK_LIST_PARQUET}')
    LOG(f'  output: {OUT_PATH}')

    # 1. 加载股票清单
    stock_list = pl.read_parquet(STOCK_LIST_PARQUET)
    codes = stock_list['stock_code'].to_list()
    LOG(f'\n股票数: {len(codes)}')

    # 2. 加载 cninfo
    cninfo_all = pd.read_parquet(CNINFO_PARQUET)
    LOG(f'cninfo: {len(cninfo_all)} 行, {cninfo_all["stock_code"].nunique()} 只')
    cninfo_dict = {code: sub for code, sub in cninfo_all.groupby('stock_code')}

    # 3. 加载 div
    div_all = pd.read_parquet(DIV_PARQUET)
    div_all['ex_dividend_date'] = pd.to_datetime(div_all['ex_dividend_date'], errors='coerce')
    div_all = div_all.dropna(subset=['ex_dividend_date'])
    div_all['bonus_shares'] = pd.to_numeric(div_all['bonus_shares'], errors='coerce').fillna(0)
    div_all['transfer_shares'] = pd.to_numeric(div_all['transfer_shares'], errors='coerce').fillna(0)
    LOG(f'div: {len(div_all)} 行, {div_all["stock_code"].nunique()} 只')
    div_dict = {code: sub for code, sub in div_all.groupby('stock_code')}

    # 4. 切 chunks
    chunks = [codes[i:i+CHUNK_SIZE] for i in range(0, len(codes), CHUNK_SIZE)]
    LOG(f'\n切 {len(chunks)} 个 chunk, 每个 {CHUNK_SIZE} 只, {N_PROC} 进程并发')
    args_list = [(ck, cninfo_dict, div_dict) for ck in chunks]

    # 5. 并发执行 + 流式收集
    t0 = time.time()
    all_dfs = []
    failures = []
    done_count = 0
    with mp.Pool(N_PROC) as pool:
        for ci, ck_results in enumerate(pool.imap_unordered(worker, args_list)):
            for code, df, msg in ck_results:
                done_count += 1
                if df is not None:
                    all_dfs.append(df)
                else:
                    failures.append((code, msg))
            elapsed = time.time() - t0
            rate = done_count / elapsed if elapsed > 0 else 0
            eta = (len(codes) - done_count) / rate if rate > 0 else 0
            LOG(f'  [{done_count:5}/{len(codes)}]  ok={len(all_dfs):5}  fail={len(failures):3}  用时 {elapsed:.0f}s  速度 {rate:.2f}/s  ETA {eta/60:.1f}min')

    elapsed = time.time() - t0
    LOG(f'\n=== 完成 总耗时 {elapsed:.0f}s ({elapsed/60:.1f} min) ===')
    LOG(f'  ok={len(all_dfs)}  fail={len(failures)}')

    # 6. 合并保存
    if all_dfs:
        out_df = pd.concat(all_dfs, ignore_index=True)
        LOG(f'\n合并行数: {len(out_df):,}')
        pl_df = pl.from_pandas(out_df)
        pl_df.write_parquet(OUT_PATH)
        LOG(f'✓ 已保存: {OUT_PATH}')
    else:
        LOG('⚠ 无数据')

    # 7. 失败清单
    if failures:
        pd.DataFrame(failures, columns=['code','error']).to_csv(FAIL_PATH, index=False)
        LOG(f'\n失败 {len(failures)} 只 → {FAIL_PATH}')
        LOG(f'前 20 失败:')
        for c, e in failures[:20]:
            LOG(f'  {c}: {e}')

    # 8. Sanity 抽检
    if all_dfs:
        LOG(f'\n=== Sanity 抽检 ===')
        for code in ['000001', '600519', '002594', '300750']:
            sub = pl_df.filter(pl.col('stock_code') == code)
            if len(sub) > 0:
                tm = sub['total_mv'].drop_nulls()
                LOG(f'  {code}: {len(sub)} 行, total_mv [{tm.min():.0f}, {tm.max():.0f}] 亿')

if __name__ == '__main__':
    main()
