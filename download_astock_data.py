"""
A股量化回测数据下载脚本 v2
===========================
下载全A股5年历史数据，存储为 Parquet 格式，供回测工具使用。

6张数据表覆盖14个因子：
  1. stock_list.parquet          - 股票列表
  2. index_daily.parquet         - 沪深300指数日线 (Beta因子)
  3. daily_ohlcv.parquet         - 日线行情 (动量/反转/波动率/换手率/量价背离/流动性)
  4. valuation_daily.parquet     - PE/PB/市值 (估值因子)
  5. financial_quarterly.parquet - ROE/营收增长/利润增长
  6. dividend_history.parquet    - 分红记录 (股息率因子)

用法:
  python download_astock_data.py                        # 全量下载全部
  python download_astock_data.py --step 2               # 只执行 Step 2 (日线行情)
  python download_astock_data.py --step 2 --resume      # 断点续传
  python download_astock_data.py --incremental           # 增量更新全部
  python download_astock_data.py --incremental --step 2  # 增量更新日线
  python download_astock_data.py --step 99              # 数据验证

v2 改进:
  - 合并阶段分批写入，不再一次性 concat 进内存
  - 增量更新模式 (--incremental)：只下载已有数据之后的新数据
  - Step 3 增量模式仍用百度逐只 (不能用东方接口, 129 IP 被拉黑), 跳过已是最新的
  - Step 5 分红数据列名容错处理
  - Step 3 百度接口加长请求间隔
"""

import os
import sys
import json
import time
import argparse
import warnings
from datetime import datetime, timedelta

# ============ 控制台编码修复 (Windows GBK) ============
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ============ 强制清除代理 ============
for var in ['http_proxy', 'https_proxy', 'all_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']:
    os.environ[var] = ''
    if var in os.environ:
        del os.environ[var]

# 清除 Windows 系统级代理 (注册表级别)
if sys.platform == 'win32':
    try:
        import winreg
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                             r'Software\Microsoft\Windows\CurrentVersion\Internet Settings',
                             0, winreg.KEY_SET_VALUE)
        winreg.SetValueEx(key, 'ProxyEnable', 0, winreg.REG_DWORD, 0)
        winreg.CloseKey(key)
    except Exception:
        pass

import requests
from requests.sessions import Session
_original_request = Session.request
def _no_proxy_request(self, *args, **kwargs):
    kwargs['proxies'] = {'http': None, 'https': None}
    return _original_request(self, *args, **kwargs)
Session.request = _no_proxy_request

# 额外: 让 urllib3 也不走代理
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

import akshare as ak
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

warnings.filterwarnings('ignore')

# ============ 配置 ============
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'astock_data')
PROGRESS_FILE = os.path.join(OUTPUT_DIR, 'progress.json')
ERRORS_FILE = os.path.join(OUTPUT_DIR, 'errors.json')
# V4 增量用: cninfo 全市场股本变动 (脚本目录下, 不在 astock_data/ 子目录)
CNINFO_PATH = os.path.join(SCRIPT_DIR, 'cninfo_share_change_full.parquet')

# 数据范围: 5年
END_DATE = datetime.now().strftime('%Y%m%d')
START_DATE = (datetime.now() - timedelta(days=5 * 365)).strftime('%Y%m%d')

# 限频
REQUEST_INTERVAL = 0.4        # 一般接口间隔(秒)
REQUEST_INTERVAL_BAIDU = 0.8  # 百度估值接口间隔(更保守)
MAX_RETRIES = 3               # 单只股票失败最大重试次数
MERGE_BATCH_SIZE = 200        # 合并时每批处理的文件数


# ============ 工具函数 ============

def code_to_tx_symbol(code):
    """6位代码转腾讯格式: 000001→sz000001, 600519→sh600519"""
    if code.startswith('6'):
        return f'sh{code}'
    else:  # 0xx, 3xx, 8xx, 4xx
        return f'sz{code}'


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


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


def get_completed_codes(progress, step_name):
    return set(progress.get(step_name, []))


def mark_completed(progress, step_name, code):
    if step_name not in progress:
        progress[step_name] = []
    progress[step_name].append(code)
    save_progress(progress)


def record_error(errors, step_name, code, error_msg):
    if step_name not in errors:
        errors[step_name] = {}
    errors[step_name][code] = error_msg
    save_errors(errors)


def merge_temp_files(temp_dir, output_path, sort_columns):
    """分批合并临时 parquet 文件，避免内存溢出"""
    temp_files = sorted([
        os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.parquet')
    ])
    if not temp_files:
        print(f'  ❌ 无数据文件可合并')
        return

    total_files = len(temp_files)
    print(f'  合并 {total_files} 个临时文件 (每批 {MERGE_BATCH_SIZE} 个)...')

    # 如果已存在旧的输出文件，先读入（支持增量追加）
    existing_tables = []
    if os.path.exists(output_path):
        existing_tables.append(pq.read_table(output_path))
        print(f'    已有数据加载完成')

    # 分批读取临时文件, promote_options='permissive' 自动统一类型(如 int64→double)
    all_tables = existing_tables
    for batch_start in range(0, total_files, MERGE_BATCH_SIZE):
        batch_end = min(batch_start + MERGE_BATCH_SIZE, total_files)
        batch_files = temp_files[batch_start:batch_end]

        batch_tables = []
        for tf in batch_files:
            try:
                batch_tables.append(pq.read_table(tf))
            except Exception as e:
                print(f'    ⚠ 跳过损坏文件: {os.path.basename(tf)} ({e})')

        if batch_tables:
            batch_combined = pa.concat_tables(batch_tables, promote_options='permissive')
            all_tables.append(batch_combined)
            print(f'    批次 {batch_start+1}-{batch_end}/{total_files} 完成')

    # 最终合并
    if all_tables:
        final_table = pa.concat_tables(all_tables, promote_options='permissive')
        # 转 pandas 排序后写回（排序需要较少内存因为只操作索引）
        df = final_table.to_pandas()
        df = df.sort_values(sort_columns).reset_index(drop=True)
        # 去重（增量追加时可能有重叠）
        df = df.drop_duplicates(subset=sort_columns, keep='last').reset_index(drop=True)
        df.to_parquet(output_path, index=False, engine='pyarrow')
        size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f'  ✅ {len(df)} 行, {size_mb:.1f}MB → {output_path}')
    else:
        print(f'  ❌ 无数据可合并')


def get_existing_max_date(output_path, stock_code=None, date_col='date'):
    """获取已有数据的最新日期，用于增量更新"""
    if not os.path.exists(output_path):
        return None
    try:
        df = pd.read_parquet(output_path, columns=['stock_code', date_col] if stock_code else [date_col])
        if stock_code:
            df = df[df['stock_code'] == stock_code]
        if df.empty:
            return None
        return df[date_col].max()
    except Exception:
        return None


def flexible_rename(df, column_mapping):
    """灵活列名映射: 尝试多个候选名，找到第一个匹配的就用"""
    rename_dict = {}
    for target_name, candidates in column_mapping.items():
        if isinstance(candidates, str):
            candidates = [candidates]
        for candidate in candidates:
            if candidate in df.columns:
                rename_dict[candidate] = target_name
                break
    return df.rename(columns=rename_dict)


# ============================================================
# Step 0: 获取全A股代码列表
# ============================================================
def step0_stock_list():
    print('=' * 60)
    print('Step 0: 获取全A股股票列表')
    print('=' * 60)

    output_path = os.path.join(OUTPUT_DIR, 'stock_list.parquet')
    all_stocks = []

    # 方案1: 用 spot_em 接口 (实时行情, 数据最全)
    print('  方案1: 拉取沪深A股实时列表...')
    try:
        df_sh = ak.stock_sh_a_spot_em()
        for _, row in df_sh.iterrows():
            all_stocks.append({
                'stock_code': str(row['代码']).zfill(6),
                'stock_name': row['名称'],
                'market': '沪A'
            })
        print(f'    沪A: {len(df_sh)} 只')
        time.sleep(REQUEST_INTERVAL)

        df_sz = ak.stock_sz_a_spot_em()
        for _, row in df_sz.iterrows():
            all_stocks.append({
                'stock_code': str(row['代码']).zfill(6),
                'stock_name': row['名称'],
                'market': '深A'
            })
        print(f'    深A: {len(df_sz)} 只')
    except Exception as e:
        print(f'    方案1失败: {e}')
        all_stocks = []

    # 方案2: 用 stock_info_a_code_name 接口 (备选, 走不同数据源)
    if not all_stocks:
        print('  方案2: 尝试 stock_info_a_code_name...')
        try:
            df_info = ak.stock_info_a_code_name()
            for _, row in df_info.iterrows():
                code = str(row['code']).zfill(6)
                market = '沪A' if code.startswith(('6', '5')) else '深A'
                all_stocks.append({
                    'stock_code': code,
                    'stock_name': row['name'],
                    'market': market
                })
            print(f'    获取: {len(all_stocks)} 只')
        except Exception as e:
            print(f'    方案2失败: {e}')

    # 方案3: 用 stock_zh_a_hist 逐个板块拉代码列表
    if not all_stocks:
        print('  方案3: 尝试从板块接口获取...')
        try:
            # 沪深京A股代码范围: 00/30/60/68/83/87开头
            df_all = ak.stock_board_industry_name_em()
            codes_set = set()
            for _, row in df_all.head(5).iterrows():  # 只取几个板块凑代码
                try:
                    df_cons = ak.stock_board_industry_cons_em(symbol=row['板块名称'])
                    for _, r in df_cons.iterrows():
                        code = str(r['代码']).zfill(6)
                        if code not in codes_set:
                            codes_set.add(code)
                            market = '沪A' if code.startswith(('6', '5')) else '深A'
                            all_stocks.append({
                                'stock_code': code,
                                'stock_name': r['名称'],
                                'market': market
                            })
                    time.sleep(REQUEST_INTERVAL)
                except Exception:
                    continue
            print(f'    获取: {len(all_stocks)} 只 (部分, 来自板块)')
        except Exception as e:
            print(f'    方案3失败: {e}')

    if not all_stocks:
        print('  ❌ 所有方案均失败，无法获取股票列表')
        print('  提示: 可能是代理/VPN拦截了国内网站，请检查网络设置')
        return False

    df = pd.DataFrame(all_stocks)
    before_filter = len(df)
    df = df[~df['stock_name'].str.contains(r'ST|退', case=False, na=False)].reset_index(drop=True)
    # 去重 (方案3可能有重复)
    df = df.drop_duplicates(subset='stock_code').reset_index(drop=True)
    print(f'  过滤ST/退市+去重: {before_filter} → {len(df)} 只')

    df.to_parquet(output_path, index=False, engine='pyarrow')
    print(f'  ✅ 保存 {len(df)} 只股票 → {output_path}')
    return True


# ============================================================
# Step 1: 下载沪深300指数日线
# ============================================================
def step1_index_daily(incremental=False):
    print('=' * 60)
    print('Step 1: 下载沪深300指数日线')
    print('=' * 60)

    output_path = os.path.join(OUTPUT_DIR, 'index_daily.parquet')

    try:
        df = ak.stock_zh_index_daily(symbol='sh000300')
        df['date'] = pd.to_datetime(df['date'])
        start = pd.to_datetime(START_DATE)
        df = df[df['date'] >= start].copy()
        # 保留 akshare 返回的全部字段 (date, open, high, low, close, volume)
        # 之前只保留 date+close 丢失了 OHLV, 现在补全 — 方便将来做 benchmark 波动率/量能分析
        keep_cols = [c for c in ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'] if c in df.columns]
        df = df[keep_cols].reset_index(drop=True)

        if incremental and os.path.exists(output_path):
            df_old = pd.read_parquet(output_path)
            df = pd.concat([df_old, df], ignore_index=True)
            df = df.drop_duplicates(subset=['date'], keep='last').sort_values('date').reset_index(drop=True)

        df.to_parquet(output_path, index=False, engine='pyarrow')
        print(f'  ✅ {len(df)} 个交易日 ({df["date"].min().date()} ~ {df["date"].max().date()})')
        return True
    except Exception as e:
        print(f'  ❌ 失败: {e}')
        return False


# ============================================================
# Step 2: 逐只下载日线 OHLCV
# ============================================================
def step2_daily_ohlcv(resume=False, incremental=False, limit=0):
    print('=' * 60)
    print(f'Step 2: {"增量更新" if incremental else "全量下载"}全A股日线行情 (OHLCV)')
    print('=' * 60)

    stock_list_path = os.path.join(OUTPUT_DIR, 'stock_list.parquet')
    output_path = os.path.join(OUTPUT_DIR, 'daily_ohlcv.parquet')
    temp_dir = os.path.join(OUTPUT_DIR, 'temp_ohlcv')
    os.makedirs(temp_dir, exist_ok=True)

    if not os.path.exists(stock_list_path):
        print('  ❌ stock_list.parquet 不存在，请先运行 Step 0')
        return False

    stock_list = pd.read_parquet(stock_list_path)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    # 增量模式: 读取已有数据的最新日期
    incremental_start_dates = {}
    if incremental and os.path.exists(output_path):
        print('  读取已有数据的最新日期...')
        df_existing = pd.read_parquet(output_path, columns=['stock_code', 'date'])
        incremental_start_dates = df_existing.groupby('stock_code')['date'].max().to_dict()
        print(f'  已有 {len(incremental_start_dates)} 只股票的历史数据')
        del df_existing  # 释放内存

    progress = load_progress()
    errors = load_errors()
    completed = get_completed_codes(progress, 'step2') if resume else set()

    if resume and completed:
        print(f'  断点续传: 已完成 {len(completed)}/{total}')

    success_count = len(completed)
    fail_count = 0
    skip_count = 0

    for i, code in enumerate(codes):
        if code in completed:
            continue

        pct = (i + 1) / total * 100
        print(f'  [{i+1}/{total}] ({pct:.1f}%) {code}', end='', flush=True)

        # 增量模式: 确定起始日期
        if incremental and code in incremental_start_dates:
            last_date = incremental_start_dates[code]
            fetch_start = (last_date + timedelta(days=1)).strftime('%Y%m%d')
            # 如果最新日期就是今天，跳过
            if fetch_start > END_DATE:
                print(f' → 已是最新')
                mark_completed(progress, 'step2', code)
                skip_count += 1
                success_count += 1
                continue
        else:
            fetch_start = START_DATE

        ok = False
        tx_symbol = code_to_tx_symbol(code)
        for attempt in range(MAX_RETRIES):
            try:
                df = ak.stock_zh_a_daily(
                    symbol=tx_symbol,
                    start_date=fetch_start, end_date=END_DATE,
                    adjust='qfq'
                )
                if df is not None and not df.empty:
                    # 腾讯源列: date,open,high,low,close,volume,amount,outstanding_share,turnover
                    df['stock_code'] = code
                    df['date'] = pd.to_datetime(df['date'])
                    # turnover 是比率(0.0047), 转为百分比(0.47)
                    df['turnover_rate'] = df['turnover'] * 100 if 'turnover' in df.columns else None
                    # 从 close 计算涨跌幅(%)
                    df = df.sort_values('date')
                    df['pct_change'] = df['close'].pct_change() * 100
                    df = df[['stock_code', 'date', 'open', 'high', 'low', 'close',
                             'volume', 'amount', 'turnover_rate', 'pct_change']]

                    temp_path = os.path.join(temp_dir, f'{code}.parquet')
                    df.to_parquet(temp_path, index=False, engine='pyarrow')
                    mode = '增量' if incremental and code in incremental_start_dates else '全量'
                    print(f' → {len(df)}行({mode}) ✓')
                    ok = True
                    break
                else:
                    print(f' → 无新数据')
                    ok = True
                    break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    print(f' 重试{attempt+1}', end='', flush=True)
                    time.sleep(2)
                else:
                    print(f' ✗ {str(e)[:50]}')
                    record_error(errors, 'step2', code, str(e))

        if ok:
            success_count += 1
            mark_completed(progress, 'step2', code)
        else:
            fail_count += 1

        time.sleep(REQUEST_INTERVAL)

    # 分批合并
    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'date'])

    if incremental:
        print(f'  成功: {success_count}, 跳过(已最新): {skip_count}, 失败: {fail_count}')
    else:
        print(f'  成功: {success_count}, 失败: {fail_count}')
    return True


# ============================================================
# Step 3: PE/PB/市值
#   全量模式: 逐只调百度估值接口拉5年历史
#   增量模式: 同样调百度逐只, 但跳过已是最新的, 只追加新日期数据
#   ⚠️ 不能用 _em 后缀接口 (129 服务器 IP 已被东方财富拉黑)
# ============================================================
def step3_valuation_daily(resume=False, incremental=False, limit=0):
    print('=' * 60)
    print(f'Step 3: {"增量更新" if incremental else "全量下载"} PE/PB/市值')
    print('=' * 60)

    stock_list_path = os.path.join(OUTPUT_DIR, 'stock_list.parquet')
    output_path = os.path.join(OUTPUT_DIR, 'valuation_daily.parquet')

    if not os.path.exists(stock_list_path):
        print('  ❌ stock_list.parquet 不存在，请先运行 Step 0')
        return False

    # ---- 增量模式: 百度逐只, 跳过已是最新的 ----
    if incremental:
        return _step3_incremental(stock_list_path, output_path, limit)

    # ---- 全量模式: 逐只调百度 ----
    return _step3_full(stock_list_path, output_path, resume, limit)


def _step3_incremental(stock_list_path, output_path, limit=0):
    """周更模式 V4 (2026-04-13 由 baidu 切到 baostock + cninfo + 季报 + div 修正)

    数据源:
      - daily (pe/pb/ps/pcf + close): baostock query_history_k_data_plus (adjustflag='3' 未复权)
      - share timeline: cninfo 全市场股本变动 + baostock 季报 pubDate 兜底 + dividend_history 除权日对齐
      - total_mv: close × total_share (单位: 元)
    每只股票按自己的 last_date+1 开始拉, 追加写入, 不破坏存量.
    """
    # 依赖 V4 工具函数 (同目录 download_valuation_baostock_v4_full.py)
    if SCRIPT_DIR not in sys.path:
        sys.path.insert(0, SCRIPT_DIR)
    try:
        import baostock as bs
        from download_valuation_baostock_v4_full import (
            to_bs_code, fix_cninfo_with_divhistory, merge_share_timeline, compute_cap,
        )
    except ImportError as e:
        print(f'  ❌ 依赖缺失: {e}')
        print(f'     需要 baostock 库 + 同目录下的 download_valuation_baostock_v4_full.py')
        return False

    if not os.path.exists(output_path):
        print(f'  ❌ {output_path} 不存在, 请先跑全量 V4')
        return False
    if not os.path.exists(CNINFO_PATH):
        print(f'  ❌ {CNINFO_PATH} 不存在 (V4 股本时间线数据源)')
        return False

    # ---- 备份 ----
    import shutil
    backup_path = output_path + '.bak'
    shutil.copy2(output_path, backup_path)
    backup_mb = os.path.getsize(backup_path) / 1024 / 1024
    print(f'  ✅ 已备份: {os.path.basename(backup_path)} ({backup_mb:.1f}MB)')

    # ---- 读存量 ----
    df_old = pd.read_parquet(output_path)
    last_dates = df_old.groupby('stock_code')['date'].max().to_dict()
    old_cols = df_old.columns.tolist()
    print(f'  现有 {len(df_old)} 行, 覆盖 {len(last_dates)} 只, 字段: {old_cols}')

    # ---- 读 cninfo + dividend_history ----
    import numpy as np
    cninfo_all = pd.read_parquet(CNINFO_PATH)
    cninfo_dict = {code: sub for code, sub in cninfo_all.groupby('stock_code')}
    print(f'  cninfo: {len(cninfo_all)} 行, {len(cninfo_dict)} 只')

    div_path = os.path.join(OUTPUT_DIR, 'dividend_history.parquet')
    if os.path.exists(div_path):
        div_all = pd.read_parquet(div_path)
        div_all['ex_dividend_date'] = pd.to_datetime(div_all['ex_dividend_date'], errors='coerce')
        div_all = div_all.dropna(subset=['ex_dividend_date'])
        div_all['bonus_shares'] = pd.to_numeric(div_all['bonus_shares'], errors='coerce').fillna(0)
        div_all['transfer_shares'] = pd.to_numeric(div_all['transfer_shares'], errors='coerce').fillna(0)
        div_dict = {code: sub for code, sub in div_all.groupby('stock_code')}
        print(f'  dividend_history: {len(div_all)} 行, {len(div_dict)} 只')
    else:
        div_dict = {}
        print(f'  ⚠ dividend_history 缺失, 跳过除权日对齐 fix')

    # ---- 读股票清单 ----
    stock_list = pd.read_parquet(stock_list_path)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    today = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
    today_yyyymmdd = today.strftime('%Y-%m-%d')
    current_year = today.year
    errors = load_errors()

    # ---- baostock 登录 ----
    lg = bs.login()
    if lg.error_code != '0':
        print(f'  ❌ baostock login 失败: {lg.error_msg}')
        return False
    print(f'  ✅ baostock login ok')

    new_rows = []
    skipped = 0
    success = 0
    fail = 0

    try:
        for i, code in enumerate(codes):
            last_date = last_dates.get(code)
            if last_date is not None and last_date >= today:
                skipped += 1
                continue

            if last_date is not None:
                start_date = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                start_date = '2021-04-12'

            pct = (i + 1) / total * 100
            print(f'  [{i+1}/{total}] ({pct:.1f}%) {code}', end='', flush=True)

            ok = False
            last_err = None
            for attempt in range(MAX_RETRIES):
                try:
                    bs_code = to_bs_code(code)

                    # 1. 拉 daily (仅新增日期)
                    rs = bs.query_history_k_data_plus(
                        bs_code, 'date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM',
                        start_date=start_date, end_date=today_yyyymmdd,
                        frequency='d', adjustflag='3'
                    )
                    if rs.error_code != '0':
                        last_err = f'daily: {rs.error_msg}'
                        bs.logout(); bs.login()
                        time.sleep(1 + attempt)
                        continue
                    daily_rows = []
                    while rs.next():
                        daily_rows.append(rs.get_row_data())
                    if not daily_rows:
                        print(' → 无新交易日')
                        skipped += 1
                        ok = True
                        break
                    daily = pd.DataFrame(daily_rows, columns=rs.fields)
                    daily['date'] = pd.to_datetime(daily['date'])
                    for c in ['close', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM']:
                        daily[c] = pd.to_numeric(daily[c], errors='coerce')

                    # 2. 拉季报 (当前+上年 = 最多 8 次查询, cninfo 兜底用)
                    bao_rows = []
                    for year in [current_year, current_year - 1]:
                        for q in range(1, 5):
                            rs2 = bs.query_profit_data(code=bs_code, year=year, quarter=q)
                            if rs2.error_code != '0':
                                continue
                            try:
                                while rs2.next():
                                    bao_rows.append(dict(zip(rs2.fields, rs2.get_row_data())))
                            except Exception:
                                continue
                    if bao_rows:
                        bao_sh = pd.DataFrame(bao_rows)
                        bao_sh['pubDate'] = pd.to_datetime(bao_sh['pubDate'], errors='coerce')
                        bao_sh['totalShare'] = pd.to_numeric(bao_sh['totalShare'], errors='coerce')
                        bao_sh = bao_sh.dropna(subset=['pubDate', 'totalShare']).sort_values('pubDate').reset_index(drop=True)
                    else:
                        bao_sh = pd.DataFrame()

                    # 3. 合并 share timeline + div 修正
                    cninfo_sh = cninfo_dict.get(code, pd.DataFrame())
                    div_ev = div_dict.get(code, pd.DataFrame())
                    cninfo_sh_fixed = fix_cninfo_with_divhistory(cninfo_sh, div_ev) if len(cninfo_sh) > 0 else cninfo_sh
                    timeline = merge_share_timeline(cninfo_sh_fixed, bao_sh)

                    # 4. 计算 total_mv (compute_cap 输出是亿元, 我们要元)
                    merged = compute_cap(daily, timeline)
                    merged['stock_code'] = code
                    merged = merged.rename(columns={
                        'peTTM': 'pe_ttm', 'pbMRQ': 'pb',
                        'psTTM': 'ps_ttm', 'pcfNcfTTM': 'pcf_ttm',
                    })
                    if 'total_mv' in merged.columns:
                        merged['total_mv'] = merged['total_mv'] * 1e8  # 亿元 → 元

                    out = merged[['stock_code', 'date', 'pe_ttm', 'pb', 'ps_ttm', 'pcf_ttm', 'total_share', 'total_mv']].copy()
                    if last_date is not None:
                        out = out[out['date'] > last_date]

                    if not out.empty:
                        new_rows.append(out)
                        print(f' → +{len(out)}行 ✓')
                    else:
                        print(' → 无新数据')
                    ok = True
                    break

                except Exception as e:
                    last_err = str(e)[:80]
                    if attempt < MAX_RETRIES - 1:
                        print(f' 重试{attempt+1}', end='', flush=True)
                        try: bs.logout()
                        except Exception: pass
                        try: bs.login()
                        except Exception: pass
                        time.sleep(2)

            if ok:
                success += 1
            else:
                print(f' ✗ {last_err}')
                fail += 1
                record_error(errors, 'step3_inc', code, last_err or 'unknown')

    finally:
        try: bs.logout()
        except Exception: pass

    # ---- 合并写回 (schema 严格对齐 df_old) ----
    if new_rows:
        df_new = pd.concat(new_rows, ignore_index=True)
        # schema 对齐
        for col in old_cols:
            if col not in df_new.columns:
                df_new[col] = None
        df_new = df_new[old_cols]
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(
            subset=['stock_code', 'date'], keep='last'
        ).sort_values(['stock_code', 'date']).reset_index(drop=True)
        # 用 polars 写, 和 V4 全量 parquet 保持一致 (小 row_group → zstd 压缩率更高)
        import polars as pl
        pl.from_pandas(df_combined).write_parquet(output_path, compression='zstd')
        size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f'  ✅ 新增 {len(df_new)} 行, 总计 {len(df_combined)} 行, {size_mb:.1f}MB')
    else:
        print('  ℹ️ 无新数据需追加, parquet 未变动')

    print(f'  成功: {success}, 失败: {fail}, 跳过(已最新): {skipped}')
    return True


def _step3_full(stock_list_path, output_path, resume, limit=0):
    """全量: 逐只调百度估值接口拉5年历史 PE/PB/市值"""
    temp_dir = os.path.join(OUTPUT_DIR, 'temp_valuation')
    os.makedirs(temp_dir, exist_ok=True)

    stock_list = pd.read_parquet(stock_list_path)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    progress = load_progress()
    errors = load_errors()
    completed = get_completed_codes(progress, 'step3') if resume else set()

    if resume and completed:
        print(f'  断点续传: 已完成 {len(completed)}/{total}')

    success_count = len(completed)
    fail_count = 0

    indicators = [
        ('市盈率(TTM)', 'pe_ttm'),
        ('市净率', 'pb'),
        ('总市值', 'total_mv'),
    ]

    for i, code in enumerate(codes):
        if code in completed:
            continue

        pct = (i + 1) / total * 100
        print(f'  [{i+1}/{total}] ({pct:.1f}%) {code}', end='', flush=True)

        ok = False
        for attempt in range(MAX_RETRIES):
            try:
                dfs_indicator = []
                for indicator_name, col_name in indicators:
                    df_val = ak.stock_zh_valuation_baidu(
                        symbol=code, indicator=indicator_name, period='近五年'
                    )
                    if df_val is not None and not df_val.empty:
                        df_val = df_val.rename(columns={'date': 'date', 'value': col_name})
                        df_val['date'] = pd.to_datetime(df_val['date'])
                        dfs_indicator.append(df_val.set_index('date')[[col_name]])
                    time.sleep(0.3)  # 同一只股票3次调用之间间隔

                if dfs_indicator:
                    merged = pd.concat(dfs_indicator, axis=1).reset_index()
                    merged['stock_code'] = code
                    merged = merged[['stock_code', 'date', 'pe_ttm', 'pb', 'total_mv']]
                    temp_path = os.path.join(temp_dir, f'{code}.parquet')
                    merged.to_parquet(temp_path, index=False, engine='pyarrow')
                    print(f' → {len(merged)}行 ✓')
                else:
                    print(f' → 无数据')

                ok = True
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    print(f' 重试{attempt+1}', end='', flush=True)
                    time.sleep(3)
                else:
                    print(f' ✗ {str(e)[:50]}')
                    record_error(errors, 'step3', code, str(e))

        if ok:
            success_count += 1
            mark_completed(progress, 'step3', code)
        else:
            fail_count += 1

        time.sleep(REQUEST_INTERVAL_BAIDU)

    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'date'])
    print(f'  成功: {success_count}, 失败: {fail_count}')
    return True


# ============================================================
# Step 4: 逐只下载财务摘要 (ROE / 增长率)
# ============================================================

# 需要提取的行名 → 列名映射
# stock_financial_abstract 返回: ~80行 x N列
# 第一列="选项", 第二列="指标"(行名), 后面列=各季度
# 行名可能因股票不同而变化，所以用行名匹配而非行号
FINANCIAL_ROW_MAPPING = {
    '净资产收益率(ROE)': 'roe',
    '营业总收入': 'revenue',
    '归母净利润': 'net_profit',
    '归属母公司净利润': 'net_profit',       # 兼容不同版本
    '营业总收入增长率': 'revenue_growth_yoy',
    '营业总收入同比增长率': 'revenue_growth_yoy',  # 兼容不同版本
    '归属母公司净利润增长率': 'profit_growth_yoy',
    '归属母公司净利润同比增长率': 'profit_growth_yoy',  # 兼容不同版本
}


def calc_publish_deadline(report_date_str):
    """根据报告期计算最迟披露日(回测中该日期之后才能使用此数据)"""
    month = int(report_date_str[4:6])
    year = int(report_date_str[:4])
    if month == 3:     # Q1 → 当年4月30日
        return f'{year}0501'
    elif month == 6:   # H1 → 当年8月31日
        return f'{year}0901'
    elif month == 9:   # Q3 → 当年10月31日
        return f'{year}1101'
    elif month == 12:  # 年报 → 次年4月30日
        return f'{year+1}0501'
    else:
        return f'{year}0501'


def step4_financial_quarterly(resume=False, incremental=False, limit=0):
    print('=' * 60)
    print(f'Step 4: {"增量更新" if incremental else "全量下载"}季度财务数据 (ROE/增长率)')
    print('=' * 60)

    stock_list_path = os.path.join(OUTPUT_DIR, 'stock_list.parquet')
    output_path = os.path.join(OUTPUT_DIR, 'financial_quarterly.parquet')
    temp_dir = os.path.join(OUTPUT_DIR, 'temp_financial')
    os.makedirs(temp_dir, exist_ok=True)

    if not os.path.exists(stock_list_path):
        print('  ❌ stock_list.parquet 不存在，请先运行 Step 0')
        return False

    stock_list = pd.read_parquet(stock_list_path)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    # 增量模式: 获取已有数据的最新报告期
    existing_max_reports = {}
    if incremental and os.path.exists(output_path):
        print('  读取已有数据的最新报告期...')
        df_existing = pd.read_parquet(output_path, columns=['stock_code', 'report_date'])
        existing_max_reports = df_existing.groupby('stock_code')['report_date'].max().to_dict()
        print(f'  已有 {len(existing_max_reports)} 只股票的财务数据')
        del df_existing

    progress = load_progress()
    errors = load_errors()
    completed = get_completed_codes(progress, 'step4') if resume else set()

    if resume and completed:
        print(f'  断点续传: 已完成 {len(completed)}/{total}')

    success_count = len(completed)
    fail_count = 0

    for i, code in enumerate(codes):
        if code in completed:
            continue

        pct = (i + 1) / total * 100
        print(f'  [{i+1}/{total}] ({pct:.1f}%) {code}', end='', flush=True)

        ok = False
        for attempt in range(MAX_RETRIES):
            try:
                df_raw = ak.stock_financial_abstract(symbol=code)

                if df_raw is None or df_raw.empty:
                    print(f' → 无数据')
                    ok = True
                    break

                indicator_col = df_raw.columns[1]
                quarter_cols = [c for c in df_raw.columns[2:]
                                if str(c).isdigit() and len(str(c)) == 8]

                # 只保留5年内
                five_years_ago = int(START_DATE[:4])
                quarter_cols = [c for c in quarter_cols if int(str(c)[:4]) >= five_years_ago]

                # 增量: 只保留比已有数据更新的季度
                if incremental and code in existing_max_reports:
                    max_existing = existing_max_reports[code].strftime('%Y%m%d')
                    quarter_cols = [c for c in quarter_cols if str(c) > max_existing]
                    if not quarter_cols:
                        print(f' → 已是最新')
                        ok = True
                        break

                if not quarter_cols:
                    print(f' → 无近5年数据')
                    ok = True
                    break

                # 用行名匹配找需要的行
                row_indices = {}
                for idx, row in df_raw.iterrows():
                    row_name = str(row[indicator_col]).strip()
                    if row_name in FINANCIAL_ROW_MAPPING:
                        target_col = FINANCIAL_ROW_MAPPING[row_name]
                        if target_col not in row_indices:
                            row_indices[target_col] = idx

                records = []
                for qcol in quarter_cols:
                    record = {
                        'stock_code': code,
                        'report_date': pd.to_datetime(str(qcol)),
                        'publish_deadline': pd.to_datetime(calc_publish_deadline(str(qcol))),
                    }
                    for target_col, row_idx in row_indices.items():
                        val = df_raw.at[row_idx, qcol]
                        try:
                            record[target_col] = float(val)
                        except (ValueError, TypeError):
                            record[target_col] = None
                    records.append(record)

                if records:
                    df_result = pd.DataFrame(records)
                    for col in ['roe', 'revenue', 'net_profit',
                                'revenue_growth_yoy', 'profit_growth_yoy']:
                        if col not in df_result.columns:
                            df_result[col] = None
                    df_result = df_result[['stock_code', 'report_date', 'publish_deadline',
                                           'roe', 'revenue', 'net_profit',
                                           'revenue_growth_yoy', 'profit_growth_yoy']]
                    temp_path = os.path.join(temp_dir, f'{code}.parquet')
                    df_result.to_parquet(temp_path, index=False, engine='pyarrow')
                    print(f' → {len(df_result)}期 ✓')
                else:
                    print(f' → 未提取到数据')

                ok = True
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    print(f' 重试{attempt+1}', end='', flush=True)
                    time.sleep(2)
                else:
                    print(f' ✗ {str(e)[:50]}')
                    record_error(errors, 'step4', code, str(e))

        if ok:
            success_count += 1
            mark_completed(progress, 'step4', code)
        else:
            fail_count += 1

        time.sleep(REQUEST_INTERVAL)

    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'report_date'])
    print(f'  成功: {success_count}, 失败: {fail_count}')
    return True


# ============================================================
# Step 5: 逐只下载分红历史 (列名容错)
# ============================================================

# 分红接口可能返回的列名变体
DIVIDEND_COLUMN_MAPPING = {
    'announce_date': ['公告日期', '报告日期', '公告时间'],
    'dividend_per_share': ['派息', '每股派息', '派息(税前)', '每股分红'],
    'bonus_shares': ['送股', '送股(股)', '每10股送股'],
    'transfer_shares': ['转增', '转增(股)', '每10股转增'],
    'ex_dividend_date': ['除权除息日', '除息日', '除权日'],
    'progress': ['进度', '方案进度', '实施进度'],
}


def step5_dividend_history(resume=False, incremental=False, limit=0):
    print('=' * 60)
    print(f'Step 5: {"增量更新" if incremental else "全量下载"}分红历史')
    print('=' * 60)

    stock_list_path = os.path.join(OUTPUT_DIR, 'stock_list.parquet')
    output_path = os.path.join(OUTPUT_DIR, 'dividend_history.parquet')
    temp_dir = os.path.join(OUTPUT_DIR, 'temp_dividend')
    os.makedirs(temp_dir, exist_ok=True)

    if not os.path.exists(stock_list_path):
        print('  ❌ stock_list.parquet 不存在，请先运行 Step 0')
        return False

    stock_list = pd.read_parquet(stock_list_path)
    codes = stock_list['stock_code'].tolist()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)

    progress = load_progress()
    errors = load_errors()
    completed = get_completed_codes(progress, 'step5') if resume else set()

    if resume and completed:
        print(f'  断点续传: 已完成 {len(completed)}/{total}')

    success_count = len(completed)
    fail_count = 0

    for i, code in enumerate(codes):
        if code in completed:
            continue

        pct = (i + 1) / total * 100
        print(f'  [{i+1}/{total}] ({pct:.1f}%) {code}', end='', flush=True)

        ok = False
        for attempt in range(MAX_RETRIES):
            try:
                df = ak.stock_history_dividend_detail(symbol=code, indicator='分红')

                if df is not None and not df.empty:
                    # 容错列名映射
                    df = flexible_rename(df, DIVIDEND_COLUMN_MAPPING)
                    df['stock_code'] = code

                    # 检查关键列是否存在，缺失的填 None
                    required_cols = ['announce_date', 'dividend_per_share', 'bonus_shares',
                                     'transfer_shares', 'ex_dividend_date', 'progress']
                    for col in required_cols:
                        if col not in df.columns:
                            df[col] = None

                    # 日期转换
                    for date_col in ['announce_date', 'ex_dividend_date']:
                        if df[date_col] is not None:
                            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

                    # 数值转换
                    for num_col in ['dividend_per_share', 'bonus_shares', 'transfer_shares']:
                        df[num_col] = pd.to_numeric(df[num_col], errors='coerce')

                    # 只保留近5年
                    start = pd.to_datetime(START_DATE)
                    if df['announce_date'].notna().any():
                        df = df[df['announce_date'] >= start].copy()

                    if not df.empty:
                        df = df[['stock_code'] + required_cols]
                        temp_path = os.path.join(temp_dir, f'{code}.parquet')
                        df.to_parquet(temp_path, index=False, engine='pyarrow')
                        print(f' → {len(df)}条 ✓')
                    else:
                        print(f' → 5年内无分红')
                else:
                    print(f' → 无分红记录')

                ok = True
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    print(f' 重试{attempt+1}', end='', flush=True)
                    time.sleep(2)
                else:
                    print(f' ✗ {str(e)[:50]}')
                    record_error(errors, 'step5', code, str(e))

        if ok:
            success_count += 1
            mark_completed(progress, 'step5', code)
        else:
            fail_count += 1

        time.sleep(REQUEST_INTERVAL)

    merge_temp_files(temp_dir, output_path, sort_columns=['stock_code', 'announce_date'])
    print(f'  成功: {success_count}, 失败: {fail_count}')
    return True


# ============================================================
# 验证
# ============================================================
def validate():
    print('=' * 60)
    print('数据验证')
    print('=' * 60)

    files = {
        'stock_list': ('stock_list.parquet', None),
        'index_daily': ('index_daily.parquet', None),
        'daily_ohlcv': ('daily_ohlcv.parquet', ['stock_code', 'date']),
        'valuation_daily': ('valuation_daily.parquet', ['stock_code', 'date']),
        'financial_quarterly': ('financial_quarterly.parquet', ['stock_code', 'report_date']),
        'dividend_history': ('dividend_history.parquet', ['stock_code', 'announce_date']),
    }

    for name, (filename, key_cols) in files.items():
        path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(path):
            df = pd.read_parquet(path)
            size_mb = os.path.getsize(path) / 1024 / 1024
            print(f'  ✅ {name}: {len(df)} 行, {size_mb:.1f}MB, 列: {list(df.columns)}')

            if 'stock_code' in df.columns:
                print(f'       覆盖 {df["stock_code"].nunique()} 只股票')

            if 'date' in df.columns:
                print(f'       日期: {df["date"].min()} ~ {df["date"].max()}')
            elif 'report_date' in df.columns:
                print(f'       报告期: {df["report_date"].min()} ~ {df["report_date"].max()}')

            # 空值检查
            null_pct = df.isnull().mean()
            high_null = null_pct[null_pct > 0.1]
            if not high_null.empty:
                print(f'       ⚠ 高空值率列: {dict(high_null.round(3))}')

            # 重复检查
            if key_cols and all(c in df.columns for c in key_cols):
                dups = df.duplicated(subset=key_cols).sum()
                if dups > 0:
                    print(f'       ⚠ 发现 {dups} 条重复数据 (按 {key_cols})')
        else:
            print(f'  ❌ {name}: 文件不存在')

    # 对齐抽检
    ohlcv_path = os.path.join(OUTPUT_DIR, 'daily_ohlcv.parquet')
    val_path = os.path.join(OUTPUT_DIR, 'valuation_daily.parquet')
    if os.path.exists(ohlcv_path) and os.path.exists(val_path):
        print('\n  对齐抽检 (OHLCV vs 估值):')
        df_ohlcv = pd.read_parquet(ohlcv_path, columns=['stock_code', 'date'])
        df_val = pd.read_parquet(val_path, columns=['stock_code', 'date'])
        sample_codes = df_ohlcv['stock_code'].drop_duplicates().sample(
            min(5, df_ohlcv['stock_code'].nunique()), random_state=42
        ).tolist()
        for code in sample_codes:
            ohlcv_dates = set(df_ohlcv[df_ohlcv['stock_code'] == code]['date'])
            val_dates = set(df_val[df_val['stock_code'] == code]['date'])
            if val_dates:
                overlap = len(ohlcv_dates & val_dates) / max(len(ohlcv_dates), 1) * 100
                print(f'    {code}: OHLCV {len(ohlcv_dates)}天, 估值 {len(val_dates)}天, 重叠 {overlap:.1f}%')
            else:
                print(f'    {code}: 无估值数据')
        del df_ohlcv, df_val

    # 财务时间泄露检查
    fin_path = os.path.join(OUTPUT_DIR, 'financial_quarterly.parquet')
    if os.path.exists(fin_path):
        df_fin = pd.read_parquet(fin_path, columns=['report_date', 'publish_deadline'])
        bad = df_fin[df_fin['publish_deadline'] <= df_fin['report_date']]
        if bad.empty:
            print(f'\n  ✅ 财务时间泄露检查通过')
        else:
            print(f'\n  ❌ 发现 {len(bad)} 条 publish_deadline <= report_date')
        del df_fin


# ============================================================
# 主入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='A股量化回测数据下载 v2')
    parser.add_argument('--step', type=int, choices=[0, 1, 2, 3, 4, 5, 99],
                        help='执行指定步骤 (0=股票列表, 1=指数, 2=日线, 3=估值, 4=财务, 5=分红, 99=验证)')
    parser.add_argument('--resume', action='store_true',
                        help='断点续传 (从上次中断位置继续)')
    parser.add_argument('--incremental', action='store_true',
                        help='增量更新 (只下载已有数据之后的新数据)')
    parser.add_argument('--limit', type=int, default=0,
                        help='限制处理的股票数量 (测试用, 0=不限制)')
    args = parser.parse_args()

    ensure_output_dir()

    mode = '增量更新' if args.incremental else '全量下载'
    print(f'运行模式: {mode}')
    print(f'数据输出目录: {OUTPUT_DIR}')
    print(f'数据范围: {START_DATE} ~ {END_DATE} (约5年)')
    print()

    start_time = time.time()

    lmt = args.limit

    if args.step is not None:
        if args.step == 0:
            step0_stock_list()
        elif args.step == 1:
            step1_index_daily(incremental=args.incremental)
        elif args.step == 2:
            step2_daily_ohlcv(resume=args.resume, incremental=args.incremental, limit=lmt)
        elif args.step == 3:
            step3_valuation_daily(resume=args.resume, incremental=args.incremental, limit=lmt)
        elif args.step == 4:
            step4_financial_quarterly(resume=args.resume, incremental=args.incremental, limit=lmt)
        elif args.step == 5:
            step5_dividend_history(resume=args.resume, incremental=args.incremental, limit=lmt)
        elif args.step == 99:
            validate()
    else:
        if args.incremental:
            step1_index_daily(incremental=True)
            step2_daily_ohlcv(resume=args.resume, incremental=True, limit=lmt)
            step3_valuation_daily(incremental=True, limit=lmt)
            step4_financial_quarterly(resume=args.resume, incremental=True, limit=lmt)
            step5_dividend_history(resume=args.resume, incremental=True, limit=lmt)
        else:
            step0_stock_list()
            step1_index_daily()
            step2_daily_ohlcv(resume=args.resume, limit=lmt)
            step3_valuation_daily(resume=args.resume, limit=lmt)
            step4_financial_quarterly(resume=args.resume, limit=lmt)
            step5_dividend_history(resume=args.resume, limit=lmt)

        print('\n下载完成，开始验证...\n')
        validate()

    elapsed = time.time() - start_time
    hours = int(elapsed // 3600)
    mins = int((elapsed % 3600) // 60)
    print(f'\n总耗时: {hours}小时{mins}分钟')


if __name__ == '__main__':
    main()
