# -*- coding: utf-8 -*-
"""
董监高增减持回填 — 沪深北交易所直连 (信息模块 Phase 1 剩余项③, 2026-07-04)

- 三所全市场披露列表分页拉取(akshare 同名函数的 API 参数照搬, 但自带重试+限速,
  akshare 内置版无重试, 深交所 6148 页必被掐断)
- 落 SQLite: astock_data/info_archive.db
    holder_changes(exchange, code, ..., row_key UNIQUE)  按行内容 md5 去重
    holder_progress(exchange, next_page, total_pages, rows, updated_at)  断点续传
- 限速 0.25~0.45s/页, 指数退避 5 次

用法:
  venv/bin/python info_backfill_holder_changes.py            # 全量三所(断点续传)
  venv/bin/python info_backfill_holder_changes.py --smoke    # 每所只抓前3页烟测
  venv/bin/python info_backfill_holder_changes.py --exchange szse
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import random
import sqlite3
import time

import requests

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "astock_data", "info_archive.db")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

FIELDS = ["exchange", "code", "name", "person", "position", "actor", "relation",
          "shares_before", "change_shares", "avg_price", "shares_after",
          "change_ratio", "reason", "change_date", "disclosure_date"]


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS holder_changes(
        exchange TEXT NOT NULL,
        code TEXT NOT NULL,
        name TEXT,
        person TEXT,
        position TEXT,
        actor TEXT,
        relation TEXT,
        shares_before REAL,
        change_shares REAL,
        avg_price REAL,
        shares_after REAL,
        change_ratio REAL,
        reason TEXT,
        change_date TEXT,
        disclosure_date TEXT,
        row_key TEXT UNIQUE)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hc_code_date ON holder_changes(code, change_date)")
    conn.execute("""CREATE TABLE IF NOT EXISTS holder_progress(
        exchange TEXT PRIMARY KEY,
        next_page INTEGER,
        total_pages INTEGER,
        rows INTEGER DEFAULT 0,
        updated_at TEXT)""")
    conn.commit()
    return conn


SLEEP_RANGE = [0.25, 0.45]


def polite_sleep():
    time.sleep(random.uniform(*SLEEP_RANGE))


def get_with_retry(session, url, params, headers, max_tries=5, jsonp=False):
    delay = 5
    for attempt in range(max_tries):
        try:
            polite_sleep()
            r = session.get(url, params=params, headers=headers, timeout=30)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            if jsonp:
                return json.loads(r.text.strip("null(").strip(")"))
            return r.json()
        except Exception as e:
            if attempt == max_tries - 1:
                raise
            print(f"    retry {attempt + 1} after {delay}s: {type(e).__name__}: {e}", flush=True)
            time.sleep(delay)
            delay = min(delay * 3, 300)
    return None


def num(v):
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def d10(v):
    s = str(v or "").strip()
    return s[:10] if s else None


def save_rows(conn, rows):
    n = 0
    for row in rows:
        vals = [row.get(f) for f in FIELDS]
        key = hashlib.md5("|".join(str(v) for v in vals).encode("utf-8")).hexdigest()
        cur = conn.execute(
            f"INSERT OR IGNORE INTO holder_changes({','.join(FIELDS)}, row_key) "
            f"VALUES({','.join('?' * len(FIELDS))}, ?)", vals + [key])
        n += cur.rowcount
    conn.commit()
    return n


def run_exchange(conn, exchange, get_total, fetch_page, first_page, smoke_pages=0):
    session = requests.Session()
    prog = conn.execute("SELECT next_page, total_pages FROM holder_progress WHERE exchange=?",
                        (exchange,)).fetchone()
    total = get_total(session)
    start = prog[0] if prog and prog[0] is not None and prog[0] > first_page else first_page
    end = total - 1 + first_page
    if smoke_pages:
        end = min(end, start + smoke_pages - 1)
    print(f"[{exchange}] pages {start}..{end} (total={total})", flush=True)
    t0 = time.time()
    n_total = 0
    for page in range(start, end + 1):
        rows = fetch_page(session, page)
        n_total += save_rows(conn, rows)
        conn.execute("INSERT OR REPLACE INTO holder_progress VALUES(?,?,?,?,?)",
                     (exchange, page + 1, total, n_total, dt.datetime.now().isoformat()))
        conn.commit()
        done_pages = page - start + 1
        if done_pages % 50 == 0 or page == end:
            rate = done_pages / (time.time() - t0) * 3600
            eta_h = (end - page) / max(rate, 1)
            print(f"  [{exchange} {page}/{end}] saved={n_total} rate={rate:.0f}p/h eta={eta_h:.1f}h",
                  flush=True)
    print(f"[{exchange}] DONE saved={n_total}", flush=True)


# ---------- SSE 上交所 ----------
SSE_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_HEADERS = {"Host": "query.sse.com.cn", "Referer": "https://www.sse.com.cn/", "User-Agent": UA}


def sse_params(page):
    return {
        "isPagination": "true",
        "pageHelp.pageSize": "100",
        "pageHelp.pageNo": str(page),
        "pageHelp.beginPage": str(page),
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": str(page),
        "sqlId": "COMMON_SSE_XXPL_CXJL_SSGSGFBDQK_S",
        "COMPANY_CODE": "",
        "NAME": "",
        "BEGIN_DATE": "1990-01-01",
        "END_DATE": "2050-01-01",
        "BOARDTYPE": "",
    }


def sse_total(session):
    j = get_with_retry(session, SSE_URL, sse_params(1), SSE_HEADERS)
    return int(j["pageHelp"]["pageCount"])


def sse_page(session, page):
    j = get_with_retry(session, SSE_URL, sse_params(page), SSE_HEADERS)
    out = []
    for it in j.get("result") or []:
        out.append({
            "exchange": "sse",
            "code": str(it.get("COMPANY_CODE") or "").strip(),
            "name": it.get("COMPANY_ABBR"),
            "person": it.get("NAME"),
            "position": it.get("DUTY"),
            "shares_before": num(it.get("CURRENT_NUM")),
            "change_shares": num(it.get("CHANGE_NUM")),
            "avg_price": num(it.get("CURRENT_AVG_PRICE")),
            "shares_after": num(it.get("HOLDSTOCK_NUM")),
            "reason": it.get("CHANGE_REASON"),
            "change_date": d10(it.get("CHANGE_DATE")),
            "disclosure_date": d10(it.get("FORM_DATE")),
        })
    return out


# ---------- SZSE 深交所 ----------
SZSE_URL = "https://www.szse.cn/api/report/ShowReport/data"
SZSE_HEADERS = {"User-Agent": UA, "Referer": "https://www.szse.cn/disclosure/supervision/change/index.html"}


def szse_params(page):
    return {"SHOWTYPE": "JSON", "CATALOGID": "1801_cxda", "TABKEY": "tab1",
            "PAGENO": str(page), "random": str(random.random())}


def szse_total(session):
    j = get_with_retry(session, SZSE_URL, szse_params(1), SZSE_HEADERS)
    return int(j[0]["metadata"]["pagecount"])


def szse_page(session, page):
    j = get_with_retry(session, SZSE_URL, szse_params(page), SZSE_HEADERS)
    out = []
    for it in j[0].get("data") or []:
        out.append({
            "exchange": "szse",
            "code": str(it.get("zqdm") or "").strip(),
            "name": it.get("zqjc"),
            "person": it.get("ggxm"),
            "position": it.get("zw"),
            "actor": it.get("gdxm"),
            "relation": it.get("gxlb"),
            "change_shares": num(it.get("bdgs")),
            "avg_price": num(it.get("bdjj")),
            "shares_after": num(it.get("cgzs")),
            "change_ratio": num(it.get("cgbdbl")),
            "reason": it.get("bdyy"),
            "change_date": d10(it.get("jyrq")),
        })
    return out


# ---------- BSE 北交所 ----------
BSE_URL = "https://www.bse.cn/djgCgbdController/getDjgCgbdList.do"
BSE_HEADERS = {"User-Agent": UA}


def bse_params(page):
    return {
        "page": str(page), "startTime": "", "endTime": "", "stockCode": "",
        "djgName": "", "ssgs": "1",
        "sortfield": "bean.change_date desc, bean.stock_code asc, bean.change_amount desc, bean.price",
        "sorttype": "desc",
    }


def bse_total(session):
    j = get_with_retry(session, BSE_URL, bse_params(0), BSE_HEADERS, jsonp=True)
    return int(j[0]["result"]["totalPages"])


def bse_page(session, page):
    j = get_with_retry(session, BSE_URL, bse_params(page), BSE_HEADERS, jsonp=True)
    out = []
    for it in j[0]["result"].get("content") or []:
        out.append({
            "exchange": "bse",
            "code": str(it.get("stockCode") or "").strip(),
            "name": it.get("stockName"),
            "person": it.get("djgName"),
            "position": it.get("duty"),
            "shares_before": num(it.get("preAmount")),
            "change_shares": num(it.get("changeAmount")),
            "avg_price": num(it.get("price")),
            "shares_after": num(it.get("newAmount")),
            "reason": it.get("reason"),
            "change_date": d10(it.get("changeDate")),
        })
    return out


EXCHANGES = {
    "sse": (sse_total, sse_page, 1),
    "szse": (szse_total, szse_page, 1),
    "bse": (bse_total, bse_page, 0),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true", help="每所只抓前3页")
    ap.add_argument("--exchange", type=str, default="", help="只跑指定交易所 sse/szse/bse")
    ap.add_argument("--sleep-min", type=float, default=0.25)
    ap.add_argument("--sleep-max", type=float, default=0.45)
    args = ap.parse_args()
    SLEEP_RANGE[0], SLEEP_RANGE[1] = args.sleep_min, args.sleep_max

    conn = init_db()
    targets = [args.exchange] if args.exchange else ["sse", "szse", "bse"]
    for ex in targets:
        get_total, fetch_page, first_page = EXCHANGES[ex]
        try:
            run_exchange(conn, ex, get_total, fetch_page, first_page,
                         smoke_pages=3 if args.smoke else 0)
        except Exception as e:
            print(f"[{ex}] FATAL {type(e).__name__}: {e}", flush=True)

    n = conn.execute("SELECT exchange, COUNT(*) FROM holder_changes GROUP BY 1").fetchall()
    print(f"FINISHED per-exchange={n}", flush=True)


if __name__ == "__main__":
    main()
