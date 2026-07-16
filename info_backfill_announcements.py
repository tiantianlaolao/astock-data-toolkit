# -*- coding: utf-8 -*-
"""
公告元数据 15 年回填 — 巨潮 hisAnnouncement API 直连
(信息模块 Phase 1, 2026-07-02)

- 覆盖 astock_data/stock_list.parquet 全部股票, 2011-01-01 ~ 今天
- 落库 SQLite: astock_data/info_archive.db
    announcements(code, name, title, publish_time, ann_id, url, category, impact)
    crawl_progress(code, status, ann_count, err, updated_at)
- 断点续传: status='done' 的股票跳过, 随时 kill 随时重跑
- 限速: 每请求 sleep 0.25~0.45s, 失败指数退避最多 5 次, 单股连败记 error 继续下一只
- 只存元数据(标题/时间/链接/分类), 不下载公告 PDF 正文

用法:
  venv/bin/python info_backfill_announcements.py                # 全量(断点续传)
  venv/bin/python info_backfill_announcements.py --limit 3      # 烟测前3只
  venv/bin/python info_backfill_announcements.py --codes 600519 # 指定股票
"""
import argparse
import datetime as dt
import json
import os
import random
import sqlite3
import sys
import time

import pandas as pd
import requests

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "astock_data")
DB_PATH = os.path.join(DATA_DIR, "info_archive.db")
ORGID_CACHE = os.path.join(BASE, "cninfo_orgid_map.json")

QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
STOCK_JSON_URL = "http://www.cninfo.com.cn/new/data/szse_stock.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
}

START_DATE = "2011-01-01"

# 分类规则: 顺序匹配, 先命中先赢。(category, impact, keywords)
RULES = [
    ("业绩预告", "high", ["业绩预告", "业绩快报", "预亏", "预盈", "预增", "预减"]),
    ("定期报告", "high", ["年度报告", "半年度报告", "季度报告"]),
    ("减值", "high", ["商誉减值", "计提减值", "资产减值"]),
    ("解禁", "high", ["限售股份上市流通", "解除限售", "解禁"]),
    ("减持", "high", ["减持"]),
    ("增持", "mid", ["增持"]),
    ("重组并购", "high", ["重大资产重组", "发行股份购买资产", "收购报告书", "要约收购", "吸收合并"]),
    ("停复牌", "high", ["停牌", "复牌"]),
    ("监管", "high", ["问询函", "关注函", "监管函", "警示函", "立案", "行政处罚", "整改"]),
    ("退市风险", "high", ["退市", "风险警示", "暂停上市", "终止上市"]),
    ("质押", "mid", ["质押"]),
    ("回购", "mid", ["回购"]),
    ("分红", "mid", ["利润分配", "权益分派", "分红派息", "除权除息"]),
    ("增发配股", "mid", ["非公开发行", "定向增发", "配股", "向特定对象发行"]),
    ("可转债", "mid", ["可转债", "可转换公司债"]),
    ("担保诉讼", "mid", ["担保", "诉讼", "仲裁"]),
    ("澄清致歉", "mid", ["澄清", "致歉", "更正"]),
    ("治理", "routine", ["股东大会", "董事会", "监事会", "换届", "辞职", "聘任", "章程"]),
]


def classify(title: str):
    for cat, impact, kws in RULES:
        for kw in kws:
            if kw in title:
                return cat, impact
    return "其他", "routine"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS announcements(
        code TEXT NOT NULL,
        name TEXT,
        title TEXT NOT NULL,
        publish_time TEXT NOT NULL,
        ann_id TEXT NOT NULL,
        url TEXT,
        category TEXT,
        impact TEXT,
        UNIQUE(code, ann_id))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ann_code_time ON announcements(code, publish_time)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ann_time ON announcements(publish_time)")
    conn.execute("""CREATE TABLE IF NOT EXISTS crawl_progress(
        code TEXT PRIMARY KEY,
        status TEXT,
        ann_count INTEGER DEFAULT 0,
        err TEXT,
        updated_at TEXT)""")
    conn.commit()
    return conn


def get_orgid_map(session):
    if os.path.exists(ORGID_CACHE):
        with open(ORGID_CACHE, encoding="utf-8") as f:
            return json.load(f)
    r = session.get(STOCK_JSON_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    mapping = {item["code"]: item["orgId"] for item in r.json()["stockList"]}
    with open(ORGID_CACHE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False)
    return mapping


def polite_sleep():
    time.sleep(random.uniform(0.25, 0.45))


def post_with_retry(session, payload, max_tries=5):
    delay = 5
    for attempt in range(max_tries):
        try:
            polite_sleep()
            r = session.post(QUERY_URL, data=payload, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            return r.json()
        except Exception as e:
            if attempt == max_tries - 1:
                raise
            print(f"    retry {attempt + 1} after {delay}s: {type(e).__name__}: {e}", flush=True)
            time.sleep(delay)
            delay = min(delay * 3, 300)


def fetch_stock_range(session, code, org_id, se_start, se_end, page_size=30):
    """抓一只股票一个日期段的全部公告, 返回 list[dict]
    ⚠️ pageSize 必须 30: 巨潮每页内容硬顶 30 条, 但翻页偏移按传入 pageSize 算,
    传大于 30 会跳条漏数据(2026-07-02 烟测实证)。翻页信 hasMore, 不自己算页数。"""
    payload = {
        "pageNum": "1",
        "pageSize": "30",
        "column": "szse",
        "tabName": "fulltext",
        "plate": "",
        "stock": f"{code},{org_id}",
        "searchkey": "",
        "secid": "",
        "category": "",
        "trade": "",
        "seDate": f"{se_start}~{se_end}",
        "sortName": "",
        "sortType": "",
        "isHLtitle": "false",
    }
    rows = []
    page = 1
    while page <= 400:  # 安全上限: 单段(≤4年)不可能超 12000 条
        payload["pageNum"] = str(page)
        j = post_with_retry(session, payload)
        got = j.get("announcements") or []
        rows.extend(got)
        if not j.get("hasMore") or not got:
            break
        page += 1
    return rows


def save_rows(conn, code, raw_rows):
    n = 0
    for item in raw_rows:
        ann_id = str(item.get("announcementId") or "")
        title = (item.get("announcementTitle") or "").strip()
        if not ann_id or not title:
            continue
        ts_ms = item.get("announcementTime")
        try:
            pub = dt.datetime.fromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        org_id = item.get("orgId") or ""
        url = (f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode={code}"
               f"&announcementId={ann_id}&orgId={org_id}&announcementTime={pub[:10]}")
        cat, impact = classify(title)
        cur = conn.execute(
            "INSERT OR IGNORE INTO announcements(code,name,title,publish_time,ann_id,url,category,impact) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (code, item.get("secName") or "", title, pub, ann_id, url, cat, impact))
        n += cur.rowcount
    conn.commit()
    return n


def date_chunks(start_str, end_str, years=4):
    """按 N 年切段, 降低单次查询页数"""
    start = dt.date.fromisoformat(start_str)
    end = dt.date.fromisoformat(end_str)
    chunks = []
    cur = start
    while cur < end:
        nxt = min(dt.date(cur.year + years, cur.month, cur.day), end)
        chunks.append((cur.isoformat(), nxt.isoformat()))
        cur = nxt + dt.timedelta(days=1)
    return chunks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 只(烟测)")
    ap.add_argument("--codes", type=str, default="", help="逗号分隔指定股票")
    args = ap.parse_args()

    today = dt.date.today().isoformat()
    conn = init_db()
    session = requests.Session()

    stock_list = pd.read_parquet(os.path.join(DATA_DIR, "stock_list.parquet"))
    codes = [str(c).zfill(6) for c in stock_list["stock_code"].tolist()]
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    if args.limit:
        codes = codes[: args.limit]

    org_map = get_orgid_map(session)
    done = {r[0] for r in conn.execute("SELECT code FROM crawl_progress WHERE status='done'")}
    todo = [c for c in codes if c not in done]
    print(f"total={len(codes)} done={len(codes) - len(todo)} todo={len(todo)} "
          f"range={START_DATE}~{today}", flush=True)

    t0 = time.time()
    for i, code in enumerate(todo, 1):
        org_id = org_map.get(code)
        if not org_id:
            conn.execute("INSERT OR REPLACE INTO crawl_progress VALUES(?,?,?,?,?)",
                         (code, "no_orgid", 0, "", dt.datetime.now().isoformat()))
            conn.commit()
            continue
        try:
            total_saved = 0
            for se_start, se_end in date_chunks(START_DATE, today):
                rows = fetch_stock_range(session, code, org_id, se_start, se_end)
                total_saved += save_rows(conn, code, rows)
            conn.execute("INSERT OR REPLACE INTO crawl_progress VALUES(?,?,?,?,?)",
                         (code, "done", total_saved, "", dt.datetime.now().isoformat()))
            conn.commit()
        except Exception as e:
            conn.execute("INSERT OR REPLACE INTO crawl_progress VALUES(?,?,?,?,?)",
                         (code, "error", 0, f"{type(e).__name__}: {e}",
                          dt.datetime.now().isoformat()))
            conn.commit()
            print(f"  [ERR] {code}: {type(e).__name__}: {e}", flush=True)
        if i % 25 == 0 or i == len(todo):
            elapsed = time.time() - t0
            rate = i / elapsed * 3600
            eta_h = (len(todo) - i) / max(rate, 1)
            n_ann = conn.execute("SELECT COUNT(*) FROM announcements").fetchone()[0]
            print(f"[{i}/{len(todo)}] {code} ann_total={n_ann} "
                  f"rate={rate:.0f}/h eta={eta_h:.1f}h", flush=True)

    n_ann = conn.execute("SELECT COUNT(*) FROM announcements").fetchone()[0]
    n_err = conn.execute("SELECT COUNT(*) FROM crawl_progress WHERE status='error'").fetchone()[0]
    print(f"FINISHED ann={n_ann} err_stocks={n_err}", flush=True)


if __name__ == "__main__":
    main()
