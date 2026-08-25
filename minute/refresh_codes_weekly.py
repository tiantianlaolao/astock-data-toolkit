# -*- coding: utf-8 -*-
"""分钟线名单周刷: 用东财 clist_delay 全市场枚举, 把新上市股票补进 codes.csv

判新股三条件(缺一不可):
  1. 不在 codes.csv 里
  2. f26(上市日期)为数字且 >= CUTOFF —— 排除退市/停牌壳(它们也不在名单但是老代码)
  3. 沪深 A 前缀(0/3/6 开头) —— 名单历来不含北交所, 保持口径
f26='-' 的是已核准未上市新股, 跳过, 上市后自然会被捞到。
f26 在未来(还没上市)的同样跳过 —— 否则下载会留空 gz 占坑, skip-existing 永不重试。
新增走原子重写(tmp+mv)。--dry-run 只报告不写。
用法: python3 refresh_codes_weekly.py [--dry-run]

名单文件 = $ASTOCK_HOME/minute_data/codes.csv (单列 stock_code, 六位字符串)。
首次使用可自行生成一份初始名单, 之后由本脚本每周增量维护。
"""
import datetime as dt
import os
import sys
import time

import pandas as pd
import requests

BASE = os.environ.get("ASTOCK_HOME") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CODES_CSV = os.path.join(BASE, "minute_data", "codes.csv")
URL = "http://push2delay.eastmoney.com/api/qt/clist/get"
FS_HUSHEN = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
CUTOFF = 20260707  # ⚠️ 改成"你自己首批名单定稿日"往前留一个月; 早于此又不在名单的 = 退市壳


def enumerate_market():
    sess = requests.Session()
    rows = {}
    pn = 1
    while True:
        j = None
        for k in range(3):
            try:
                r = sess.get(URL, params={"pn": str(pn), "pz": "100", "po": "1", "np": "1",
                                          "fltt": "2", "invt": "2", "fid": "f12",
                                          "fs": FS_HUSHEN, "fields": "f12,f14,f26"},
                             timeout=10)
                if r.status_code == 200:
                    j = r.json()
                    break
            except Exception:
                time.sleep(1 + k)
        if j is None:
            raise RuntimeError(f"clist_delay 第 {pn} 页三次失败")
        diff = (j.get("data") or {}).get("diff") or []
        if not diff:
            break
        for s in diff:
            rows[s["f12"]] = (s.get("f14", ""), s.get("f26"))
        pn += 1
        time.sleep(0.2)
    total = (j.get("data") or {}).get("total")
    if total and len(rows) < int(total) * 0.95:
        raise RuntimeError(f"枚举对账失败: claimed={total} fetched={len(rows)}")
    return rows


def main():
    dry = "--dry-run" in sys.argv
    df = pd.read_csv(CODES_CSV, dtype={"stock_code": str})
    have = set(df["stock_code"].str.zfill(6))
    market = enumerate_market()
    print(f"[refresh] 枚举 {len(market)} 只, 名单 {len(have)} 只", flush=True)

    today = int(dt.date.today().strftime("%Y%m%d"))
    new = []
    for code, (name, f26) in sorted(market.items()):
        if code in have or not code.startswith(("0", "3", "6")):
            continue
        try:
            listed = int(f26)
        except (TypeError, ValueError):
            continue  # '-' = 未上市
        if CUTOFF <= listed <= today:
            new.append((code, name, listed))
        elif listed > today:
            print(f"[refresh] 跳过未上市: {code} {name} 上市 {listed}", flush=True)

    if not new:
        print("[refresh] 无新股, 名单不变", flush=True)
        return
    for code, name, listed in new:
        print(f"[refresh] 新股: {code} {name} 上市 {listed}", flush=True)
    if dry:
        print(f"[refresh] dry-run, 不写入 ({len(new)} 只)", flush=True)
        return

    add = pd.DataFrame([{"stock_code": c, "stock_name": n} for c, n, _ in new])
    out = pd.concat([df, add], ignore_index=True).sort_values("stock_code")
    tmp = CODES_CSV + ".tmp"
    out.to_csv(tmp, index=False)
    os.replace(tmp, CODES_CSV)
    print(f"[refresh] 已写入 {len(new)} 只新股, 名单 {len(have)} -> {len(out)}", flush=True)


if __name__ == "__main__":
    main()
