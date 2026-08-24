#!/bin/bash
# fin 财务摘要 跨轮续跑 — 每日入口 (2026-08-23 新增)
#
# 背景: 中报/年报季单个窗口 3000~5000 只, 远超新浪单日配额(实测 790~840)。
#   旧逻辑"熔断→整窗下轮重来"在 窗口量>配额 时永不收敛, fin_last_ok 会永久卡死
#   (8-22 那轮即此: 窗口 1266 只熔断于 144, 游标停在 8-16 再没动过)。
#   现在 info_weekly_update.py 正常模式会冻结窗口 + 记断点, 单轮抓够 FIN_RUN_CAP
#   就优雅停; 本脚本每天接着上次的断点跑一轮, 分多天把洪峰吃完。
#
# 无欠账时 --fin-catchup 立即退出, 零请求零开销 → 平时挂着不影响任何东西。
#
# 部署: crontab
#   0 10 * * 1-5  /path/to/fin_catchup_run.sh
#   ⚠️ 周六不跑: 周更(0 2 * * 6)自己会跑一轮 fin, 同日再跑必撞配额。
#   ⚠️ 周日不跑: 周更 Step6 的 fin 段实际落在周日凌晨 3~4 点, 留一天间隔。
#   ⚠️ 一天只跑一轮, 不要图快改成一天多轮 —— 8-23 实测: 500 只 + 隔 2 小时 + 294 只
#      = 794 只照样被封, 限流按滚动时间窗算, **同日拆批不重置配额**。

BASE="${ASTOCK_HOME:-$(cd "$(dirname "$0")" && pwd)}"
PYTHON="${PYTHON:-python3}"   # 可用环境变量指定 venv 里的解释器
LOG=$BASE/fin_catchup.log
LOCK=/tmp/astock_fin_catchup.lock
WEEKLY_LOCK=/tmp/astock_weekly.lock

# ---- 周更正在跑就让路(它自己会跑 fin, 抢着跑只会一起撞配额 + 争数据库写锁) ----
if [ -f $WEEKLY_LOCK ]; then
    pid=$(cat $WEEKLY_LOCK 2>/dev/null)
    if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
        echo "[$(date '+%F %T')] weekly update running (pid=$pid), skip" >> $LOG
        exit 0
    fi
fi

# ---- 防自身并发 ----
if [ -f $LOCK ]; then
    pid=$(cat $LOCK 2>/dev/null)
    if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
        echo "[$(date '+%F %T')] another instance running (pid=$pid), skip" >> $LOG
        exit 0
    fi
fi
echo $$ > $LOCK
trap "rm -f $LOCK" EXIT

cd $BASE

echo "" >> $LOG
echo "======== fin catchup $(date '+%F %T') ========" >> $LOG
$PYTHON $BASE/info_weekly_update.py --sections fin --fin-catchup >> $LOG 2>&1
code=$?
if [ $code -ne 0 ]; then
    # 熔断(疑似封禁)也会走到这里 —— 断点已落库, 明天同一时间自动接着跑, 无需人工。
    # 但仍打 ALERT: 连续多天出现说明配额或节奏出了新问题, 要人看。
    echo "[$(date '+%F %T')] ⚠️ FIN CATCHUP ALERT (exit=$code) 断点已落库, 明日自动续" >> $LOG
fi
echo "======== done $(date '+%F %T') exit=$code ========" >> $LOG
exit $code
