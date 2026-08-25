#!/bin/bash
# 通达信分钟线每日增量 — 运行入口 (交易日收盘后, 建议 15:30)
#   cron 示例: 30 15 * * 1-5  /path/to/astock-data-toolkit/minute/tdx_increment_run.sh
BASE="${ASTOCK_HOME:-$(cd "$(dirname "$0")/.." && pwd)}"
PYTHON="${PYTHON:-python3}"   # 可用环境变量指定 venv 里的解释器
HERE="$(cd "$(dirname "$0")" && pwd)"
LOG=$BASE/minute_data/tdx_increment.log
LOCK=/tmp/tdx_increment.lock

if [ -f $LOCK ]; then
    pid=$(cat $LOCK 2>/dev/null)
    if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
        echo "[$(date '+%F %T')] another instance running (pid=$pid), skip" >> $LOG
        exit 0
    fi
fi
echo $$ > $LOCK
trap "rm -f $LOCK" EXIT

echo "" >> $LOG
echo "======== tdx increment $(date '+%F %T') ========" >> $LOG
$PYTHON $HERE/tdx_increment.py >> $LOG 2>&1
code=$?
if [ $code -ne 0 ]; then
    echo "[$(date '+%F %T')] ⚠️ INCREMENT ALERT (exit=$code)" >> $LOG
fi
echo "======== done $(date '+%F %T') exit=$code ========" >> $LOG
exit $code
