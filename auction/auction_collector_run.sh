#!/bin/bash
# 竞价采集驱动: auction_collector_run.sh open|close
#   cron 示例: 25 9 * * 1-5 (open) / 57 14 * * 1-5 (close)
BASE="${ASTOCK_HOME:-$(cd "$(dirname "$0")/.." && pwd)}"
PYTHON="${PYTHON:-python3}"   # 可用环境变量指定 venv 里的解释器
HERE="$(cd "$(dirname "$0")" && pwd)"
MODE=$1
mkdir -p $BASE/auction_data
LOG=$BASE/auction_data/auction_collector.log
LOCK=$BASE/auction_data/.lock_$MODE

exec 9>"$LOCK"
flock -n 9 || { echo "$(date '+%F %T') $MODE 已有实例在跑, 退出" >> $LOG; exit 0; }

echo "======== auction $MODE $(date '+%F %T') ========" >> $LOG
$PYTHON $HERE/auction_collector.py $MODE >> $LOG 2>&1
RC=$?
echo "======== done $MODE $(date '+%F %T') exit=$RC ========" >> $LOG
exit $RC
