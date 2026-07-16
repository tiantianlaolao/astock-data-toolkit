#!/bin/bash
# 运行一个 backfill 表, 启动看门狗自动保护
#   ./run_backfill.sh <table> [--resume] [--limit N]
# 举例:
#   ./run_backfill.sh ohlcv
#   ./run_backfill.sh valuation --resume

set -e

TABLE="${1:?usage: $0 <table> [extra args]}"
shift
EXTRA_ARGS="$@"

BACKFILL_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$BACKFILL_DIR/backfill_15y.py"
VENV_PY="${PYTHON:-python3}"   # 可用环境变量指定 venv 里的解释器
LOG_FILE="${BACKFILL_DIR}/run_${TABLE}_$(date +%Y%m%d_%H%M%S).log"
PID_FILE="${BACKFILL_DIR}/backfill_${TABLE}.pid"
WATCHDOG_SCRIPT="${BACKFILL_DIR}/mem_watchdog.sh"
WATCHDOG_PID_FILE="${BACKFILL_DIR}/watchdog_${TABLE}.pid"

mkdir -p "$BACKFILL_DIR"

# 启动 backfill 进程
$VENV_PY "$SCRIPT" --table "$TABLE" $EXTRA_ARGS > "$LOG_FILE" 2>&1 &
BACKFILL_PID=$!
echo "$BACKFILL_PID" > "$PID_FILE"
echo "[$(date)] backfill($TABLE) pid=$BACKFILL_PID log=$LOG_FILE"

# 启动看门狗 (阈值 1500 MB)
bash "$WATCHDOG_SCRIPT" "$PID_FILE" 1500 &
WATCHDOG_PID=$!
echo "$WATCHDOG_PID" > "$WATCHDOG_PID_FILE"
echo "[$(date)] watchdog pid=$WATCHDOG_PID"

echo ""
echo "tail -f $LOG_FILE   # 看进度"
echo "cat ${BACKFILL_DIR}/heartbeat.json   # 实时状态"
