#!/bin/bash
# 内存看门狗: 监控可用内存, 低于阈值时 SIGSTOP 目标进程, 恢复后 SIGCONT
# 用法: ./mem_watchdog.sh <pid_file> <threshold_mb>
# 举例: ./mem_watchdog.sh ./backfill.pid 1500

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="${1:-$SCRIPT_DIR/backfill.pid}"
THRESHOLD_MB="${2:-1500}"
CHECK_INTERVAL=30
LOG="$SCRIPT_DIR/watchdog.log"

echo "[$(date '+%F %T')] watchdog started, pid_file=$PID_FILE, threshold=${THRESHOLD_MB}MB, interval=${CHECK_INTERVAL}s" >> "$LOG"

STOPPED=0
while true; do
    if [ ! -f "$PID_FILE" ]; then
        echo "[$(date '+%F %T')] pid_file not found, exiting" >> "$LOG"
        exit 0
    fi
    PID=$(cat "$PID_FILE" 2>/dev/null)
    if [ -z "$PID" ] || ! kill -0 "$PID" 2>/dev/null; then
        echo "[$(date '+%F %T')] target pid $PID not alive, exiting" >> "$LOG"
        exit 0
    fi

    AVAIL=$(awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo)

    if [ "$AVAIL" -lt "$THRESHOLD_MB" ] && [ "$STOPPED" -eq 0 ]; then
        kill -STOP "$PID" 2>/dev/null
        STOPPED=1
        echo "[$(date '+%F %T')] AVAIL=${AVAIL}MB < ${THRESHOLD_MB}MB, STOP pid $PID" >> "$LOG"
    elif [ "$AVAIL" -gt $((THRESHOLD_MB + 500)) ] && [ "$STOPPED" -eq 1 ]; then
        kill -CONT "$PID" 2>/dev/null
        STOPPED=0
        echo "[$(date '+%F %T')] AVAIL=${AVAIL}MB > $((THRESHOLD_MB + 500))MB, CONT pid $PID" >> "$LOG"
    fi

    sleep "$CHECK_INTERVAL"
done
