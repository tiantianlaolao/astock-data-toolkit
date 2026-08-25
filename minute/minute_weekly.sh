#!/bin/bash
# 分钟线周更 (每周一次) — 与 run_weekly_update.sh(日线/公告周更)完全独立
#   ① codes.csv 新股刷新 (东财枚举+f26上市日期判新)
#   ② 新股历史首刷 (tdx_download* 自带 skip-existing, 无新股时秒过)
#   ③ parquet 重建 (gz权威源 -> 按年parquet + 全量校验, 供分钟回测)
# ⚠️ 排期要点: 必须排在日线/公告周更之后 —— parquet 的 D8 交叉校验拿日线库当基准,
# 基准没更新完就重建, D8 会成片报"不通过"。
#   cron 示例: 0 9 * * 6  /path/to/astock-data-toolkit/minute/minute_weekly.sh
BASE="${ASTOCK_HOME:-$(cd "$(dirname "$0")/.." && pwd)}"
PYTHON="${PYTHON:-python3}"   # 可用环境变量指定 venv 里的解释器
HERE="$(cd "$(dirname "$0")" && pwd)"
LOG=$BASE/minute_data/minute_weekly.log
LOCK=/tmp/minute_weekly.lock

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
echo "======== minute weekly $(date '+%F %T') ========" >> $LOG
fails=""

step() {  # $1=名字 其余=命令
    name=$1; shift
    echo "-------- $name $(date '+%F %T') --------" >> $LOG
    "$@" >> $LOG 2>&1
    code=$?
    if [ $code -ne 0 ]; then
        echo "[$(date '+%F %T')] ⚠️ $name FAILED (exit=$code)" >> $LOG
        fails="$fails $name"
    fi
    return $code
}

# ① 新股刷新 (失败则跳过②, 但③照做 — 老股票的 parquet 照常更新)
if step refresh $PYTHON $HERE/refresh_codes_weekly.py; then
    # ② 新股首刷 (skip-existing; 日线秒过, 分钟给足余量但设硬停防失控)
    step dl_daily $PYTHON $HERE/tdx_download_daily.py
    step dl_5min  $PYTHON $HERE/tdx_download.py 5min --deadline 13:00
    step dl_1min  $PYTHON $HERE/tdx_download.py 1min --deadline 13:00
fi

# ③ parquet 重建 (两频率)
step build_5min $PYTHON $HERE/build_minute_parquet.py 5min
step build_1min $PYTHON $HERE/build_minute_parquet.py 1min

if [ -n "$fails" ]; then
    echo "[$(date '+%F %T')] ⚠️ MINUTE_WEEKLY ALERT: 失败步骤:$fails" >> $LOG
    echo "======== done $(date '+%F %T') WITH FAILURES ========" >> $LOG
    exit 1
fi
echo "======== done $(date '+%F %T') ALL OK ========" >> $LOG
exit 0
