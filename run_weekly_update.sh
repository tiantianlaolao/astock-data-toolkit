#!/bin/bash
# ===================================================================
# A股数据每周增量更新脚本
# ===================================================================
# 与 run_full_download.sh 不同: 本脚本只增量追加, 绝不删除已有数据
# 所有 step 都用 --incremental 模式
#
# 部署方式: crontab
#   每周六 02:00 (全周市场都收盘, 跑 3-4 小时不影响任何人)
#   0 2 * * 6  /path/to/astock-data-toolkit/run_weekly_update.sh
#
# ⚠️ Step 3 (百度估值) 逐只调接口, 全量 5321 只约 2-4 小时, 其它 step 各 ~10-30 分钟
# ===================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PYTHON:-python3}"   # 可用环境变量指定 venv 里的解释器
DATA_DIR=$SCRIPT_DIR/astock_data
LOG=$SCRIPT_DIR/weekly_update.log
LOCK=/tmp/astock_weekly.lock

cd $SCRIPT_DIR

# ---- 防并发 ----
if [ -f $LOCK ]; then
    pid=$(cat $LOCK 2>/dev/null)
    if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
        echo "[$(date '+%F %H:%M:%S')] Another instance running (pid=$pid), skip" >> $LOG
        exit 0
    fi
fi
echo $$ > $LOCK
trap "rm -f $LOCK" EXIT

echo "" >> $LOG
echo "========================================" >> $LOG
echo "Weekly update started: $(date)" >> $LOG
echo "========================================" >> $LOG

# ---- 安全保护: 备份 progress.json ----
if [ -f $DATA_DIR/progress.json ]; then
    cp $DATA_DIR/progress.json $DATA_DIR/progress.json.bak
    echo "[$(date '+%H:%M:%S')] Backed up progress.json" >> $LOG
fi

# ---- 安全保护: 记录跑前的数据文件大小 ----
echo "" >> $LOG
echo "Pre-update data sizes:" >> $LOG
ls -lh $DATA_DIR/*.parquet 2>&1 >> $LOG

# ---- 跑 step 1~5 增量 ----
# 不跑 step 0 (股票列表), 每周变化很小, 偶尔手动跑即可
overall_failed=0
for step in 1 2 3 4 5; do
    echo "" >> $LOG
    echo "[$(date '+%H:%M:%S')] ===== Step $step (incremental) =====" >> $LOG

    $PYTHON download_astock_data.py --step $step --incremental >> $LOG 2>&1
    exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "[$(date '+%H:%M:%S')] Step $step FAILED (code=$exit_code)" >> $LOG
        overall_failed=$((overall_failed + 1))
        # 不退出, 继续下一 step
    else
        echo "[$(date '+%H:%M:%S')] Step $step done" >> $LOG
    fi
    sleep 5
done

# ---- Step 6: 信息模块增量 (公告/财务摘要/增减持 -> info_archive.db) ----
# 与 parquet 链路和回测服务内存完全解耦, 失败不影响 step 1~5 数据
echo "" >> $LOG
echo "[$(date '+%H:%M:%S')] ===== Step 6 (info archive incremental) =====" >> $LOG
$PYTHON info_weekly_update.py >> $LOG 2>&1
if [ $? -ne 0 ]; then
    echo "[$(date '+%H:%M:%S')] Step 6 FAILED" >> $LOG
    overall_failed=$((overall_failed + 1))
else
    echo "[$(date '+%H:%M:%S')] Step 6 done" >> $LOG
fi

# ---- 总结 ----
echo "" >> $LOG
echo "Post-update data sizes:" >> $LOG
ls -lh $DATA_DIR/*.parquet 2>&1 >> $LOG

echo "" >> $LOG
echo "========================================" >> $LOG
if [ $overall_failed -eq 0 ]; then
    echo "Weekly update completed: $(date) [ALL OK]" >> $LOG
else
    echo "Weekly update completed: $(date) [$overall_failed step(s) failed]" >> $LOG
fi
echo "========================================" >> $LOG

# ---- 数据更新后钩子: 若有下游服务依赖这些数据, 用环境变量 POST_UPDATE_CMD 指定 (例: "pm2 restart my-service") ----
if [ -n "$POST_UPDATE_CMD" ]; then
    $POST_UPDATE_CMD >> $LOG 2>&1
    echo "[$(date '+%H:%M:%S')] post-update cmd done: $POST_UPDATE_CMD" >> $LOG
    sleep 20
fi

exit $overall_failed
