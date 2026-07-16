#!/bin/bash
# 15 年 backfill 总调度器 — 串行跑 6 个阶段, 独立 session, 不受 ssh 退出影响
#
# 用法 (必须 setsid 启动, 脱离终端):
#   setsid ./orchestrator.sh > ./orchestrator.log 2>&1 < /dev/null &
#
# 状态查看:
#   cat master_status.json
#   tail -f orchestrator.log

BACKFILL_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_DIR="$(dirname "$BACKFILL_DIR")"
PY="${PYTHON:-python3}"   # 可用环境变量指定 venv 里的解释器
STATUS="$BACKFILL_DIR/master_status.json"

mkdir -p "$BACKFILL_DIR"

update_status() {
    local phase="$1"; local state="$2"; local msg="${3:-}"
    cat > "$STATUS" <<EOF
{
  "phase": "$phase",
  "state": "$state",
  "msg": "$msg",
  "ts": "$(date -Iseconds)",
  "pid": $$
}
EOF
}

run_phase() {
    local name="$1"; shift
    local start_ts=$(date +%s)
    echo ""
    echo "===================================================="
    echo "PHASE START: $name  @  $(date)"
    echo "===================================================="
    update_status "$name" "running" ""
    "$@"
    local rc=$?
    local elapsed=$(( $(date +%s) - start_ts ))
    if [ $rc -ne 0 ]; then
        update_status "$name" "failed" "rc=$rc elapsed=${elapsed}s"
        echo "!!! PHASE FAILED: $name (rc=$rc, elapsed ${elapsed}s)"
        return $rc
    fi
    update_status "$name" "done" "elapsed=${elapsed}s"
    echo "### PHASE DONE: $name (elapsed ${elapsed}s)"
    return 0
}

cd "$SCRIPT_DIR"

echo "=== 15y backfill orchestrator 启动 @ $(date) ==="
echo "  PID: $$"
echo "  status file: $STATUS"
update_status "init" "starting" "pid=$$"

# ============================================================
# Phase 1: cninfo 全市场重拉 (扩到 1991+)
# ============================================================
run_phase "cninfo_refetch" $PY "$BACKFILL_DIR/refetch_cninfo_full.py" || {
    echo "cninfo refetch 失败, 终止"
    exit 1
}

# 验证新 cninfo 比旧的覆盖更早
NEW_CNINFO="$SCRIPT_DIR/cninfo_share_change_full_15y.parquet"
OLD_CNINFO="$SCRIPT_DIR/cninfo_share_change_full.parquet"
if [ -f "$NEW_CNINFO" ]; then
    $PY -c "
import pandas as pd
new = pd.read_parquet('$NEW_CNINFO')
old = pd.read_parquet('$OLD_CNINFO')
new['change_date'] = pd.to_datetime(new['change_date'])
old['change_date'] = pd.to_datetime(old['change_date'])
print(f'new: {len(new)} 行 最早 {new[\"change_date\"].min()} 最晚 {new[\"change_date\"].max()}')
print(f'old: {len(old)} 行 最早 {old[\"change_date\"].min()} 最晚 {old[\"change_date\"].max()}')
assert new['change_date'].min() < old['change_date'].min(), 'new 没比 old 更早'
assert len(new) > len(old) * 0.9, 'new 行数异常'
print('✓ new cninfo 比 old 覆盖更早')
" || { echo "cninfo 验证失败"; exit 1; }

    # 原子替换
    cp "$OLD_CNINFO" "${OLD_CNINFO}.bak_before_15y_swap"
    mv "$NEW_CNINFO" "$OLD_CNINFO"
    echo "✓ cninfo 替换完成: $OLD_CNINFO"
fi

# ============================================================
# Phase 2: OHLCV backfill
# ============================================================
run_phase "ohlcv" $PY "$BACKFILL_DIR/backfill_15y.py" --table ohlcv --resume || {
    echo "ohlcv 失败, 仍继续后面"
}

# ============================================================
# Phase 3: dividend backfill
# ============================================================
run_phase "dividend" $PY "$BACKFILL_DIR/backfill_15y.py" --table dividend --resume || {
    echo "dividend 失败, 仍继续"
}

# ============================================================
# Phase 4: financial backfill
# ============================================================
run_phase "financial" $PY "$BACKFILL_DIR/backfill_15y.py" --table financial --resume || {
    echo "financial 失败, 仍继续"
}

# ============================================================
# Phase 5: valuation backfill (依赖 cninfo + dividend)
# ============================================================
run_phase "valuation" $PY "$BACKFILL_DIR/backfill_15y.py" --table valuation --resume || {
    echo "valuation 失败"
}

# ============================================================
# Phase 6: merge dry-run (不实际合并, 等用户确认)
# ============================================================
run_phase "merge_dryrun" $PY "$BACKFILL_DIR/merge_backfill.py" --dry-run || {
    echo "merge dry-run 失败"
}

update_status "all_done" "success" "请人工跑 merge_backfill.py --commit"
echo ""
echo "===================================================="
echo "=== orchestrator 全部阶段完成 @ $(date) ==="
echo "===================================================="
echo ""
echo "下一步 (人工确认后跑):"
echo "  $PY $BACKFILL_DIR/merge_backfill.py --commit"
