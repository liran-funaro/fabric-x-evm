#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
# Nil-view COVERAGE sweep across batch sizes, gated on sweep_nilview.sh having
# shown pipe_nilview clean+reproducible at the hard bs=128 regime. Confirms the
# nil-view pipeline stays correct AND >= serial at the larger batch sizes too
# (256/512/1024), so making nil-view the default is safe at every size, not just
# the one stress point. Default pipeline machinery; EVM_QS_NIL_VIEW is the only
# changed variable.
#
# GATE per size: pipe_nilview 8000/8000 committed, rb=0, tput >= serial_nilview.
#
#   serial_nilview : -pipeline OFF, EVM_QS_NIL_VIEW=1
#   pipe_nilview   : -pipeline,     EVM_QS_NIL_VIEW=1

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

CELL_TIMEOUT="${CELL_TIMEOUT:-12m}"

LOG="$HOME/sweep_nilview_coverage.log"
: > "$LOG"
echo "=========== SWEEP_NILVIEW_COVERAGE window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local bs="$1" label="$2" pflag="$3" extra="$4"
  echo | tee -a "$LOG"
  echo "########## historic bs=$bs $label $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/sweep_nvcov_bs${bs}_${label}.out"
  # shellcheck disable=SC2086
  timeout "$CELL_TIMEOUT" env $extra go test -timeout 18m -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml \
    -dataset historic -max-batch-size "$bs" $pflag >"$OUT" 2>&1
  local rc=$?

  local line tput com tot rb
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')
  echo "RESULT historic bs=$bs $label rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA}" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

for bs in 256 512 1024; do
  run_cell "$bs" "serial_nilview" ""          "EVM_QS_NIL_VIEW=1"
  run_cell "$bs" "pipe_nilview"   "-pipeline" "EVM_QS_NIL_VIEW=1"
done

echo | tee -a "$LOG"
echo "=========== SWEEP_NILVIEW_COVERAGE SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SWEEP_NILVIEW_COVERAGE_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
