#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
# Goal-2 comparison for the pipeline fix. Root cause (bisect1+2): the COLD
# query-view clone inheritance (query.View.Reopen's maps.Clone, consulted first
# in View.Get) serves auth a stale warm-time committed read -> MVCC abort cascade
# -> livelock on hot-key traffic. The fix is to REMOVE the cold clone so auth
# re-reads current committed state (identical to serial). This script checks the
# binding goal "pipeline never slower than serial" and decides whether the
# write-cache-resolution inheritance (cold_off keeps it) earns its complexity vs
# removing both (no_inherit):
#
#   serial     : -pipeline OFF (baseline correctness+speed reference)
#   cold_off   : -pipeline + EVM_PIPE_NO_COLD_INHERIT=1  (cold clone removed, write inherit kept)
#   no_inherit : -pipeline + EVM_PIPE_NO_COLD_INHERIT=1 EVM_PIPE_NO_WRITE_INHERIT=1 (both removed)
#
# across bs in {128,1024} x {historic,synthetic}. GATE: every cell WINDOW/WINDOW,
# rb=0; cold_off/no_inherit tput >= serial tput at every cell.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

SIZES="${SIZES:-128 1024}"
DATASETS="${DATASETS:-historic synthetic}"
CELL_TIMEOUT="${CELL_TIMEOUT:-12m}"

LOG="$HOME/compare_fix.log"
: > "$LOG"
echo "=========== COMPARE_FIX window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local ds="$1" bs="$2" label="$3" pflag="$4" extra="$5"
  echo | tee -a "$LOG"
  echo "########## $ds bs=$bs $label $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/compare_${ds}_bs${bs}_${label}.out"
  # shellcheck disable=SC2086
  timeout "$CELL_TIMEOUT" env $extra go test -timeout 18m -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml \
    -dataset "$ds" -max-batch-size "$bs" $pflag >"$OUT" 2>&1
  local rc=$?

  local line tput com tot rb
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')
  echo "RESULT $ds bs=$bs $label rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA}" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

for ds in $DATASETS; do
  for bs in $SIZES; do
    run_cell "$ds" "$bs" "serial"     ""          ""
    run_cell "$ds" "$bs" "cold_off"   "-pipeline" "EVM_PIPE_NO_COLD_INHERIT=1"
    run_cell "$ds" "$bs" "no_inherit" "-pipeline" "EVM_PIPE_NO_COLD_INHERIT=1 EVM_PIPE_NO_WRITE_INHERIT=1"
  done
done

echo | tee -a "$LOG"
echo "=========== COMPARE_FIX SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "COMPARE_FIX_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
