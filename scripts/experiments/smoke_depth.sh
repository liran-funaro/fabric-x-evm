#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
# Fast depth-independence smoke for the warm(N+1)‖auth(N) pipeline after the
# 87080f7 correctness fix (remove stale warmWrites snapshot; View.Reopen begins
# a FRESH committed view) + the uncommitted inherited-write-cache read-cost fix.
#
# Targets ONLY the historically LIVELOCKING points (historic hot-key USDC,
# small batches bs<=256), pipeline ON, with a bounded window + per-cell timeout
# so a still-livelocking cell FAILS fast instead of hanging on the 4h timeout.
#
# PASS (depth-independent): each cell commits WINDOW/WINDOW, 0 rolled-back
# batches. FAIL (still depth-1 bound): low throughput, climbing rb, timeout.
# Serial baseline included per size for a same-window pairwise sanity check.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

SIZES="${SIZES:-128 256}"
CELL_TIMEOUT="${CELL_TIMEOUT:-15m}"

LOG="$HOME/smoke_depth.log"
: > "$LOG"
echo "=========== DEPTH SMOKE window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local bs="$1" pflag="$2" label="$3"
  echo | tee -a "$LOG"
  echo "########## historic bs=$bs pipeline='$pflag' $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/smoke_${label}.out"
  # shellcheck disable=SC2086
  timeout "$CELL_TIMEOUT" go test -timeout 20m -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml \
    -dataset historic -max-batch-size "$bs" $pflag >"$OUT" 2>&1
  local rc=$?

  local line tput com tot rb
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')
  echo "RESULT historic $bs ${pflag:-serial} rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA}" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

for bs in $SIZES; do
  run_cell "$bs" ""          "historic_bs${bs}_off"
  run_cell "$bs" "-pipeline" "historic_bs${bs}_on"
done

echo | tee -a "$LOG"
echo "=========== SMOKE SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SMOKE_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
