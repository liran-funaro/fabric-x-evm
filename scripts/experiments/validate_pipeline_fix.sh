#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
# Validate the pipelined-executor auth-reopen fix across BOTH datasets.
#
# The pipelined loop (warm(N+1) || auth(N)) must be IDENTICAL IN EFFECT to the
# serial executor (design invariant). Before the reopen fix, the auth pass
# reused warm's now-stale pinned query-service view and recorded stale MVCC
# read-versions on hot keys, producing a committer abort cascade / livelock on
# the historic trace. The fix: auth opens a FRESH view of latest committed
# state (sharing warm's read cache) so recorded read-versions match committed.
#
# GATE (historic, pipeline ON): 20000/20000 EVM txs committed, 0 rolled-back
# batches, and pipelined throughput >= serial throughput. Synthetic is the
# conflict-free regression check (pipeline was already a win there).
#
# Matrix at BS=1024 (representative point where synthetic won ~1.16-1.53x and
# historic previously livelocked). Fresh stack per cell.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-20000}"
BS="${BS:-1024}"

LOG="$HOME/validate_pipeline.log"
: > "$LOG"
echo "=========== PIPELINE-FIX VALIDATION bs=$BS window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

# Matrix cells: "<label> <dataset> <pipeline-flag>". Order: historic first (the
# gate), serial baseline before pipeline so the pairwise compare is adjacent.
run_cell() {
  local label="$1" ds="$2" pflag="$3"
  echo | tee -a "$LOG"
  echo "########## CELL=$label dataset=$ds pipeline='$pflag'  $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1
  echo "[stack up $label] $(date +%H:%M:%S)" | tee -a "$LOG"

  local OUT="$HOME/validate_${label}.out"
  # shellcheck disable=SC2086
  go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml \
    -dataset "$ds" -max-batch-size "$BS" $pflag >"$OUT" 2>&1
  local rc=$?

  local summary
  summary=$(grep -E 'Replay complete:|TestReplayJSONDataset: [0-9]|Stalled: no commit progress' "$OUT" | tail -3)
  echo "[done $label rc=$rc] $(date +%H:%M:%S)" | tee -a "$LOG"
  echo "$summary" | tee -a "$LOG"
  local ts rb
  ts=$(grep -oE 'TestReplayJSONDataset: [0-9]+ EVM tx/s \([0-9]+/[0-9]+ committed' "$OUT" | tail -1)
  rb=$(grep -oE '[0-9]+ rolled-back batches' "$OUT" | tail -1)
  echo "RESULT $label rc=$rc | $ts | $rb" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

run_cell "historic_serial"   historic  ""
run_cell "historic_pipeline" historic  "-pipeline"
run_cell "synthetic_serial"  synthetic ""
run_cell "synthetic_pipeline" synthetic "-pipeline"

echo | tee -a "$LOG"
echo "=========== VALIDATION SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "VALIDATE_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
