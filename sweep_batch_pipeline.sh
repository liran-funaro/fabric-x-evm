#!/bin/bash
# Post-fix batch-size sweep for the warm(N+1)‖auth(N) pipeline, BOTH datasets,
# serial vs pipeline, to regenerate report/results.json's batch_scan after the
# auth read-layering fix (reopen fresh committed view + frozen per-batch warm-
# write snapshot; see core.VersionedCache.SnapshotEntries and
# cachedView.warmWrites). Supersedes the pre-fix "historic livelocks" sweep.
#
# GATE: every cell 20000/20000 committed, 0 rolled-back batches. Emits one
# machine-parseable RESULT line per cell:
#   RESULT <ds> <bs> <pflag> rc=<rc> tput=<n> com=<c>/<t> rb=<r>
# Fresh full stack per cell.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-20000}"

SIZES="${SIZES:-128 256 512 1024 2048 4096}"
DATASETS="${DATASETS:-synthetic historic}"

LOG="$HOME/sweep_batch.log"
: > "$LOG"
echo "=========== BATCH-SWEEP (post-fix) window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local ds="$1" bs="$2" pflag="$3" label="$4"
  echo | tee -a "$LOG"
  echo "########## ds=$ds bs=$bs pipeline='$pflag' $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/sweep_${label}.out"
  # shellcheck disable=SC2086
  go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
    -dataset "$ds" -max-batch-size "$bs" $pflag >"$OUT" 2>&1
  local rc=$?

  # Parse: "... N EVM tx/s ... C/T committed ... R rolled-back batches"
  local line tput com tot rb
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')
  echo "RESULT $ds $bs ${pflag:-serial} rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA}" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

for ds in $DATASETS; do
  for bs in $SIZES; do
    run_cell "$ds" "$bs" ""          "${ds}_bs${bs}_off"
    run_cell "$ds" "$bs" "-pipeline" "${ds}_bs${bs}_on"
  done
done

echo | tee -a "$LOG"
echo "=========== SWEEP SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SWEEP_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
