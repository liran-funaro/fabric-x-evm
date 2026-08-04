#!/bin/bash
# Bisect the pipeline auth-read-cost layers at the historically LIVELOCKING
# point (historic hot-key USDC, bs=128, pipeline ON). Analysis kept concluding
# the current code is correct while the smoke shows it still livelocks at
# bs<=256, so we localize the culprit EMPIRICALLY by toggling one read-cost
# layer off at a time (env vars only DISABLE a layer; default = current code):
#
#   baseline    : all layers ON (current code)          -> expect livelock
#   no_inherit  : EVM_PIPE_NO_COLD_INHERIT + NO_WRITE    -> KEY cell
#   no_defer    : EVM_PIPE_NO_DEFER                      -> deferred-eviction hold off
#   all_off     : all three off (naive fresh-reopen)     -> pipeline floor
#
# PASS (clean): cell commits WINDOW/WINDOW, rb=0, high tput.
# FAIL (livelock): low tput, climbing rb, timeout.
# Short window + per-cell timeout so a livelocking cell FAILS FAST.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-4000}"

BS="${BS:-128}"
CELL_TIMEOUT="${CELL_TIMEOUT:-10m}"

LOG="$HOME/bisect_pipeline.log"
: > "$LOG"
echo "=========== BISECT bs=$BS window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local label="$1" extra="$2"
  echo | tee -a "$LOG"
  echo "########## historic bs=$BS pipeline label='$label' env='$extra' $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/bisect_${label}.out"
  # shellcheck disable=SC2086
  timeout "$CELL_TIMEOUT" env $extra go test -timeout 15m -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
    -dataset historic -max-batch-size "$BS" -pipeline >"$OUT" 2>&1
  local rc=$?

  local line tput com tot rb
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')
  echo "RESULT $label rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA}" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

run_cell "baseline"   ""
run_cell "no_inherit" "EVM_PIPE_NO_COLD_INHERIT=1 EVM_PIPE_NO_WRITE_INHERIT=1"
run_cell "no_defer"   "EVM_PIPE_NO_DEFER=1"
run_cell "all_off"    "EVM_PIPE_NO_COLD_INHERIT=1 EVM_PIPE_NO_WRITE_INHERIT=1 EVM_PIPE_NO_DEFER=1"

echo | tee -a "$LOG"
echo "=========== BISECT SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "BISECT_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
