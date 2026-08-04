#!/bin/bash
# Second bisect: the first run localized the historic bs=128 pipeline livelock
# to the read-cost INHERITANCE layers (no_inherit cell: 4000/4000, rb=0, 117 t/s
# vs baseline 896/4000, rb=2713, 15 t/s). Now isolate WHICH inheritance is unsafe
# by turning each off INDIVIDUALLY (the other stays ON):
#
#   cold_off   : EVM_PIPE_NO_COLD_INHERIT=1  (write-cache-resolution inherit ON)
#                -> query.View.Reopen starts auth with an EMPTY view cache
#   write_off  : EVM_PIPE_NO_WRITE_INHERIT=1 (cold query-view clone ON)
#                -> warm views stop capturing; auth inherits no write-cache map
#
# PASS (clean): commits WINDOW/WINDOW, rb=0, high tput -> THAT layer was the culprit.
# FAIL (livelock): low tput, climbing rb -> the OTHER layer is (also) unsafe.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-4000}"

BS="${BS:-128}"
CELL_TIMEOUT="${CELL_TIMEOUT:-10m}"

LOG="$HOME/bisect_pipeline2.log"
: > "$LOG"
echo "=========== BISECT2 bs=$BS window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local label="$1" extra="$2"
  echo | tee -a "$LOG"
  echo "########## historic bs=$BS pipeline label='$label' env='$extra' $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/bisect2_${label}.out"
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

run_cell "cold_off"  "EVM_PIPE_NO_COLD_INHERIT=1"
run_cell "write_off" "EVM_PIPE_NO_WRITE_INHERIT=1"

echo | tee -a "$LOG"
echo "=========== BISECT2 SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "BISECT2_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
