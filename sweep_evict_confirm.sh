#!/bin/bash
# Confirmation sweep for fix B (eviction-hold depth), after the first sweep found
# hold>=16 gives a CLEAN WIN at historic bs=128 (8000/8000, rb=0, 1.44x serial)
# while h1/h2/h8 livelock and h4 was a partial (7808/8000). That non-monotonicity
# (h4 > h8 in correctness despite a shorter hold) is a BISTABLE transition zone:
# near the threshold the outcome is timing-dependent, safely above it it is clean.
# h16 and h32 both landed clean, but h16 has only ONE clean sample and h8
# livelocked right below it -- so before trusting hold=16/32 as a shippable
# default this sweep nails down THREE things:
#
#   1. RELIABILITY  -- bs=128 @ h16 re-run 3x: does the clean result reproduce,
#                      or was it a lucky landing at the edge of the zone?
#   2. MARGIN       -- bs=128 @ h24: a hold above 16 -- is there a stable margin,
#                      and does bs=128 stay clean deeper in?
#   3. COVERAGE     -- bs=256 & bs=512 @ h16 (intermediate sizes must not livelock
#                      at the shippable hold), and bs=1024 @ h16 & h32 (the
#                      shippable hold must keep the large-batch case a win, not
#                      just h8 which the first sweep already showed clean).
#
# GATE: all three bs=128 h16 re-runs 8000/8000 rb=0 tput>=serial; h24 clean;
# bs=256/512 @ h16 clean + >= their serial; bs=1024 @ h16/h32 clean + a win.
# If any bs=128 h16 re-run livelocks -> 16 is near-threshold, not shippable ->
# probe deeper for a stable default (or the fixed-hold approach is too fragile
# and a visibility-gated eviction is needed instead). Report honestly either way.
#
#   serial : -pipeline OFF
#   h<N>   : -pipeline + EVM_PIPE_EVICT_HOLD_DEPTH=<N>

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

CELL_TIMEOUT="${CELL_TIMEOUT:-12m}"

LOG="$HOME/sweep_evict_confirm.log"
: > "$LOG"
echo "=========== SWEEP_EVICT_CONFIRM window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local bs="$1" label="$2" pflag="$3" extra="$4"
  echo | tee -a "$LOG"
  echo "########## historic bs=$bs $label $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/sweep_confirm_bs${bs}_${label}.out"
  # shellcheck disable=SC2086
  timeout "$CELL_TIMEOUT" env $extra go test -timeout 18m -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
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

# 1. RELIABILITY: bs=128 @ h16 x3 (plus serial reference).
run_cell 128 "serial"   ""         ""
run_cell 128 "h16_r1"   "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=16"
run_cell 128 "h16_r2"   "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=16"
run_cell 128 "h16_r3"   "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=16"
# 2. MARGIN: bs=128 @ h24.
run_cell 128 "h24"      "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=24"

# 3a. COVERAGE: intermediate sizes @ h16 (with serial reference each).
run_cell 256 "serial"   ""         ""
run_cell 256 "h16"      "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=16"
run_cell 512 "serial"   ""         ""
run_cell 512 "h16"      "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=16"
# 3b. COVERAGE: bs=1024 @ the shippable hold (serial reference + h16 + h32).
run_cell 1024 "serial"  ""         ""
run_cell 1024 "h16"     "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=16"
run_cell 1024 "h32"     "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=32"

echo | tee -a "$LOG"
echo "=========== SWEEP_EVICT_CONFIRM SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SWEEP_EVICT_CONFIRM_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
