#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
RESULTS_DIR="${RESULTS_DIR:-${EVM_PERF_DATA:-$HOME/workspace/evm-perf-data}/results}"; mkdir -p "$RESULTS_DIR"
# Eviction-hold-depth sweep (fix B: hold committed writes N boundaries).
#
# The depth sweep (sweep_depth.sh) disproved fix A (selective clone
# invalidation): at historic bs=128 it fails to fix the livelock at every
# invalidation depth 0-8 (~1000/8000 committed, rb~2700), and at bs=1024 the
# plain full clone (D=0) already beats serial while invalidation depths >=4 fall
# BELOW serial. So the clone is the wrong lever -- it is downstream of the stale
# read. The stale read is born UPSTREAM, in warm's cold-fetch: the executor
# evicts a committed batch's writes on its commit notification, but the query
# service reflects that commit only after a lag. At small batch sizes the
# boundaries are fast (~37ms) and the eviction outruns visibility, so a later
# warm pass cold-fetches a just-committed hot key from the query view at its
# STALE pre-commit version -> the pipelined auth clone carries it forward ->
# MVCC abort cascade -> livelock. At bs=1024 the boundaries are slow (~180ms),
# the query service catches up within one boundary, and the existing one-boundary
# hold suffices (rb=0, 1.15x serial).
#
# Fix B holds each committed batch's writes in the write cache for
# EVM_PIPE_EVICT_HOLD_DEPTH boundaries (default 1 = today's behavior) so warm
# serves the hot key from the cache (fresh: a committed batch's spec version ==
# its committed version) instead of cold-fetching stale, until the hold elapses
# and the query view is guaranteed to reflect the commit. Selective invalidation
# is left OFF (EVM_PIPE_INVALIDATE_DEPTH unset => 0, full clone).
#
# GATE: smallest EVM_PIPE_EVICT_HOLD_DEPTH at bs=128 with com==WINDOW/WINDOW &&
# rb=0 && no timeout -- that depth measures the visibility lag in boundaries --
# and at that depth tput >= serial. bs=1024 must STAY correct + a win at the
# larger holds (the hold must not regress the large-batch case). If no hold <= 32
# is both correct and >= serial at bs=128 -> the wall; report honestly.
#
#   serial : -pipeline OFF
#   h<N>   : -pipeline + EVM_PIPE_EVICT_HOLD_DEPTH=<N>

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

# bs=128 needs the deep sweep (fast boundaries); bs=1024 only confirms the hold
# does not regress the already-winning large-batch case.
SMALL_HOLDS="${SMALL_HOLDS:-1 2 4 8 16 32}"
LARGE_HOLDS="${LARGE_HOLDS:-1 8}"
CELL_TIMEOUT="${CELL_TIMEOUT:-12m}"

LOG="$RESULTS_DIR/sweep_evict_hold.log"
: > "$LOG"
echo "=========== SWEEP_EVICT_HOLD window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC small='$SMALL_HOLDS' large='$LARGE_HOLDS' timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local bs="$1" label="$2" pflag="$3" extra="$4"
  echo | tee -a "$LOG"
  echo "########## historic bs=$bs $label $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$RESULTS_DIR/sweep_evict_bs${bs}_${label}.out"
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

# bs=128: the failing case -- deep hold sweep.
run_cell 128 "serial" "" ""
for h in $SMALL_HOLDS; do
  run_cell 128 "h${h}" "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=${h}"
done

# bs=1024: already a win at hold 1 -- confirm larger holds keep it correct+fast.
run_cell 1024 "serial" "" ""
for h in $LARGE_HOLDS; do
  run_cell 1024 "h${h}" "-pipeline" "EVM_PIPE_EVICT_HOLD_DEPTH=${h}"
done

echo | tee -a "$LOG"
echo "=========== SWEEP_EVICT_HOLD SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SWEEP_EVICT_HOLD_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
