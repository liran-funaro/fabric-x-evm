#!/bin/bash
# Selective-clone-invalidation depth sweep (the "measure, then decide" gate).
#
# Root cause (bisect1+2): the pipeline's inherited COLD query-view clone
# (query.View.Reopen's maps.Clone, consulted first in View.Get) serves auth a
# stale warm-time committed read when a key's committed version advanced between
# warm(N+1) and auth(N+1) -> MVCC abort cascade -> historic hot-key livelock.
# The two extremes both fail: the full clone (D=0) livelocks at small batches;
# the empty clone (cold_off, HEAD) is correct but ~30x slower and stalls at
# bs=1024. Selective invalidation is the middle ground: on the auth-view reopen,
# drop ONLY the keys whose committed version advanced over the last D pipeline
# boundaries (VersionedCache.RecentlyCommittedKeys -> View.ReopenInvalidating),
# so auth re-reads exactly those fresh (correctness) and keeps every other warm
# read a cache hit (speed). D is set by EVM_PIPE_INVALIDATE_DEPTH.
#
# This sweep finds, per batch size, the SMALLEST D that is correct (rb=0, no
# stall) -- that D measures the query-service visibility-lag horizon -- and
# whether that D's throughput beats serial. D=0 is the full-clone baseline
# (expected to livelock at bs=128). serial is the correctness+speed reference.
#
#   serial : -pipeline OFF
#   d<N>   : -pipeline + EVM_PIPE_INVALIDATE_DEPTH=<N>
#
# across bs in {128,1024}. GATE: smallest D with com==WINDOW/WINDOW && rb=0 &&
# no timeout; at that D, tput >= serial tput. If no D <= 8 is both correct and
# >= serial -> the wall; report and fall back to fix B (visibility-gated
# eviction) or abandon.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

SIZES="${SIZES:-128 1024}"
DEPTHS="${DEPTHS:-0 1 2 4 8}"
CELL_TIMEOUT="${CELL_TIMEOUT:-12m}"

LOG="$HOME/sweep_depth.log"
: > "$LOG"
echo "=========== SWEEP_DEPTH window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC depths='$DEPTHS' sizes='$SIZES' timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local bs="$1" label="$2" pflag="$3" extra="$4"
  echo | tee -a "$LOG"
  echo "########## historic bs=$bs $label $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/sweep_historic_bs${bs}_${label}.out"
  # shellcheck disable=SC2086
  timeout "$CELL_TIMEOUT" env $extra go test -timeout 18m -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
    -dataset historic -max-batch-size "$bs" $pflag >"$OUT" 2>&1
  local rc=$?

  local line tput com tot rb rt
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')
  # Best-effort auth-phase readtime (present only when phase timing is logged);
  # NA when absent, never fatal.
  rt=$(grep -oE 'auth[^0-9]*readtime[= ]*[0-9.]+[a-zµ]*' "$OUT" | tail -1)
  echo "RESULT historic bs=$bs $label rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA} ${rt:+authrt=$rt}" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

for bs in $SIZES; do
  run_cell "$bs" "serial" "" ""
  for d in $DEPTHS; do
    run_cell "$bs" "d${d}" "-pipeline" "EVM_PIPE_INVALIDATE_DEPTH=${d}"
  done
done

echo | tee -a "$LOG"
echo "=========== SWEEP_DEPTH SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SWEEP_DEPTH_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
