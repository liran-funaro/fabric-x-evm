#!/bin/bash
# Nil-view validation sweep. Tests the hypothesis that reading CURRENT committed
# state (query-service nil-view path, EVM_QS_NIL_VIEW=1) instead of a pinned,
# aggregation-shared snapshot eliminates the pipelined-auth stale read that drove
# the historic hot-key abort cascade -- WITHOUT any cache-side hold/invalidation
# hack. The pipeline runs with the DEFAULT machinery (full clone + one-boundary
# deferred eviction; no EVM_PIPE_* knobs), so EVM_QS_NIL_VIEW is the ONLY changed
# variable vs the pinned-view baseline.
#
# Root cause (confirmed by reading fabric-x-committer query service): consistent
# views (BeginView) share one long-lived Serializable snapshot (sharedLazyTx)
# across a 100ms view-aggregation window, so a freshly-begun view observes
# committed state as of up to 100ms ago -> a just-committed hot key reads back at
# its pre-commit version -> stale auth read-version -> MVCC abort cascade. Nil
# view (View:nil on the wire) uses the non-consistent path (sharedPool: a fresh
# pooled connection per query-batch, read-committed per statement) = current
# committed state, no snapshot lag. Intra-pass consistency is instead provided by
# the endorser read-cache (query.View) + write-cache (VersionedCache).
#
# bs=128 historic is the HARDEST regime: fast boundaries (~37ms) give the least
# margin, and BOTH prior cache-side fixes (A: selective clone invalidation; B:
# fixed-count eviction hold) failed reliability HERE while passing at bs>=256.
#
# GATE (all must hold to call nil-view the fix):
#   1. RELIABILITY : pipe_nilview bs=128 x3 all 8000/8000 committed, rb=0.
#   2. NOT SLOWER  : pipe_nilview tput >= serial_nilview tput.
#   3. NO SERIAL REGRESSION : serial_nilview ~ serial_pinned (nil-view must not
#      hurt the serial path, which is what ships by default).
# If ANY pipe_nilview run livelocks (com<8000 or rb>0) -> nil-view alone is NOT
# sufficient; report honestly, do NOT commit. If clean + reproducible + >=serial
# -> proceed to coverage sweep (bs=256/512/1024) then cleanup + commit.
#
#   serial_*  : -pipeline OFF ;  pipe_* : -pipeline
#   *_pinned  : default (BeginView consistent view) ; *_nilview : EVM_QS_NIL_VIEW=1

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

CELL_TIMEOUT="${CELL_TIMEOUT:-12m}"

LOG="$HOME/sweep_nilview.log"
: > "$LOG"
echo "=========== SWEEP_NILVIEW window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local bs="$1" label="$2" pflag="$3" extra="$4"
  echo | tee -a "$LOG"
  echo "########## historic bs=$bs $label $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/sweep_nilview_bs${bs}_${label}.out"
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

# Reference: serial with and without nil-view (regression check).
run_cell 128 "serial_pinned"   ""          ""
run_cell 128 "serial_nilview"  ""          "EVM_QS_NIL_VIEW=1"
# The hypothesis under test: pipeline + nil-view, default machinery, x3 reliability.
run_cell 128 "pipe_nilview_r1" "-pipeline" "EVM_QS_NIL_VIEW=1"
run_cell 128 "pipe_nilview_r2" "-pipeline" "EVM_QS_NIL_VIEW=1"
run_cell 128 "pipe_nilview_r3" "-pipeline" "EVM_QS_NIL_VIEW=1"

echo | tee -a "$LOG"
echo "=========== SWEEP_NILVIEW SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SWEEP_NILVIEW_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
