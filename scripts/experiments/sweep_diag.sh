#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
# Residual-diagnosis sweep (Phase-1: CONFIRM the residual before any fix).
#
# Reproduces the EXACT config that livelocked in sweep_nilview.sh run r3
# (-pipeline + EVM_QS_NIL_VIEW=1, default pipeline machinery, bs=128 historic --
# the hardest regime, fastest boundaries, least margin) and adds EVM_PIPE_DIAG=1,
# which turns on common/pipediag: gated instrumentation that classifies the
# residual stale read driving the MVCC abort cascade into exactly one of:
#
#   H1  query-service visibility lag   -> a per-key visibility-gated eviction is
#       (a cold fetch trailed an acked    the right fix.
#       commit; PIPE-DIAG-QSLAG fires)
#   H2  stale view clone across a boundary (cold-fetched current, committed higher
#       after, clone carried it forward)  -> the clone/inherit layer is the bug.
#   H3  write-cache / inheritance (never cold-fetched) -> a write-cache logic bug.
#
# Diagnostic keys off PIPE-DIAG-ABORT summary lines (one per aborted batch:
# H1/H2/H3 tally + running qslag_total) and PIPE-DIAG-QSLAG lines. When diag is
# off it is a no-op, so it does NOT change the serial or clean-pipeline behaviour;
# per-event logging is capped (pipediag.logCap) so a heavy-lag run cannot flood
# stderr and perturb the livelock it is measuring.
#
# Livelock incidence under this config was ~1/3, so we run N times to CAPTURE at
# least one. We do NOT gate/commit here -- the goal is a classified residual to
# report, per the user's "confirm residual first" decision.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-8000}"

BS="${BS:-128}"
RUNS="${RUNS:-6}"
CELL_TIMEOUT="${CELL_TIMEOUT:-12m}"

LOG="$HOME/sweep_diag.log"
: > "$LOG"
echo "=========== SWEEP_DIAG bs=$BS runs=$RUNS window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC timeout=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local i="$1"
  echo | tee -a "$LOG"
  echo "########## historic bs=$BS pipe_nilview_diag_r$i $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/sweep_diag_bs${BS}_r${i}.out"
  timeout "$CELL_TIMEOUT" env EVM_QS_NIL_VIEW=1 EVM_PIPE_DIAG=1 \
    go test -timeout 18m -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml \
    -dataset historic -max-batch-size "$BS" -pipeline >"$OUT" 2>&1
  local rc=$?

  local line tput com tot rb
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')

  # DIAG aggregates: sum the direction buckets (notinlog/under/equal/over) and the
  # Under sub-attributions (H1/H2/H3) across EVERY PIPE-DIAG-ABORT summary. Parse
  # by splitting each line into key=val tokens and summing per key -- robust,
  # unlike the earlier `grep -oE 'H1=[0-9]+' | grep -oE '[0-9]+'` which also
  # extracted the field-name digit (the "1" in "H1") and triple-counted.
  local aborts qslag lastabort sums
  aborts=$(grep -c 'PIPE-DIAG-ABORT' "$OUT")
  qslag=$(grep -c 'PIPE-DIAG-QSLAG' "$OUT")
  lastabort=$(grep 'PIPE-DIAG-ABORT' "$OUT" | tail -1)
  sums=$(grep 'PIPE-DIAG-ABORT' "$OUT" | awk '
    { for (i=1;i<=NF;i++) { n=split($i,a,"="); if(n==2){ s[a[1]]+=a[2] } } }
    END { printf "notinlog=%d under=%d equal=%d over=%d H1=%d H2=%d H3=%d",
          s["notinlog"], s["under"], s["equal"], s["over"], s["H1"], s["H2"], s["H3"] }')

  echo "RESULT historic bs=$BS r$i rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA}" | tee -a "$LOG"
  echo "DIAG   r$i abort_lines=$aborts qslag_lines=$qslag $sums" | tee -a "$LOG"
  echo "DIAG   r$i last_abort_summary: ${lastabort:-<none>}" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

for i in $(seq 1 "$RUNS"); do
  run_cell "$i"
done

echo | tee -a "$LOG"
echo "=========== SWEEP_DIAG SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep -E '^RESULT |^DIAG ' "$LOG" | tee -a "$LOG"
echo "SWEEP_DIAG_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
