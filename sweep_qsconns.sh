#!/bin/bash
# Sweep the endorser->query-service connection-pool size (PERF_QS_CONNS) to find
# the throughput knee. The DB-resource discovery proved nothing downstream of the
# endorser is saturated (99.995% PG cache hit, 2/10 DB conns, QS 1-2/32 cores), so
# the read ceiling is serialization on the single shared endorser->QS gRPC conn.
# This measures how much a connection pool (round-robin GetRows across N conns)
# recovers.
#
# N=1 must reproduce the pre-change baseline (single shared connection). N>=2 also
# VERIFIES that a server-side view is queryable across connections: if it were
# connection-scoped, GetRows on a non-origin conn would error and the point would
# not hold "N/N committed, 0 rolled back". A point that does not hold is discarded.
#
# No profiling (throughput must be clean). Fresh stack per point.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-20000}"
BS="${BS:-1024}"

# Connection counts. Override with: NS="1 8 64" ./sweep_qsconns.sh
NS="${NS:-1 2 4 8 16 32}"

LOG="$HOME/sweep_qsconns.log"
: > "$LOG"
echo "=========== QS-CONNS SWEEP bs=$BS window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC $(date +%H:%M:%S) ===========" | tee -a "$LOG"
echo "NS=[$NS]" | tee -a "$LOG"

for N in $NS; do
  export PERF_QS_CONNS="$N"
  echo | tee -a "$LOG"
  echo "########## QS_CONNS=$N  $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1
  echo "[stack up N=$N] $(date +%H:%M:%S)" | tee -a "$LOG"

  OUT="$HOME/sweep_qsconns_${N}.out"
  go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
    -max-batch-size "$BS" >"$OUT" 2>&1
  rc=$?

  summary=$(grep -E 'Replay complete:|TestReplayJSONDataset: [0-9]' "$OUT" | tail -2)
  echo "[done N=$N rc=$rc] $(date +%H:%M:%S)" | tee -a "$LOG"
  echo "$summary" | tee -a "$LOG"
  ts=$(grep -oE 'TestReplayJSONDataset: [0-9]+ EVM tx/s \([0-9]+/[0-9]+ committed' "$OUT" | tail -1)
  rb=$(grep -oE '[0-9]+ rolled-back batches' "$OUT" | tail -1)
  # Surface any GetRows/view errors (would prove cross-conn views broke).
  errs=$(grep -icE 'view|GetRows|rpc error|Unavailable' "$OUT")
  echo "RESULT N=$N rc=$rc | $ts | $rb | logmatches(view/rpc)=$errs" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
done

echo | tee -a "$LOG"
echo "=========== SWEEP SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "QSCONNS_SWEEP_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
