#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
# DB / query-service resource discovery during a replay. Answers: is the read
# path CPU-bound, disk-IO-bound, or connection/concurrency-bound?
#
#   - QS_avg_cores / DB_avg_cores (cgroup cpu delta / wall): how many of the 32
#     vCPU each process actually burns. Low => not CPU-bound.
#   - PG cache-hit ratio (pg_stat_database blks_hit vs blks_read after a reset):
#     ~99%+ => reads served from shared_buffers/page cache, NOT disk-IO-bound.
#   - PG max ACTIVE connections during the run vs the QS->DB pool cap (10): if it
#     pins at ~10, the DB pool is the ceiling (raise max-connections); if it sits
#     well below 10, the endorser is not pushing enough concurrent reads (the
#     single shared gRPC conn is the client-side serialization point).
#   - committer-db BlockIO delta: bytes actually read from disk during the run.
#
# Fresh stack, no profiling. Container names + cgroup version discovered at runtime.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-30000}"
BS="${BS:-1024}"
DOCKER="${DOCKER:-docker}"
LOG="$HOME/measure_qs.log"; : > "$LOG"

log() { echo "$@" | tee -a "$LOG"; }

echo "=== MEASURE QS/DB window=$PERF_REPLAY_WINDOW_SIZE bs=$BS $(date +%T) ===" | tee -a "$LOG"
make stop-full clean-x >/dev/null 2>&1 || true
make clean-x init-x start-full >/dev/null 2>&1
log "[stack up $(date +%T)]"

# Discover actual container names (compose adds project prefix + index).
QS=$($DOCKER ps --format '{{.Names}}' | grep -i 'query-service' | head -1)
DB=$($DOCKER ps --format '{{.Names}}' | grep -Ei 'committer-db|postgres' | head -1)
log "QS container = $QS"
log "DB container = $DB"
if [ -z "$QS" ] || [ -z "$DB" ]; then log "ERROR: could not find QS/DB containers"; $DOCKER ps --format '{{.Names}}' | tee -a "$LOG"; exit 1; fi

pg() { $DOCKER exec "$DB" psql -U sc_user -d sc_db -tAc "$1" 2>/dev/null; }
# usage_usec (cgroup v2) else cpuacct.usage ns->us (v1).
cpuusec() { $DOCKER exec "$1" sh -c 'v=$(awk "/^usage_usec/{print \$2}" /sys/fs/cgroup/cpu.stat 2>/dev/null); if [ -n "$v" ]; then echo "$v"; else n=$(cat /sys/fs/cgroup/cpu/cpuacct.usage 2>/dev/null); echo $((n/1000)); fi' 2>/dev/null; }

log "--- PG settings (stock postgres:18.3 unless tuned) ---"
for s in shared_buffers max_connections work_mem effective_cache_size max_parallel_workers max_worker_processes; do
  log "  $s = $(pg "SHOW $s;")"
done
pg "SELECT pg_stat_reset();" >/dev/null

# Background samplers: PG active-connection count (fast) + docker stats snapshots.
ACT="$HOME/pg_active.samples"; : > "$ACT"
( while :; do pg "SELECT count(*) FROM pg_stat_activity WHERE datname='sc_db' AND state='active';" >> "$ACT"; sleep 0.25; done ) &
SAMP_PG=$!
STATS="$HOME/dockerstats.samples"; : > "$STATS"
( while :; do $DOCKER stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}} blkio={{.BlockIO}}' "$QS" "$DB" 2>/dev/null >> "$STATS"; done ) &
SAMP_ST=$!

qs0=$(cpuusec "$QS"); db0=$(cpuusec "$DB"); t0=$(date +%s.%N)

go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v -count=1 \
  ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml -max-batch-size "$BS" \
  > "$HOME/measure_run.out" 2>&1
rc=$?

t1=$(date +%s.%N); qs1=$(cpuusec "$QS"); db1=$(cpuusec "$DB")
kill $SAMP_PG $SAMP_ST 2>/dev/null

wall=$(awk "BEGIN{print $t1-$t0}")
qscores=$(awk "BEGIN{if($wall>0)printf \"%.2f\", ($qs1-$qs0)/1e6/$wall}")
dbcores=$(awk "BEGIN{if($wall>0)printf \"%.2f\", ($db1-$db0)/1e6/$wall}")
maxact=$(sort -n "$ACT" 2>/dev/null | tail -1)
avgact=$(awk '{s+=$1;n++}END{if(n)printf "%.1f",s/n}' "$ACT" 2>/dev/null)

log "--- RESULT ---"
grep -E 'Replay complete:|TestReplayJSONDataset: [0-9]' "$HOME/measure_run.out" | tail -2 | tee -a "$LOG"
log "wall=${wall}s  QS_avg_cores=${qscores}  DB_avg_cores=${dbcores}   (host = 32 vCPU)"
log "PG active conns during run: max=${maxact} avg=${avgact}   (QS->DB pool cap = 10)"
log "PG cache: $(pg "SELECT 'blks_hit='||blks_hit||' blks_read='||blks_read||' hit_ratio='||round(100.0*blks_hit/nullif(blks_hit+blks_read,0),4)||'%'||' tup_returned='||tup_returned||' tup_fetched='||tup_fetched FROM pg_stat_database WHERE datname='sc_db';")"
log "peak docker-stats lines (QS/DB CPU% can exceed 100 = multiple cores):"
sort -t= -k2 -rn "$STATS" 2>/dev/null | grep -i query-service | head -3 | sed 's/^/  /' | tee -a "$LOG"
sort -t= -k2 -rn "$STATS" 2>/dev/null | grep -Ei 'committer-db|postgres' | head -3 | sed 's/^/  /' | tee -a "$LOG"

make stop-full clean-x >/dev/null 2>&1 || true
log "MEASURE_DONE rc=$rc $(date +%T)"
