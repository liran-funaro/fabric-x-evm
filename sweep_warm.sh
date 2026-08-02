#!/bin/bash
# Sweep ExecuteBatch warm-pass concurrency (PERF_WARM_WORKERS) to quantify the
# reuse-vs-IO-saturation tradeoff behind the "serial auth CPU" target.
#
# Baseline (WW=0 / unset) = one warm worker per tx (~batch size): each worker
# steals ~1 tx, so it builds a fresh primed EVM (vm.NewEVM ~10% CPU) + cold
# stack arena + JUMPDEST recompute per tx -- zero cross-tx reuse, max global
# alloc/GC pressure. A positive WW forces each worker to reuse its primed EVM
# across ~batch/WW txs, cutting NewEVM/arena/JUMPDEST churn at the cost of warm
# I/O concurrency. This sweep finds whether cutting that churn helps throughput
# now that the RO-cache serves the hot read set from memory (fewer cold reads
# to overlap => fewer workers may still saturate the query service).
#
# Correctness is invariant across WW: the warm pass only primes caches and joins
# (wg.Wait) before the serial authoritative pass, so every point must hold
# "N/N committed, 0 rolled back". A point that does not is discarded, not tuned.
#
# No profiling here (throughput measurement must be clean). Fresh stack per point.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-20000}"
BS="${BS:-1024}"

# WW list: 0 = baseline (len txs). Override with: WWS="0 256 64" ./sweep_warm.sh
WWS="${WWS:-0 512 256 128 64 32}"

LOG="$HOME/sweep_warm.log"
: > "$LOG"
echo "=========== WARM-WORKERS SWEEP bs=$BS window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC $(date +%H:%M:%S) ===========" | tee -a "$LOG"
echo "WWS=[$WWS]" | tee -a "$LOG"

for WW in $WWS; do
  export PERF_WARM_WORKERS="$WW"
  echo | tee -a "$LOG"
  echo "########## WW=$WW  $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1
  echo "[stack up WW=$WW] $(date +%H:%M:%S)" | tee -a "$LOG"

  OUT="$HOME/sweep_ww_${WW}.out"
  go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
    -max-batch-size "$BS" >"$OUT" 2>&1
  rc=$?

  # Pull the two summary lines the test prints (throughput + rolled-back count).
  summary=$(grep -E 'Replay complete:|TestReplayJSONDataset: [0-9]' "$OUT" | tail -2)
  echo "[done WW=$WW rc=$rc] $(date +%H:%M:%S)" | tee -a "$LOG"
  echo "$summary" | tee -a "$LOG"
  # Compact one-liner for the final table.
  ts=$(grep -oE 'TestReplayJSONDataset: [0-9]+ EVM tx/s \([0-9]+/[0-9]+ committed' "$OUT" | tail -1)
  rb=$(grep -oE '[0-9]+ rolled-back batches' "$OUT" | tail -1)
  echo "RESULT WW=$WW rc=$rc | $ts | $rb" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
done

echo | tee -a "$LOG"
echo "=========== SWEEP SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep '^RESULT ' "$LOG" | tee -a "$LOG"
echo "SWEEP_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
