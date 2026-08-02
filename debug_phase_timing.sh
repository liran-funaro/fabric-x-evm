#!/bin/bash
# Capture per-phase (warm vs authoritative) timing for historic at bs=1024, to
# quantify WHY the pipeline loses on hot-key-serial traffic: auth is cheap
# (hot keys served from the in-memory write-cache) so there is little I/O to
# hide behind the overlap. Enables the endorser's evm.batch=debug logger so
# ExecuteBatch emits ENDORSE-TIMING (warm/auth reads + readtime + auth CPU) and
# OVERLAP-SIM (trace-driven serial vs overlap wall + stall) per batch.
#
# NOTE: the countingReader wrapper this enables adds per-read atomics, so the
# tx/s here is NOT the clean throughput number (that comes from
# validate_pipeline_fix.sh). Only the per-phase ratios matter here.

set -u
export FABRIC_LOGGING_SPEC="info:grpc=error:evm.batch=debug"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-20000}"
BS="${BS:-1024}"

LOG="$HOME/debug_phase.log"
: > "$LOG"
echo "=========== PHASE-TIMING (historic, bs=$BS) $(date +%H:%M:%S) ===========" | tee -a "$LOG"

run_cell() {
  local label="$1" pflag="$2"
  echo | tee -a "$LOG"
  echo "########## $label pipeline='$pflag' $(date +%H:%M:%S) ##########" | tee -a "$LOG"
  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$HOME/debug_${label}.out"
  # shellcheck disable=SC2086
  go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
    -dataset historic -max-batch-size "$BS" $pflag >"$OUT" 2>&1
  local rc=$?
  echo "[done $label rc=$rc] $(date +%H:%M:%S)" | tee -a "$LOG"
  # Median-ish sample: show a handful of ENDORSE-TIMING + OVERLAP-SIM lines.
  echo "--- ENDORSE-TIMING (first 3, last 3) ---" | tee -a "$LOG"
  grep -E "ENDORSE-TIMING" "$OUT" | head -3 | tee -a "$LOG"
  grep -E "ENDORSE-TIMING" "$OUT" | tail -3 | tee -a "$LOG"
  echo "--- OVERLAP-SIM (first 3, last 3) ---" | tee -a "$LOG"
  grep -E "OVERLAP-SIM" "$OUT" | head -3 | tee -a "$LOG"
  grep -E "OVERLAP-SIM" "$OUT" | tail -3 | tee -a "$LOG"
  grep -E "Replay complete:" "$OUT" | tail -1 | tee -a "$LOG"
  make stop-full clean-x >/dev/null 2>&1 || true
}

run_cell "historic_serial"   ""
run_cell "historic_pipeline" "-pipeline"

echo | tee -a "$LOG"
echo "PHASE_TIMING_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
