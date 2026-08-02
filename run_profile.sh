#!/bin/bash
# Measurement-only profiling run: quantify time spent in the per-read cache
# locks (VersionedCache.Read, ReadOnlyCache.get) and the serial-auth
# overlayReader lock. Mirrors test.sh but arms CPU/mutex/block profiling
# (PERF_PROFILE_DIR) and runs WITHOUT evm.batch=debug so no timing/OVERLAP-SIM
# instrumentation perturbs the CPU profile. Mutex/block profiles sample every
# event (fraction/rate=1), so they are complete regardless of run length.

export FABRIC_LOGGING_SPEC="info:grpc=error"
export HOST_DATA="${HOST_DATA:-0}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"

export PERF_PROFILE_DIR="$HOME/perfprof"
rm -rf "$PERF_PROFILE_DIR"
mkdir -p "$PERF_PROFILE_DIR"

export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-20000}"
BS="${BS:-1024}"

echo "=========== PROFILE bs=$BS window=$PERF_REPLAY_WINDOW_SIZE $(date +%H:%M:%S) ==========="

# Fresh stack (proven recipe).
make stop-full clean-x >/dev/null 2>&1 || true
make clean-x init-x start-full

echo "[stack up] $(date +%H:%M:%S)"

echo "[run] $(date +%H:%M:%S)"
go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v \
  -count=1 ./integration/perf/... -gateway-config fabx-full.yaml \
  -max-batch-size "$BS"
rc=$?
echo "[done rc=$rc $(date +%H:%M:%S)]"

echo "=== profiles in $PERF_PROFILE_DIR ==="
ls -la "$PERF_PROFILE_DIR"

make stop-full clean-x >/dev/null 2>&1 || true
echo "PROFILE_DONE rc=$rc $(date +%H:%M:%S)"
