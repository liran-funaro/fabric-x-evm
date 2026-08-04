#!/bin/bash
# Analyze the cpu/mutex/block profiles from run_profile.sh. The redundant locks
# are RLocks (concurrent readers), so mutex/block show contention (expected ~0),
# while cpu shows the RLock/RUnlock atomic cost across the ~1024 warm workers.
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
D="$HOME/perfprof"
BIN=$(ls -t /tmp/go-build*/b001/perf.test 2>/dev/null | head -1)

echo "############ PROFILE FILES ############"
ls -la "$D"

echo; echo "############ CPU: top 40 (flat) ############"
go tool pprof -top -nodecount=40 "$D/cpu.prof" 2>/dev/null

echo; echo "############ CPU: lock/cache call paths (cum) ############"
go tool pprof -top -cum -nodecount=120 "$D/cpu.prof" 2>/dev/null \
  | grep -iE "RLock|RUnlock|\bLock\b|\bUnlock\b|semacquire|VersionedCache|ReadOnlyCache|overlayReader|cachedView|\.Read\b|readOnly|batch_executor|ExecuteBatch|codeHash"

echo; echo "############ CPU: total lock time (RWMutex + Mutex) ############"
go tool pprof -top -nodecount=400 "$D/cpu.prof" 2>/dev/null \
  | grep -iE "RWMutex|sync\.\(\*Mutex\)|runtime_Semacquire|sync\.runtime"

echo; echo "############ MUTEX: contention (delay waiting to acquire) ############"
go tool pprof -top -nodecount=25 "$D/mutex.prof" 2>/dev/null

echo; echo "############ BLOCK: goroutine blocking ############"
go tool pprof -top -nodecount=25 "$D/block.prof" 2>/dev/null

echo; echo "############ DONE ############"
