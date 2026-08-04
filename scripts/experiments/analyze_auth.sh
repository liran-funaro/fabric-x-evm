#!/bin/bash
# Focused analysis of the fresh CPU profile for the "serial auth CPU" question.
# Goal: attribute the alloc/GC + memclr + keccak costs and decide whether the
# churn lives in go-ethereum's interpreter (needs a dependency fork) or in our
# layer (fixable in-tree).
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
source ~/.bashrc 2>/dev/null
D="$HOME/perfprof"
P="$D/cpu.prof"

echo "############ FILES ############"; ls -la "$D"

echo; echo "############ FLAT top 45 (self CPU: memclr/mallocgc/keccak show here) ############"
go tool pprof -top -nodecount=45 "$P" 2>/dev/null

echo; echo "############ CUM top: execution tree (ExecuteBatch/runOn/ApplyMessage/EVM.Run) ############"
go tool pprof -top -cum -nodecount=200 "$P" 2>/dev/null \
  | grep -iE "ExecuteBatch|runOn|classify|ApplyMessage|stateTransition|EVM\)\.(Call|Run|Create)|Interpreter|opCall|DelegateCall|Memory|newstack|NewEVM|codeBitmap|Keccak|keccak|GetCode|resolveCodeHash|getState|viewGet|cachedView|accKey|storeKey|Result\b|mallocgc|memclr|makeslice|growslice|newobject|GC\b" | head -80

echo; echo "############ PEEK memclrNoHeapPointers (who zeroes memory?) ############"
go tool pprof -peek='memclrNoHeapPointers' "$P" 2>/dev/null | head -40

echo; echo "############ PEEK mallocgc (who allocates?) ############"
go tool pprof -peek='mallocgc$' "$P" 2>/dev/null | head -45

echo; echo "############ PEEK go-ethereum Memory.Resize / NewMemory ############"
go tool pprof -peek='vm\.\(\*Memory\)\.(Resize|Set)|vm\.NewMemory' "$P" 2>/dev/null | head -40

echo; echo "############ PEEK keccak (SLOAD slot hash + code hash) ############"
go tool pprof -peek='keccakF1600|Keccak256|NewLegacyKeccak256' "$P" 2>/dev/null | head -40

echo; echo "############ PEEK our accKey/storeKey (per-read key building) ############"
go tool pprof -peek='execution\.(accKey|storeKey)' "$P" 2>/dev/null | head -30

echo; echo "############ DONE ############"
