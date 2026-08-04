#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)/../.." || exit 1  # repo root (scripts/experiments -> ../../)
RESULTS_DIR="${RESULTS_DIR:-${EVM_PERF_DATA:-$HOME/workspace/evm-perf-data}/results}"; mkdir -p "$RESULTS_DIR"
# Depth-1 ordered-submitter go/no-go sweep (plan Task 7).
#
# Grid: batch-size x dataset x config, each cell on a FRESH network. For each cell
# it spins up a clean fabric-x network, replays a fixed window of EVM txs, and
# parses the "Replay complete:" line for throughput / committed / rolled-back.
#
# Configs (the ONLY difference between them is pipelining + the ordered gate):
#   A  serial baseline           -> fabx-full.yaml,          no -pipeline
#   B  pipeline, ordered-submit OFF -> fabx-full.yaml,        -pipeline
#      (default 64 orderer submitters; EXPECTED to livelock/roll back at small bs
#       -- this is the baseline-sanity check that the reorder bug is still present
#       without the gate)
#   C  pipeline, ordered-submit ON  -> fabx-full-ordered.yaml, -pipeline
#      (the harness sees ordered-submit=true, clamps orderer submission to a single
#       worker, and installs the NoFT ordered-delivery gate)
#
# HARD gate (correctness, the go/no-go): config C commits WINDOW/WINDOW with
# 0 rolled-back batches at EVERY batch size, BOTH datasets. No livelock.
# Baseline sanity: config B rolls back / under-commits at small bs.
# Throughput (informational): C-vs-A per bs; C may trail A at very small bs
# (depth-1 block-cut latency), which is the accepted tradeoff.
#
# Everything is overridable via env so a cheap smoke cell can run first:
#   CONFIGS='C' BS_LIST='128' DATASETS='historic' ./sweep_ordered.sh
#
# Dataset inputs live OUTSIDE the repo tree (DATA_DIR, default
# ${EVM_PERF_DATA:-~/workspace/evm-perf-data}) so a repo sync never touches
# them; ds_path_for maps the short dataset label to
# an absolute .json.gz path, which the perf test opens directly (replay test
# accepts an absolute path with a .gz extension, bypassing the testdata/ join).
#
# Config C's ordered-delivery reads ordered blocks from the assemblers over the
# NoFT deliver stream. That stream dials the config-block's "deliver" endpoints,
# which are the Docker-internal names orderer-partyN-assembler:70N3. The perf
# gateway runs on the HOST, which can't resolve those names -- but the compose
# publishes each assembler's 70N3 port to 127.0.0.1 and the assembler TLS certs
# carry orderer-partyN-assembler as a SAN. So mapping those four names to
# 127.0.0.1 in /etc/hosts is sufficient: the name resolves to loopback, the
# published port disambiguates each party, and TLS ServerName still matches the
# SAN. ensure_assembler_hosts() installs that mapping idempotently (passwordless
# sudo) before the grid runs. It does NOT affect the Docker containers (they use
# Docker's embedded DNS, not the host /etc/hosts), so intra-network delivery is
# untouched. Config A/B don't use the stream; the mapping is harmless for them.
set -u

export FABRIC_LOGGING_SPEC="${FABRIC_LOGGING_SPEC:-info:grpc=error}"
export GOGC="${GOGC:-500}"
export GOMEMLIMIT="${GOMEMLIMIT:-48GiB}"
export PERF_REPLAY_WINDOW_SIZE="${WINDOW:-20000}"

BS_LIST="${BS_LIST:-1 16 128 512 1024}"
DATASETS="${DATASETS:-historic synthetic}"
CONFIGS="${CONFIGS:-A B C}"
CELL_TIMEOUT="${CELL_TIMEOUT:-25m}"
GO_TIMEOUT="${GO_TIMEOUT:-30m}"
DATA_DIR="${DATA_DIR:-${EVM_PERF_DATA:-$HOME/workspace/evm-perf-data}}"

LOG="$RESULTS_DIR/sweep_ordered.log"
: > "$LOG"
echo "=========== SWEEP_ORDERED window=$PERF_REPLAY_WINDOW_SIZE gogc=$GOGC bs='$BS_LIST' ds='$DATASETS' cfg='$CONFIGS' cell_to=$CELL_TIMEOUT $(date +%Y-%m-%dT%H:%M:%S) ===========" | tee -a "$LOG"

# Map a config letter to its gateway-config file and pipeline flag.
gwcfg_for() { case "$1" in A|B) echo ../config/gateway/fabx-full.yaml ;; C) echo ../config/gateway/fabx-full-ordered.yaml ;; esac; }
pipe_for()  { case "$1" in A) echo "" ;; B|C) echo "-pipeline" ;; esac; }

# Map a dataset label to an absolute .json.gz path under DATA_DIR (outside the
# repo). A label already containing a path separator or extension is passed
# through unchanged, so an explicit path still works.
ds_path_for() {
  case "$1" in
    historic|synthetic) echo "$DATA_DIR/USDC_dataset.$1.json.gz" ;;
    *)                  echo "$1" ;;
  esac
}

# Ensure the host can resolve the assembler Docker hostnames to loopback so the
# config-C ordered-delivery stream reaches the published 70N3 deliver ports.
# Idempotent: guarded by a sentinel line; appends once via passwordless sudo.
HOSTS_SENTINEL="# fabric-x-evm ordered-delivery assembler mapping"
ensure_assembler_hosts() {
  if grep -qF "$HOSTS_SENTINEL" /etc/hosts 2>/dev/null; then
    echo "assembler /etc/hosts mapping already present" | tee -a "$LOG"
    return 0
  fi
  local block
  block=$(printf '%s\n127.0.0.1 orderer-party1-assembler orderer-party2-assembler orderer-party3-assembler orderer-party4-assembler\n' "$HOSTS_SENTINEL")
  if printf '%s\n' "$block" | sudo tee -a /etc/hosts >/dev/null 2>&1; then
    echo "installed assembler /etc/hosts mapping (party1-4 -> 127.0.0.1)" | tee -a "$LOG"
  else
    echo "WARNING: could not write /etc/hosts (no sudo?); config-C delivery will fail DNS and fall back to unordered" | tee -a "$LOG"
  fi
}

run_cell() {
  local cfg="$1" ds="$2" bs="$3"
  local gwcfg pipe dspath
  gwcfg=$(gwcfg_for "$cfg"); pipe=$(pipe_for "$cfg"); dspath=$(ds_path_for "$ds")

  echo | tee -a "$LOG"
  echo "########## cfg=$cfg ds=$ds bs=$bs gw=$gwcfg pipe='${pipe:-none}' data=$dspath $(date +%H:%M:%S) ##########" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full >/dev/null 2>&1

  local OUT="$RESULTS_DIR/sweep_ordered_${cfg}_${ds}_bs${bs}.out"
  # shellcheck disable=SC2086
  timeout "$CELL_TIMEOUT" \
    go test -timeout "$GO_TIMEOUT" -tags=perf -run '^TestReplayJSONDataset$' -v \
    -count=1 ./integration/perf/... -gateway-config "$gwcfg" \
    -dataset "$dspath" -max-batch-size "$bs" $pipe >"$OUT" 2>&1
  local rc=$?

  local line tput com tot rb stalled verdict
  line=$(grep -E 'Replay complete:' "$OUT" | tail -1)
  tput=$(echo "$line" | grep -oE '[0-9]+ EVM tx/s' | grep -oE '^[0-9]+')
  com=$(echo "$line" | grep -oE '[0-9]+/[0-9]+ EVM txs committed' | grep -oE '^[0-9]+')
  tot=$(echo "$line" | grep -oE '/[0-9]+ EVM txs committed' | grep -oE '[0-9]+')
  rb=$(echo "$line" | grep -oE '[0-9]+ rolled-back batches' | grep -oE '^[0-9]+')
  stalled=$(grep -c 'Stalled: no commit progress' "$OUT")

  # Per-cell verdict:
  #  C  CLEAN  iff rc==0, no stall, committed==total, 0 rolled-back  (HARD gate)
  #     DIRTY  otherwise (any rollback / under-commit / stall / nonzero rc)
  #  B  LIVELOCK if under-committed / rolled-back / stalled (baseline sanity, expected small bs)
  #     ok       otherwise
  #  A  ok / under (serial baseline)
  verdict="?"
  if [ "$cfg" = "C" ]; then
    if [ "$rc" = "0" ] && [ "${stalled:-0}" = "0" ] && [ -n "$com" ] && [ "$com" = "${tot:-x}" ] && [ "${rb:-1}" = "0" ]; then
      verdict="CLEAN"
    else
      verdict="DIRTY<<<"
    fi
  else
    if [ "${stalled:-0}" != "0" ] || { [ -n "$com" ] && [ "$com" != "${tot:-x}" ]; } || [ "${rb:-0}" != "0" ]; then
      verdict="under/livelock"
    else
      verdict="ok"
    fi
  fi

  echo "RESULT cfg=$cfg ds=$ds bs=$bs rc=$rc tput=${tput:-NA} com=${com:-NA}/${tot:-NA} rb=${rb:-NA} stalled=${stalled:-0} -> $verdict" | tee -a "$LOG"

  make stop-full clean-x >/dev/null 2>&1 || true
}

# Make the assembler deliver endpoints reachable from the host before the grid
# runs (config C's ordered-delivery stream; harmless for config A/B).
ensure_assembler_hosts

for ds in $DATASETS; do
  for bs in $BS_LIST; do
    for cfg in $CONFIGS; do
      run_cell "$cfg" "$ds" "$bs"
    done
  done
done

echo | tee -a "$LOG"
echo "=========== SWEEP_ORDERED SUMMARY $(date +%H:%M:%S) ===========" | tee -a "$LOG"
grep -E '^RESULT ' "$LOG" | tee -a "$LOG"
echo | tee -a "$LOG"
echo "HARD GATE (config C must be CLEAN at every cell):" | tee -a "$LOG"
if grep -E '^RESULT cfg=C ' "$LOG" | grep -q 'DIRTY'; then
  echo "  FAIL -- at least one config-C cell is DIRTY (see <<< above)" | tee -a "$LOG"
else
  if grep -qE '^RESULT cfg=C ' "$LOG"; then
    echo "  PASS -- every config-C cell CLEAN (WINDOW committed, 0 rolled-back)" | tee -a "$LOG"
  else
    echo "  N/A  -- no config-C cells were run" | tee -a "$LOG"
  fi
fi
echo "SWEEP_ORDERED_DONE $(date +%H:%M:%S)" | tee -a "$LOG"
