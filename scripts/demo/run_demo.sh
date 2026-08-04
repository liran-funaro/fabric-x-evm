#!/usr/bin/env bash
#
# End-to-end unattended demo run: preflight -> stack -> replay -> render -> summary.
#
# Designed so nothing needs a human overnight. Launch it under tmux and the two
# mp4s exist by morning:
#
#   tmux new -s demo -d 'bash scripts/demo/run_demo.sh --duration 8h 2>&1 | tee ~/demo-run.log'
#
# Usage:
#   scripts/demo/run_demo.sh --duration 8h [--outstanding 100000] [--no-render]
#                            [--dataset historic] [--batch-size 1024]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
cd "$REPO"

DURATION=""
OUTSTANDING=100000
DATASET=historic
BATCH_SIZE=1024
TARGET_TPS=0
LABEL=""
DO_RENDER=1
MIN_FREE_GB=10

while [ $# -gt 0 ]; do
  case "$1" in
    --duration)    DURATION="$2"; shift 2 ;;
    --outstanding) OUTSTANDING="$2"; shift 2 ;;
    --dataset)     DATASET="$2"; shift 2 ;;
    --batch-size)  BATCH_SIZE="$2"; shift 2 ;;
    --target-tps)  TARGET_TPS="$2"; shift 2 ;;
    --label)       LABEL="$2"; shift 2 ;;
    --no-render)   DO_RENDER=0; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$DURATION" ] || { echo "error: --duration is required (e.g. 8h, 20m)" >&2; exit 2; }

: "${EVM_PERF_DATA:?set EVM_PERF_DATA (e.g. \$HOME/workspace/evm-perf-data)}"

# --label scopes every artifact of this run (logs, frames, videos) so a second
# run cannot clobber the first's evidence or silently reuse its frames.
export DEMO_LABEL="$LABEL"
DEMO_DIR="$EVM_PERF_DATA/demo${LABEL:+/$LABEL}"
TSDB_DIR="$EVM_PERF_DATA/demo/prometheus-data"
REPLAY_LOG="$DEMO_DIR/replay.log"
WATCHDOG_LOG="$DEMO_DIR/watchdog.log"
SUMMARY="$DEMO_DIR/SUMMARY.txt"
mkdir -p "$DEMO_DIR"
rm -f "$DEMO_DIR/DONE"

log() { printf '[demo %s] %s\n' "$(date -Is)" "$*"; }

free_gb() { df -BG --output=avail / 2>/dev/null | tail -1 | tr -dc '0-9'; }

# --------------------------------------------------------------------------- #
# Preflight -- fail in the first minute rather than at hour six.
# --------------------------------------------------------------------------- #
log "preflight"
for f in "USDC_dataset.${DATASET}.json.gz" "USDC_contract.json"; do
  [ -f "$EVM_PERF_DATA/$f" ] || { echo "error: missing $EVM_PERF_DATA/$f (run scripts/setup.sh)" >&2; exit 1; }
done
avail=$(free_gb)
[ "${avail:-0}" -ge 20 ] || { echo "error: only ${avail}G free on / -- need >=20G" >&2; exit 1; }
log "disk: ${avail}G free"
command -v docker >/dev/null || { echo "error: docker not found" >&2; exit 1; }
go version >/dev/null || { echo "error: go not on PATH" >&2; exit 1; }

# --------------------------------------------------------------------------- #
# Fresh stack. Committed state contaminates re-runs, so always start clean.
# --------------------------------------------------------------------------- #
log "bringing up a fresh stack (DEMO=1)"
DEMO=1 make stop-full clean-x >/dev/null 2>&1 || true

# Start from an empty TSDB. The bind mount survives `down -v` by design, so
# without this a new run's opening frames would show the PREVIOUS run's tail
# inside the 15-minute sliding window -- a throughput cliff that never happened.
# Consequence: render a run before starting the next one (the orchestrator does).
log "clearing the previous run's TSDB at $TSDB_DIR"
if [ -d "$TSDB_DIR" ]; then
  # Prometheus writes as nobody (65534), so the host user cannot delete these
  # files -- a plain `rm -rf` fails with EPERM and, under `set -e`, kills the
  # whole run. Delete from a container running as root, the same trick
  # `make start-full` uses to chown this directory.
  docker run --rm -v "$EVM_PERF_DATA/demo":/d busybox sh -c 'rm -rf /d/prometheus-data' \
    || { echo "error: could not clear $TSDB_DIR" >&2; exit 1; }
fi

DEMO=1 make clean-x init-x start-full

log "waiting for Prometheus"
for _ in $(seq 1 120); do
  curl -fsS --max-time 5 http://localhost:9090/-/ready >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS --max-time 5 http://localhost:9090/-/ready >/dev/null || {
  echo "error: Prometheus never became ready" >&2; exit 1; }

log "waiting for the Grafana renderer"
for _ in $(seq 1 60); do
  curl -fsS --max-time 20 -o /dev/null \
    "http://localhost:3000/render/d/evm-demo/demo?width=400&height=300&kiosk" 2>/dev/null && break
  sleep 3
done
curl -fsS --max-time 30 -o /dev/null \
  "http://localhost:3000/render/d/evm-demo/demo?width=400&height=300&kiosk" || {
  echo "error: Grafana /render not working -- renderer container up?" >&2; exit 1; }
log "renderer ok"

# On RHEL the loadgen scrape target (host.docker.internal:2112) needs docker0
# traffic to the host allowed, or every panel in the video is empty.
if ! sudo -n iptables -C INPUT -i docker0 -p tcp --dport 2112 -j ACCEPT 2>/dev/null; then
  log "adding iptables allow for docker0 -> host:2112 (loadgen scrape)"
  sudo -n iptables -I INPUT -i docker0 -p tcp --dport 2112 -j ACCEPT 2>/dev/null \
    || log "WARNING: could not add the iptables rule; if the loadgen target stays DOWN, add it manually"
fi

# --------------------------------------------------------------------------- #
# Watchdog: an overnight run must not be ruined by a silent disk-full at hour 6.
# --------------------------------------------------------------------------- #
: > "$WATCHDOG_LOG"
watchdog() {
  while :; do
    local avail
    avail=$(free_gb)
    {
      printf '%s avail=%sG containers=%s\n' "$(date -Is)" "${avail:-?}" "$(docker ps -q | wc -l | tr -d ' ')"
      docker stats --no-stream --format '  {{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null || true
    } >> "$WATCHDOG_LOG"
    if [ "${avail:-999}" -lt "$MIN_FREE_GB" ]; then
      echo "WATCHDOG: only ${avail}G free -- stopping the replay early so the run so far is still usable" \
        | tee -a "$WATCHDOG_LOG"
      pkill -INT -f 'perf.test' 2>/dev/null || pkill -INT -f 'TestReplayJSONDataset' 2>/dev/null || true
      return
    fi
    sleep 60
  done
}
watchdog & WATCHDOG_PID=$!

# --------------------------------------------------------------------------- #
# Early gate: two minutes in, assert every dashboard panel actually has data.
# This is the check that saves the night. If the loadgen scrape target is DOWN
# (the classic RHEL docker0 -> host:2112 firewall case) or a metric got renamed,
# the run would complete perfectly and render eight hours of empty panels. Fail
# in minute two instead, while it can still be fixed.
# --------------------------------------------------------------------------- #
GATE_LOG="$DEMO_DIR/panel-gate.log"
early_gate() {
  sleep 120
  local up
  up=$(curl -sG --max-time 10 http://localhost:9090/api/v1/query \
        --data-urlencode 'query=up{job="loadgen"}' \
       | python3 -c 'import json,sys; r=json.load(sys.stdin); d=r.get("data",{}).get("result",[]); print(d[0]["value"][1] if d else "0")' 2>/dev/null || echo 0)
  if [ "$up" != "1" ]; then
    {
      echo "PANEL GATE FAILED: the loadgen scrape target is DOWN (up=$up)."
      echo "Every panel in the video would be empty. On RHEL this is usually the"
      echo "missing firewall allow for docker0 -> host:2112:"
      echo "  sudo iptables -I INPUT -i docker0 -p tcp --dport 2112 -j ACCEPT"
    } | tee "$GATE_LOG"
    pkill -INT -f 'perf.test' 2>/dev/null || pkill -INT -f 'TestReplayJSONDataset' 2>/dev/null || true
    return 1
  fi
  if bash "$HERE/check_dashboard_queries.sh" > "$GATE_LOG" 2>&1; then
    echo "PANEL GATE PASSED: every dashboard panel has data" | tee -a "$GATE_LOG"
  else
    {
      echo "PANEL GATE FAILED: at least one dashboard panel has no data."
      echo "Rendering would produce empty panels; stopping now instead of at dawn."
    } | tee -a "$GATE_LOG"
    pkill -INT -f 'perf.test' 2>/dev/null || pkill -INT -f 'TestReplayJSONDataset' 2>/dev/null || true
    return 1
  fi
}
early_gate & GATE_PID=$!

cleanup() { kill "$WATCHDOG_PID" "$GATE_PID" 2>/dev/null || true; }
trap cleanup EXIT

# --------------------------------------------------------------------------- #
# The replay. Serial executor (default): -pipeline is unsolved and must never
# appear in a client demo.
# --------------------------------------------------------------------------- #
log "starting replay: dataset=$DATASET duration=$DURATION outstanding=$OUTSTANDING batch=$BATCH_SIZE target-tps=$TARGET_TPS"
set +e
PERF_REPLAY_WINDOW_SIZE=0 \
PERF_REPLAY_WRAP_COUNT=100000 \
PERF_REPLAY_DURATION="$DURATION" \
GOGC=500 GOMEMLIMIT=48GiB \
go test -timeout 24h -tags=perf -run '^TestReplayJSONDataset$' -v -count=1 \
  ./integration/perf/... \
  -gateway-config ../config/gateway/fabx-full.yaml \
  -dataset "$DATASET" \
  -max-batch-size "$BATCH_SIZE" \
  -max-outstanding "$OUTSTANDING" \
  -target-tps "$TARGET_TPS" \
  -enable-metrics 2>&1 | tee "$REPLAY_LOG"
REPLAY_RC=${PIPESTATUS[0]}
set -e
log "replay exited rc=$REPLAY_RC"

kill "$WATCHDOG_PID" "$GATE_PID" 2>/dev/null || true

# --------------------------------------------------------------------------- #
# Render regardless of the replay's exit status. A crashed 5-hour run still
# makes a good demo; discarding it would be the worse outcome.
# --------------------------------------------------------------------------- #
RENDER_RC=0
if [ "$DO_RENDER" -eq 1 ]; then
  log "rendering videos"
  bash "$HERE/render_video.sh" || RENDER_RC=$?
  log "render exited rc=$RENDER_RC"
else
  log "skipping render (--no-render)"
fi

# --------------------------------------------------------------------------- #
# Summary
# --------------------------------------------------------------------------- #
{
  echo "EVM on Fabric-X -- client demo run"
  echo "generated: $(date -Is)"
  echo
  echo "label:   ${LABEL:-(none)}"
  echo "config:  dataset=$DATASET duration=$DURATION max-outstanding=$OUTSTANDING"
  echo "         max-batch-size=$BATCH_SIZE executor=serial GOGC=500 GOMEMLIMIT=48GiB"
  if [ "$TARGET_TPS" != "0" ]; then
    echo "         target-tps=$TARGET_TPS  <-- THROTTLED on purpose to fit the disk"
    echo "         budget; this is NOT the system's throughput ceiling."
  else
    echo "         target-tps=0 (unpaced -- this run measures the true ceiling)"
  fi
  echo "exit:    replay=$REPLAY_RC render=$RENDER_RC"
  echo
  echo "--- headline ---"
  grep -E 'Replay complete:|Commit-path timing:' "$REPLAY_LOG" || echo "(no headline lines -- check $REPLAY_LOG)"
  echo
  echo "--- last progress ---"
  grep -E 'Progress:' "$REPLAY_LOG" | tail -3 || true
  echo
  echo "--- videos ---"
  ls -lh "$DEMO_DIR/video" 2>/dev/null || echo "(none)"
  echo
  echo "--- panel gate ---"
  tail -5 "$GATE_LOG" 2>/dev/null || echo "(gate did not run)"
  echo
  echo "--- watchdog warnings ---"
  grep -E 'WATCHDOG' "$WATCHDOG_LOG" || echo "(none)"
  echo
  echo "--- disk ---"
  df -h / | tail -1
  echo
  echo "logs: $REPLAY_LOG  $WATCHDOG_LOG"
  echo "the stack is intentionally left UP so the dashboard is still live;"
  echo "tear down with: DEMO=1 make stop-full clean-x"
} > "$SUMMARY"

cat "$SUMMARY"
touch "$DEMO_DIR/DONE"
log "DONE -> $SUMMARY"

# Surface a failed render, but never a failed replay: a short run is still a
# usable demo and the summary records what happened.
exit "$RENDER_RC"
