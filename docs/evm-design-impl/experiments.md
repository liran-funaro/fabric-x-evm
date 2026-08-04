# Experiment Run-Book (both datasets, always)

How a new agent reproduces the performance numbers and collects evidence. The
authoritative measurements are taken on **native hardware** (see
[server-setup.md](server-setup.md)); local docker (colima) is relative-curve
only.

**Iron rule: always measure BOTH workloads and report them separately.**
- `-dataset synthetic` → `USDC_dataset.synthetic.json.gz` — conflict-free,
  raw throughput ceiling.
- `-dataset historic` → `USDC_dataset.historic.json.gz` (real Jan-2020 USDC) —
  extremely high conflict rate; exercises the MVCC rollback / re-batch path.

---

## 0. Prerequisites

```bash
# One-time: download the contract + BOTH datasets into the out-of-tree data dir.
export EVM_PERF_DATA="$HOME/workspace/evm-perf-data"   # input data AND output results
bash scripts/setup.sh                                  # skips files already present
```

`EVM_PERF_DATA` is the single data in/out directory and lives **outside** the
repo tree, so a `rsync` of the working tree never touches it. The perf test
and the experiment scripts both resolve datasets and write results there:

- Datasets: `$EVM_PERF_DATA/USDC_dataset.{synthetic,historic}.json.gz` and the
  prime contract `$EVM_PERF_DATA/USDC_contract.json`. A bare `-dataset
  <name>` resolves to `$EVM_PERF_DATA/USDC_dataset.<name>.json.gz` when
  `EVM_PERF_DATA` is set (else falls back to `testdata/`); a value with a path
  or extension is opened directly.
- Results: experiment scripts write logs/summaries under
  `$EVM_PERF_DATA/results/`.

Toolchain: Go (repo version) + Docker + docker compose. On a fresh host, run
[server-setup.md](server-setup.md) / `scripts/setup-server.sh` first.

---

## 1. Bring up the full stack

From the repo root:

```bash
make clean-x init-x start-full        # HOST_DATA=0 (container-local volumes) is the default
```

- `init-x` bakes the network fixture (crypto, config-block) from
  `testdata/` and `testdata/shared_config.yaml` (which carries the
  `RequestMaxBytes` fix, `92d9ba8`).
- `start-full` brings up all containers: the 4-party arma BFT orderer
  (router/batcher/assembler/consenter), the committer
  (coordinator/sidecar/validator/query-service), Postgres, **Prometheus**, and
  **Grafana**. Expect ~24 containers healthy in ~1 minute on native hardware.
- Data is container-local (docker named volumes on the VM's ext4 disk) by
  default. `HOST_DATA=1 make start-full` rebinds to `./data` on the host for
  inspection (routes writes through the host mount — slower).

Compose files live under `config/compose/`; the Makefile invokes them with
`--project-directory $(CURDIR)` so every `./`-relative mount still resolves
from repo root. Monitoring config is under `config/monitoring/`.

### Confirm monitoring is live
- **Prometheus** — http://localhost:9090 — check `Status → Targets`: the
  `loadgen` job (`host.docker.internal:2112`) goes UP once a perf run starts
  with `-enable-metrics`.
- **Grafana** — http://localhost:3000 — open the **EVM / Loadgen** dashboard
  (`config/monitoring/grafana/evm-loadgen.json`, uid `evm-loadgen`). Panels:
  EVM tx/s, committer tx/s (batches), in-flight/outstanding, warm vs auth
  phase latency, commit latency, rollback rate, spec-version aborts by class,
  queue sizes.

---

## 2. Run the perf replay — once per dataset

The harness chdirs to `integration/`, so `-gateway-config` and its internal
`../testdata/...` paths resolve from there. Run from the repo root:

```bash
# SYNTHETIC (conflict-free ceiling)
go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v -count=1 \
  ./integration/perf/... \
  -gateway-config ../config/gateway/fabx-full.yaml \
  -dataset synthetic -max-batch-size 1024 -enable-metrics

# HISTORIC (high-conflict) — fresh stack first (state accumulates on a live chain)
make stop-full clean-x && make clean-x init-x start-full
go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v -count=1 \
  ./integration/perf/... \
  -gateway-config ../config/gateway/fabx-full.yaml \
  -dataset historic -max-batch-size 1024 -enable-metrics
```

Key flags (`integration/perf/replay_json_dataset_test.go`):
- `-dataset synthetic|historic|<path>` — the workload (**run both**).
- `-max-batch-size N` — max EVM txs per merged committer tx; `0` = unbounded
  drain-all. Best steady config is 512–1024 (throughput is flat above ~1024).
- `-pipeline` — opt-in warm(N+1)‖auth(N) executor. **Default OFF (serial).**
  The pipeline is unsolved (see [findings.md](findings.md) §6); when testing
  it, the reliability gate is "never livelock at any batch size AND never
  slower than serial", validated across repeated runs, not a single run.
- `-enable-metrics` / `-metrics-addr 0.0.0.0:2112` — export Prometheus metrics
  (matches the `loadgen` scrape job).

> **Fresh stack per config.** Committed state contaminates re-runs even though
> the harness primes balances/nonces. Always `make stop-full clean-x && make
> clean-x init-x start-full` between measured configs.

### GC deployment knob (native, recommended)
Set on the perf process (Go reads these from env, no code change):
```bash
GOGC=500 GOMEMLIMIT=48GiB go test ... # +~7–10% vs default GOGC=100; sized to host
```

---

## 3. Evidence to collect (for EACH dataset)

From the test log:
- **`Replay complete:`** line — `EVM tx/s`, committer txs, avg EVM/batch,
  **rolled-back batches**, submit failures. This is the headline.
- **`Commit-path timing:`** line — committer txs, submit→commit avg/max, peak
  in-flight.
- **`Progress:`** lines — recent vs overall EVM tx/s (watch for `0 tx/s
  (recent)` stalls).

From Grafana (screenshot or note the steady-state values):
- EVM tx/s and committer tx/s (batches) — the two headline throughputs.
- Warm vs auth phase latency (p50/p99), commit latency.
- Rollback rate and spec-version aborts by class (should be flat/zero for
  serial synthetic; non-zero abort classes are the signal when testing the
  pipeline).

Metric names (Prometheus): `loadgen_transaction_committed_total` (EVM tx/s via
`rate(...)`), `loadgen_batch_committed_total` (committer tx/s),
`gateway_warm_phase_seconds`, `gateway_auth_phase_seconds`,
`gateway_commit_latency_seconds`, `gateway_batches_rolled_back_total`,
`gateway_spec_abort_total{class}`.

---

## 4. Batch-size / pipeline sweeps

For a curve across batch sizes (fresh stack per cell, writes to
`$EVM_PERF_DATA/results/`):

```bash
bash scripts/experiments/sweep_batch_pipeline.sh    # serial vs -pipeline across batch sizes
```

Other scratch sweeps under `scripts/experiments/` target specific
investigations (`sweep_qsconns.sh` = QS connection pool, `sweep_nilview.sh` /
`sweep_diag.sh` = the pipeline diagnostic, `measure_qs.sh` = QS/DB resource
sampling, `run_profile.sh` + `analyze_profile.sh` = CPU/mutex/block profiling
via `PERF_PROFILE_DIR`). These are diagnostic tools, not part of the standard
run-book — read the script header before using one.

### HTML report
The report generator renders both datasets separately into a self-contained
offline HTML (inline SVG, no CDN):

```bash
# 1. produce the sweep log (native), then
python3 scripts/report/build_results.py "$EVM_PERF_DATA/results/sweep_batch.log" > "$EVM_PERF_DATA/results/results.json"
python3 scripts/report/gen_report.py    # writes the HTML report (RESULTS embedded)
```

---

## 5. Tear down

```bash
make stop-full clean-x
```

`stop-full` runs `down -v` (removes the container-local named volumes), so the
next `init-x start-full` is a clean chain.

---

## 6. Quick reference — a full both-datasets pass

```bash
export EVM_PERF_DATA="$HOME/workspace/evm-perf-data"
bash scripts/setup.sh
for ds in synthetic historic; do
  make stop-full clean-x >/dev/null 2>&1 || true
  make clean-x init-x start-full
  GOGC=500 GOMEMLIMIT=48GiB go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v -count=1 \
    ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml \
    -dataset "$ds" -max-batch-size 1024 -enable-metrics 2>&1 | tee "$EVM_PERF_DATA/results/replay_$ds.log"
done
make stop-full clean-x
```

Report the `Replay complete:` and `Commit-path timing:` lines for **both**
`synthetic` and `historic`, plus the Grafana steady-state panels.
