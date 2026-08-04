# `config/` — authored configuration

All authored + monitoring configuration for the fabric-x-evm gateway and its
experiment stack lives here. The runtime fabric-x **network fixture**
(crypto material, per-node component configs, network bootstrap) intentionally
stays under `testdata/` — see the note at the bottom.

## Layout

- `compose/` — Docker Compose stacks.
  - `compose.fabric-x.full.yaml` — the full 4-party Arma orderer + committer +
    Prometheus + Grafana stack (`make start-full`).
  - `compose.fabric-x.hostdata.yaml` — overlay binding committer/orderer state
    to `./data` on the host (`make start-full HOST_DATA=1`).
  - `compose.fabric-x.yml` — the lighter stack used by other `make` targets.
  - The Makefile invokes compose with `--project-directory $(CURDIR)`, so every
    `./`-relative volume mount inside these files resolves from the **repo
    root** even though the files live here.

- `gateway/` — gateway configs loaded by the integration/perf test harness.
  - `fabx.yaml`, `fabx-full.yaml`, `fabx-full-ordered.yaml` — fabric-x gateway
    configs. `fablo.yaml`, `fablo.hardhat.yaml` — Fablo-network gateway configs.
  - The harness runs with CWD = `integration/`, so it loads these as
    `../config/gateway/<file>` and the in-file `../testdata/...` paths resolve
    from `integration/` (→ repo-root `testdata/`). Do not rewrite those
    `../testdata` prefixes.

- `monitoring/` — Prometheus + Grafana provisioning mounted into the full stack.
  - `prometheus.yml` — scrape config (arma nodes, committer components,
    query-service, and the `loadgen` job → `host.docker.internal:2112`).
  - `grafana/` — datasource + dashboard provisioning and dashboard JSON
    (`committer.json`, `orderer.json`, `evm-loadgen.json`).

## Why the network fixture stays in `testdata/`

`testdata/` holds the generated fabric-x network fixture — crypto-config, the
`party*`/`committer*` per-node component YAMLs, `config-block.pb.bin`, and the
bootstrap material. It is git-ignored with an allowlist and is mounted into the
containers by path. Moving it would break the container mounts and the
gitignore contract, so it deliberately remains under `testdata/`.
