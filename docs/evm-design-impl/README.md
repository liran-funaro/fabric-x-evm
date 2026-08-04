# EVM-on-Fabric-X — Implementation Docs

Durable, tracked documentation for **implementing** the EVM-on-Fabric-X BFT
redesign: findings, the experiment run-book, server setup, and the plans/specs
the work followed.

> ⚠️ **`evm-design/` is the user's design document and is READ-ONLY for
> implementers.** Never modify anything under `evm-design/`. All
> implementation notes, findings, plans, specs, and reports go **here**, under
> `docs/evm-design-impl/`.

## Contents

| File / dir | What it is |
|------------|------------|
| [findings.md](findings.md) | **Start here.** Where the throughput went, what moved it, and the one unsolved problem (the warm/auth pipeline trilemma). |
| [experiments.md](experiments.md) | Run-book: how to reproduce the numbers and collect evidence — **both datasets, always**. |
| [server-setup.md](server-setup.md) | Provision a native experiment host (ask which server; install docker + go; the RHEL reboot gotcha) and sync local → remote. |
| [specs/](specs/) | Brainstormed designs: drop-internal-state-DB, two-phase execution, perf harness, pipelined-cache, warm/auth pipelining. |
| [plans/](plans/) | Task-by-task implementation plans those specs became. |
| [reports/](reports/) | SDD outcome reports: [exec-hotpath optimization](reports/2026-07-exec-hotpath-optimization.md), [stage-2.1 final fix](reports/2026-07-stage2.1-final-fix-report.md), [pipelined-cache slice](reports/2026-07-pipelined-cache-slice.md) (single-submitter invariant, MVCC cascade, receipt fix). |

## Repository layout (post-reorg, for a new agent)

- **`config/`** — all authored + monitoring config: `config/compose/` (compose
  files), `config/gateway/` (gateway configs, referenced as
  `../config/gateway/<file>`), `config/monitoring/` (Prometheus + Grafana). The
  fabric-x **network fixture** (crypto, party/committer configs, config-block,
  `shared_config.yaml`) intentionally stays in `testdata/` (container-mounted,
  gitignore-allowlisted).
- **`scripts/`** — all scripts: `scripts/setup.sh` (fetch datasets),
  `scripts/setup-server.sh` (provision a host), `scripts/experiments/` (sweeps
  + diagnostics), `scripts/report/` (HTML report generator).
- **`~/workspace/evm-perf-data`** (`$EVM_PERF_DATA`) — the out-of-tree data
  in/out dir: input datasets AND `results/` output. A repo sync never touches
  it.
- **`docs/evm-design-impl/`** — this folder.
- **`evm-design/`** — the user's read-only design doc. Do not modify.

## Current status (as of 2026-08-04)

- **Serial two-phase executor: healthy and shipped (default).** **~4.9k–6.0k
  EVM tx/s** on native ec2 at bs 512–1024 (latest verified run: synthetic 4872 /
  historic 5617 @ 50k window, GC knob), 0 rolled back on both datasets. This is
  the production path. (The earlier ~3.8k/~4.2k figure was superseded by the
  warm-workers-restore + QS-batching lift, `54faa4f` — see findings.md §2.)
- **Committed optimizations** (branch `bft-redesign`, **not pushed**):
  endorser→QS connection pool (`6221599`), code-hash cache (`58a0df8`),
  read-only MFU cache (`1ec2cd7`), `RequestMaxBytes` liveness fix (`92d9ba8`),
  QS rate-limit disabled (`3ca917a`), warm-concurrency + QS low-latency batching
  (`54faa4f`), plus the repo reorg + monitoring extensions.
- **Pipelined-cache slice** (`Gateway.Pipelined`, opt-in): the
  execution-ahead-of-commit path — single-submitter invariant, MVCC-abort
  cascade + cache rebuild, receipt-index fix. See findings.md §9 and the
  [pipelined-cache report](reports/2026-07-pipelined-cache-slice.md).
- **Warm/auth pipeline (`-pipeline`, default OFF): UNSOLVED.** A trilemma; five
  fixes failed the same bistable livelock. The decisive v2 diagnostic
  (`common/pipediag`) **refuted** the stale-read mechanism all five targeted —
  the residual conflict is a speculative **over-read** (spec-version
  chain-misprediction), so the fix direction is the speculation/chaining layer,
  **opposite** of every prior attempt. Awaiting a user decision on fix
  direction — **do not build a 6th fix unilaterally.** Full detail in
  [findings.md](findings.md) §6 and memory note `evm-warm-auth-pipeline-findings`.

## Roadmap / open levers (need user go-ahead)

1. **Resolve the pipeline** along the over-read/chaining direction — most
   promising: guarantee commit-order == submission-order (single-submitter
   invariant, commits `54faa4f`/`73bed3d`/`9ea5470`/`2d51b2d`), or per-key
   visibility-gated eviction on top of nil-view (`EVM_QS_NIL_VIEW`, a keeper).
2. **Execution-ahead-of-commit / overlay carry-forward** — the durable code
   lever that decouples execution cadence from BFT commit latency; a real
   speculative redesign.
3. **OEV/BFT slice** — the next design slice (executor quorum, M-of-N co-sign,
   global ordering counter). Fix the dormant `overlayReader.Get`
   delete-then-read version bug when N>1 batches get exercised.

## Related memory notes

`evm-bft-redesign-project`, `evm-current-state-read-architecture`,
`evm-perf-stall-findings`, `evm-lock-profiling-findings`,
`evm-qs-db-resource-discovery`, `evm-post-pool-bottleneck-shift`,
`evm-warm-auth-pipeline-findings`, `evm-versioned-cache-rpc-race`.
