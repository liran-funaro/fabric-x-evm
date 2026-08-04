# Perf harness refactor + drain-all batch-size bound (stage 2.1 perf)

Date: 2026-07-26

## Context

Stage 2.1 replaced the gateway's worker pool + dependency queue with a single
**drain-all two-phase merged-batch executor** (one committer/Fabric tx per
batch, many EVM txs each). This note covers the perf-harness changes needed to
measure that model, and one bound the model needs to be safe under bursty load.

## What changed

### 1. Perf harness now reports EVM tx/s and fires without a flow-control cap
`integration/perf/replay_json_dataset_test.go`:

- **Dropped `-outstanding`.** The outstanding-tx flow-control semaphore existed
  to mitigate exactly the state-conflict/ordering problem the drain-all merged
  batch removes. The harness now *fires every tx* at the gateway (bounded work
  channel for feeder→submitter backpressure only) and lets the pending pool
  absorb the backlog — the model the redesign targets.
- **Dropped `-workers`.** There is no per-tx gateway worker pool anymore; the
  executor drains the whole pool into one merged batch per cycle regardless of
  arrival concurrency. `-submitters` (goroutines calling `SendTransaction`) and
  `-orderers` (BatchSubmitter workers) remain.
- **Throughput is EVM tx/s, not committer tx/s.** One committer tx now merges
  many EVM txs, so each committed-batch notification is credited with its
  committed sub-tx count via the new `gwcore.CountCommittedSubTxs(notif.Events)`
  (decodes the batch's `[]PerTxOutcome`, counts the non-excluded ones). The
  notification service itself is unchanged — still one notification per
  committer tx, used by the gateway for commit-tracking/rollback.
- **Completion / stall handling.** The run ends when every fired tx is
  accounted for (`committedEVM + submitFailed >= totalToSubmit`), or after a
  60 s no-progress stall guard, or the 4 h context — so a permanently-stuck tx
  can't hang the harness.
- **Bounded default load.** `TestReplayJSONDataset` (what `test.sh` runs) now
  fires a single 50 000-tx pass by default instead of the old infinite
  1 000 000×1 000 000 wrap (which relied on the `-outstanding` cap to bound
  in-flight txs; with fire-all it would balloon the pending pool). Scale via
  `PERF_REPLAY_WINDOW_SIZE` (0 = whole 400 k dataset) and `PERF_REPLAY_WRAP_COUNT`.

### 2. Drain-all batch-size bound (`Gateway.SetMaxBatchSize` / `PendingPool.DrainUpTo`)
`gateway/core/{pending,api,executor}.go`:

- `PendingPool.DrainUpTo(max)` returns up to `max` pending txs FIFO (all when
  `max <= 0`); `DrainAll` == `DrainUpTo(0)`.
- `Gateway.maxBatchSize` (atomic; `SetMaxBatchSize(n)`) bounds how many txs one
  drain cycle folds into a single merged committer tx. **Default 0 (unbounded)**
  preserves the committed drain-all design for production.
- **Why it's needed:** under fire-all (or any burst that outpaces the drain
  cycle), an unbounded `DrainAll` folds the whole backlog into one merged Fabric
  tx, which exceeds the orderer's max message size and/or is pathological for
  latency/memory. A positive bound pipelines the burst across right-sized
  batches; the remainder is picked up by the next cycle (which runs immediately
  after the current batch commits — no stall). The perf harness sets it via
  `-max-batch-size` (default 1024) and it can be swept to find the
  throughput/latency sweet spot.

Tests: `TestPendingPoolDrainUpTo`, `TestExecutorDrainCapBoundsBatch` (both green
under `-race`), plus all pre-existing gateway/endorser/integration suites.

## How to run (requires docker — the committer/orderer/query-service stack)

```
./test.sh
# or, to sweep batch size / submitters:
go test -timeout 4h -tags=perf -run '^TestReplayJSONDataset$' -v -count=1 \
  ./integration/perf/... -gateway-config fabx-full.yaml -enable-metrics \
  -max-batch-size 2048 -submitters 16
```

Watch, per 2 s progress line: **EVM tx/s (overall)**, `avg EVM/batch` (should sit
near `-max-batch-size` under load — if far below, submission is the bottleneck,
not the executor), `in-flight` (pending-pool depth), and `rolled back` (MVCC
aborts; should be ~0 in the single-gateway CFT model).

## Tuning `-max-batch-size`

- Larger batches amortize the per-batch Fabric round-trip (higher throughput)
  but cost latency, memory, and risk the orderer message-size limit.
- Sweep e.g. `256, 1024, 4096, 16384` and plot EVM tx/s. Pick the largest that
  stays safely under the orderer's configured max message size and doesn't
  regress commit latency. Promote the winner to a production default (wire it in
  `gateway/app` config) once measured.

## NOT measured in this session

Docker was unavailable in the authoring environment, so `test.sh` was **not**
run and **no throughput numbers were collected**. The harness compiles, all unit
/ integration suites pass under `-race`, and the batch-size bound is unit-tested;
the live measurement + batch-size sweep is the next step for a docker-capable
environment.
