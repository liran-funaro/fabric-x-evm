# Pipelined Execution over a Bounded Cross-Batch Cache

**Date:** 2026-07-27
**Status:** Approved (design), pending implementation plan
**Branch:** `bft-redesign`
**Prior art:** [drop-internal-state-db](2026-07-23-drop-internal-state-db-design.md), [two-phase-batch-execution](2026-07-24-two-phase-batch-execution-design.md)

## Problem

The stage-2.1 gateway executor is **serial and wait-for-commit**: it drains a batch,
two-phase-executes it, submits one merged committer tx, then **blocks in `awaitCommit`**
until that tx's commit notification arrives before starting the next batch. Live
`./test.sh` runs show throughput is *commit-bound* at ~80 EVM tx/s, and a single
missing/delayed commit notification deadlocks the whole pipeline (the orderer then
starves — observed deterministically at ~56 committer txs / orderer block 58).

Two independent problems:

1. **Latency-bound throughput.** We wait a full orderer→committer→notification round
   trip (~150 ms, degrading) per batch. Execution itself is now ~200K+ tx/s (see the
   execution-optimization gate below), so the commit wait is the entire bottleneck.
2. **Zero resilience to a lost notification.** One dropped confirmation wedges the
   serial pipeline; the 60 s timeout then *rolls back an already-committed batch*.

## Goal

Pipeline batches so the executor never blocks on a commit, using a **bounded
cross-batch write cache** so a later batch can execute on an earlier in-flight batch's
uncommitted writes. Switch commit tracking to a **per-TxID notification** stream with a
**query-service fallback**. Target: remove the commit-round-trip bottleneck (~80 tx/s)
so throughput is bound by execution and warm-pass reads instead. The CPU execution
ceiling is now ~200K+ tx/s (gate below); the *realized* rate will be lower, bound by
query-service warm-pass read throughput for cold keys — but no longer by commit latency.

### Pre-gate (COMPLETE)

Before this redesign, per-tx execution had to clear **>100K tx/s** so the pipeline is
not merely trading one bottleneck for another. Done and verified (commits `12e17ce`..
`a5f9ed4`): `Execute_1` **456K tx/s**, `ExecuteBatch_128` **221K tx/s** (both `-race`
clean). This design assumes that optimized two-phase executor unchanged in behavior.

## Design

### 1. `VersionedCache` — a bounded cross-batch write cache

A concurrency-safe store, keyed by KVS key, holding `{ value, specVersion uint64,
writerTxID string }`. It is **deliberately bounded** — it never holds all committed
state:

- **In-flight writes.** Every write from a submitted-but-not-yet-committed batch,
  tagged with that batch's committer-TxID, at `specVersion` (below). Persist until the
  TxID commits, then evicted.
- **This batch's warmed reads.** Populated by the warm pass, **evicted at end of batch**.

A cold key is therefore *re-fetched by the warm pass* each batch; only uncommitted
writes ride across batches (that is exactly what lets batch N+1 execute on batch N
before N commits). Cache size ≈ (in-flight batches × writes/batch) + (one batch's
reads) — bounded by the in-flight window.

**Speculative versions.** Fabric-X assigns per-key monotonic versions with a
deterministic **+1 per write** (confirmed in `storage.LightKVS.Update`:
`nextVersion = existing.Version + 1`). So the cache can predict the version a key will
have once its in-flight writer commits:

- On `ApplyWrites(txID, rws)`: for each written key, mirror `LightKVS.Update` exactly —
  `specVersion = cachedSpec+1` if the key is already in the cache; else, from the
  committed base the warm pass read, `committedVersion+1` if the key existed, or **`0`
  if it was absent** (first write of a never-before-written key gets version 0, not 1).
  Store `{value, specVersion, txID}`.
- A read served from the cache records `specVersion` in the MVCC read-set. Because
  increments are deterministic and (in the current CFT model) the gateway is the sole
  writer, that version **matches at commit** — so the committer accepts it.

**Eviction (batch boundary only).** Commit notifications are **accumulated** and applied
between batches (never mid-batch, so the cache is stable during execution):

- read-only warmed keys → evicted at end of batch.
- write keys → evicted when a notification confirms their `writerTxID` committed (their
  spec version is now the real committed version; future reads go to the query service
  and get the same value+version).

### 2. Two-phase execution stays (the cache is bounded)

Because the cache cannot hold all state, the **warm pass is retained** to refetch cold
read-keys in parallel. Per batch: warm pass (bounded worker pool) reads cold keys into
the cache — in-flight writes are cache hits and skip the query service; authoritative
pass (serial) executes against the warm cache + in-batch overlay → merged RWS. Reads of
in-flight writes record `specVersion`; cold reads record the real committed version.
This is the already-optimized `ExecuteBatch`; this design adds the cross-batch cache as
the reader beneath the per-batch overlay.

### 3. Pipelined executor (no `awaitCommit`)

`executeCycle` stops blocking on commit. Per cycle: drain (up to `maxBatchSize`) →
two-phase execute against `VersionedCache` → `ApplyWrites(txID)` → **register TxID with
the notifier, then submit** → start the next cycle immediately. A **bounded in-flight
window** (default **16**, configurable) applies backpressure: when 16 batches are
awaiting confirmation, the executor waits for one to resolve before draining the next.
Commit confirmations arrive asynchronously and only drive eviction; the pipeline never
waits on them.

### 4. Per-TxID notifications, fallback, and cascade

- **`Notifier`** (`notification.Notifier`, `OpenNotificationStream`): push each batch's
  committer-TxID onto the watch channel and receive its `TxStatusEvent`. Drop
  `AllTxStreamer`. **Register-then-submit**: the TxID is added to the notifier *before*
  the committer tx is submitted to the orderer, or a fast commit could be missed.
- **Committed** → queue the TxID for eviction at the next batch boundary.
- **Timeout** (no event within T) → **query-service fallback**: read the batch's written
  keys; if their versions reached the predicted spec versions, treat as committed;
  else invalidated.
- **Invalidated** → **cascade**: re-execute the first invalidated batch and all later
  in-flight batches from corrected state (batches are totally ordered, so re-run the
  suffix). In the CFT sole-writer model with deterministic versions this does not fire
  in the happy path; it is the correctness backstop (and the path that generalizes to
  multi-writer/BFT).

## Components and boundaries

| Unit | Responsibility | Depends on |
|---|---|---|
| `VersionedCache` | bounded {value, specVersion, writerTxID} store; Read / ApplyWrites / Evict; batch-boundary eviction queue | — |
| cache-backed `ReadStore` | serve reads from cache, fall through to the query-service snapshot on miss, record spec vs committed version | `VersionedCache`, query `Store` |
| pipelined `executeCycle` | drain → execute → ApplyWrites → register+submit → advance; in-flight window; cascade on invalidation | executor, `VersionedCache`, notifier |
| notifier adapter | per-TxID subscribe (register-then-submit); deliver committed/invalidated/timeout | `notification.Notifier`, query `Store` (fallback) |

## Correctness

- **MVCC:** cache reads record deterministic `specVersion`; cold reads record the
  committed version; both validate at commit (sole writer, +1 increments).
- **Ordering:** batches are totally ordered by the executor; the overlay handles
  in-batch same-sender sequencing; the cross-batch cache handles cross-batch.
- **Invalidation:** suffix cascade re-execution restores correctness if the committer
  ever rejects a batch; query-service fallback prevents a lost notification from either
  wedging the pipeline or rolling back a committed batch.
- **Eviction only at batch boundaries** keeps the cache immutable during a batch's
  execution (no read/evict races).

## Configuration

- **in-flight window cap** — default 16, configurable.
- **notification timeout** T before query-service fallback — configurable
  (reuse/replace today's 60 s commit backstop).
- **`WarmWorkers`** (already added) — warm-pass concurrency.

## Out of scope

- Multi-writer / BFT quorum (the cascade path is built with it in mind, but this slice
  stays CFT single-writer).
- Further per-tx execution micro-optimization beyond the >100K gate (irreducible geth
  cost remains; not pursued here).
- Read batching in the warm pass (coalescing per-key `GetRows` into multi-key calls) —
  a separate query-path optimization.

## Testing

- Unit: `VersionedCache` spec-version math + batch-boundary eviction; cache-backed
  reader hit/miss + version recording; cascade suffix re-execution; notifier
  register-then-submit ordering; query-service fallback on timeout.
- Integration: pipelined multi-batch commit with per-tx receipts (extend
  `TestBatchMergedCommit`); a forced-invalidation cascade; a dropped-notification →
  fallback path. `-race` on the executor + cache.
- Perf: `./test.sh` — expect execution-bound throughput (no commit-cliff stall);
  the flow micro-benchmarks remain the per-component gate.
