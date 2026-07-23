<!--
SPDX-License-Identifier: Apache-2.0 AND LGPL-3.0-or-later
-->

# Design: Two-phase batch execution (drain-all + merged-batch commit)

**Status:** approved for planning · **Date:** 2026-07-24 · **Builds on:** the "drop the endorser
internal state DB" slice (endorsers read committed state from the Fabric-X query service).

## Context

The previous slice made the endorser read world state on demand from the Fabric-X query service.
A full-network perf replay (`TestReplayJSONDataset`) then showed the gateway's throughput is bound
by **endorsement concurrency**, not ordering: with the default 20 gateway workers it commits ~14
tx/s; with 200 workers ~127 tx/s; changing the orderer submitter count 64→8 changed nothing. Each
endorsement now issues several sequential query-service reads (`BeginView` + per-key `GetRows` +
`EndView`), and the query service is tuned for bulk reads (`max-batch-wait: 100ms`,
`min-batch-keys: 1024`), so at low concurrency each small read waits ~100ms and batches never fill.
A fixed worker pool leaves that latency exposed.

This design replaces the gateway's fixed worker pool + dependency-aware queue with a **two-phase,
drain-all batch execution model** that (a) runs *all* pending transactions' reads concurrently so
the query service batches them efficiently, and (b) commits each batch as a single atomic Fabric
transaction so the committer does one MVCC check + commit for many EVM transactions. It realizes the
CFT execution model from the EVM-on-Fabric-X design doc (single trusted gateway owns the order;
two-phase warm/authoritative execution; merged write-set per batch).

### Decisions locked during brainstorming

- **Drain-all batching.** A single executor loop; each cycle takes *all* currently-pending TXs as
  one batch. Phase-1 concurrency = batch size; batch size self-tunes to load.
- **Merged-batch submission.** The whole batch commits as **one atomic Fabric transaction** (merged
  read/write-set), not N independent txs.
- **Pipeline via in-memory overlay.** A later batch *executes* against earlier submitted-but-not-yet-
  committed batches' writes via a gateway-held overlay (predicted value + version), avoiding a commit
  stall.
- **Wait-for-ordered commit sequencing (no counter).** Submit batch N, wait until it appears in a
  received *ordered* block, then submit N+1. Ordering is pinned by the orderer at submission time,
  so N commits before N+1 and the overlay's predicted read-versions validate. No global counter.
- **Gateway drives the loop; the (in-process) endorser executes + signs.** The gateway owns the
  drain-all loop, the overlay, and submission; it calls the endorser to two-phase-execute a batch
  against committed-state + the passed-in overlay and return one signed merged read/write-set,
  reusing the endorser EVM engine and the existing `BatchExecutor`.
- **No workers, no dependency detection.** The fixed worker pool (`WorkerCount`) and `TxQueueV2`'s
  read/write dependency graph are removed.

## Requirements

- **RQ1 — No fixed worker pool.** Execution concurrency is per-batch (phase-1 runs one goroutine per
  transaction in the batch), not a fixed count. The `WorkerCount` knob and worker-pool are removed.
- **RQ2 — No dependency detection / pre-ordering.** `TxQueueV2`'s read/write dependency graph and
  same-participant serialization are removed. A plain pending pool replaces the queue.
- **RQ3 — Two-phase batch execution.** Each batch runs a parallel warm pass (all TXs, reads only) to
  populate one query-service view, then a serial authoritative pass producing one merged
  read/write-set in which a later transaction observes earlier ones' writes.
- **RQ4 — Merged-batch commit.** A batch commits as a single atomic Fabric transaction; its
  constituent EVM transactions are individually addressable as `(block, index, sub-index)` with their
  own receipts.
- **RQ5 — Pipelined, ordered commits.** Batches commit in submission order (wait-for-ordered), and a
  later batch executes against earlier in-flight batches via a predicted value+version overlay
  without waiting for their commit.
- **RQ6 — Correct nonce handling without pre-ordering.** A transaction whose nonce exceeds the
  sender's current (committed + overlay) nonce is excluded from the batch and retried on a later
  cycle; nonce-too-low is rejected at submission.
- **RQ7 — Throughput scales with batch size.** Under load, throughput is one merged commit per drain
  cycle, so it rises with offered load rather than being pinned by a worker count.

## Non-goals

- BFT / trustless ordering, the global ordering counter, and the committer-ignored OEV envelope
  (this design is CFT: a single trusted gateway owns the order).
- Contention-based routing / demotion between optimistic and ordered modes.
- Changing the endorser's query-service read path or the state key layout.

## Current flow (what changes)

`SendTransaction` validates a tx and `Enqueue`s it into `TxQueue`/`TxQueueV2`; a fixed pool of
`WorkerCount` workers each `Dequeue` one tx (respecting `TxQueueV2`'s dependency ordering), endorse
it individually via the endorser, and submit it through the `BatchSubmitter` as its own Fabric tx.
This design removes the worker pool and the dependency queue and replaces per-tx endorse+submit with
per-batch two-phase execute + merged commit.

## Target architecture

### Components

- **Pending pool** (`gateway/core`): a set of accepted-but-not-committed transactions. `SendTransaction`
  runs `ValidateTx` and adds to it. No dependency graph, no per-tx dequeue ordering. Entries are
  removed when their batch commits; entries excluded from a batch (nonce gap) or belonging to an
  aborted batch remain for the next cycle.
- **Drain-all executor** (`gateway/core`, single goroutine, replaces the worker pool): each cycle
  snapshots the whole pending pool as the batch, drives execution + submission, and advances the
  overlay.
- **Batch execution** (endorser, reusing `execution.BatchExecutor`): the gateway calls a new endorser
  batch API that takes the batch's TXs and the current overlay, runs phase-1 (parallel warm over the
  query-service view + overlay) and phase-2 (serial authoritative over the warm cache + overlay),
  and returns **one signed merged read/write-set** (union of reads at view/overlay-consistent
  versions; writes last-write-wins). Nonce-gap / invalid TXs are reported back as excluded.
- **In-flight overlay** (`gateway/core`, gateway-owned): for each key written by a submitted-but-not-
  committed batch, the predicted `{value, version}` (committed version + count of in-flight writes to
  that key). Batches execute against committed-state layered with this overlay. A batch's entries are
  dropped when it commits.
- **Merged submission**: the gateway assembles the signed merged read/write-set into one Fabric tx,
  submits it, appends its writes to the overlay, and waits until it appears in a received ordered
  block before submitting the next.
- **Indexer** (`gateway/core` Chain + storage): maps each EVM tx in a merged Fabric tx to
  `(block, index, sub-index)` and serves per-tx receipts / hash lookups.
- **Removed**: `WorkerCount` and the worker pool (`Gateway.worker`/`Start` loop); `TxQueueV2` and its
  dependency detection; the "workers" concept in configuration and metrics.

### Data flow (one drain cycle)

```
batch = drain(pending pool)                       // all currently-pending TXs
merged = endorser.ExecuteBatch(batch, overlay)    // phase-1 parallel warm; phase-2 serial authoritative
                                                  //   -> one signed merged RWS + list of excluded TXs
submit merged as one Fabric tx
overlay.add(merged.writes)                         // predicted value+version for in-flight keys
wait until the Fabric tx appears in a received ordered block
loop
--- asynchronously, on each committed block ---
for each committed batch: overlay.drop(batch); pending.remove(batch.included); indexer.record(receipts)
```

### Overlay and version prediction

The overlay records, per key, the value the latest in-flight batch wrote and the **predicted
committed version** = the key's committed version plus the number of in-flight batches that write it.
Phase-1/phase-2 reads consult committed-state (query service) then the overlay; an overlaid key
returns the predicted value and version. Because wait-for-ordered guarantees batches commit in
submission order, these predictions hold: batch N+1's merged read-set references the versions batch N
produces, and validates once N commits (which precedes N+1).

## Error handling & edge cases

- **Nonce-too-high (gap):** the transaction cannot execute in phase-2 against current state → excluded
  from the merged batch, left in the pending pool, retried on a later cycle once the predecessor
  nonce fills. **Nonce-too-low / bad signature:** rejected at `SendTransaction` (`ValidateTx`), never
  enters the pool.
- **Revert:** a committed outcome — included in the merged batch (nonce increment + gas), recorded
  with status 0 in its receipt.
- **Merged-tx MVCC abort** (external writer or an unexpected reorder despite wait-for-ordered): the
  batch's included TXs return to the pending pool and its overlay entry is discarded; they are
  re-drained next cycle. Expected to be rare under a single trusted gateway.
- **Overlay bound:** the overlay only holds keys of batches that are submitted but not yet committed;
  wait-for-ordered keeps that window shallow, bounding overlay size.

## Staged implementation

Each stage is a separate spec → plan → implementation cycle; each leaves the system correct.

- **Stage 2.1 — drain-all + two-phase + merged-batch, wait-for-commit (no overlay).** Replace the
  worker pool + `TxQueueV2` with the drain-all loop; execute each batch two-phase; commit one merged
  Fabric tx per batch; **wait for the batch to commit** before draining the next (batch N+1 reads
  committed state — no overlay, no version prediction). Delivers RQ1–RQ4, RQ6, RQ7. Simplest correct
  core and the stepping stone.
- **Stage 2.2 — merged-batch indexer.** `(block, index, sub-index)` addressing and per-tx receipts /
  hash lookups over merged Fabric txs. Required as soon as 2.1 lands, since merging breaks per-tx
  lookup; lands with or immediately after 2.1.
- **Stage 2.3 — pipeline via overlay + wait-for-ordered.** Add the predicted value+version overlay
  and switch commit-sequencing from wait-for-commit to wait-for-ordered, so a later batch executes
  during the predecessor's ordering latency. Delivers RQ5 (the throughput optimization).

## Testing

- **Unit:** pending-pool add/drain/remove and nonce-gap retention; overlay predicted-version layering
  (a read of an overlaid key returns predicted value + version); merged-RWS correctness (union reads
  at consistent versions, last-write-wins, same-sender nonce order within a batch); nonce-gap
  exclusion.
- **Integration (in-process harness):** a batch mixing independent and same-sender TXs commits as one
  Fabric tx; each EVM tx resolves via `(block, index, sub-index)` with a correct receipt;
  `eth_getTransactionReceipt` / `eth_getTransactionByHash` work; a nonce-gap tx is deferred then
  commits once filled.
- **Perf (acceptance):** `TestReplayJSONDataset` on the full network — throughput rises with offered
  load (one merged commit per cycle) rather than being pinned by a worker count; 0 failed.

## Open items

- **Wait-for-ordered signal (2.3):** confirm the gateway can observe a tx in an *ordered* block
  (from the orderer delivery stream) distinctly from a *committed* block; if only committed blocks
  are observable, 2.3 falls back to wait-for-commit with the overlay bridging the ordered→committed
  gap, or a lighter ordered-delivery subscription is added.
- **Merged endorsement signing:** the endorser signs one merged read/write-set per batch (CFT: one
  signer meets the policy); confirm the endorsement builder can sign a synthetically-merged RWS.
- **Overlay↔commit reconciliation detail (2.3):** exact bookkeeping for dropping a batch's overlay
  entries and decrementing predicted versions as batches commit in order.
