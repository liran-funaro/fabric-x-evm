<!--
SPDX-License-Identifier: Apache-2.0 AND LGPL-3.0-or-later
-->

# Design: Drop the endorser's internal state DB (read from the Fabric-X query service)

**Status:** approved for planning · **Date:** 2026-07-23 · **Slice:** 1 of the EVM BFT/OEV redesign

## Context

This is the first implementation slice of a larger redesign whose source design document
lives in the `fabric-x-committer` repo, branch `evm-design`
(`evm-design/oev-executor-design.md`). That design has two headline goals — support BFT via
order-execute-validate (OEV), and **remove the endorser's internal state DB**. The full design
decomposes into ~6 slices built one at a time; this spec covers only the state-DB removal, which
is the foundation the EOV and later OEV paths both read through.

### Problem

Today each endorser maintains a **synced local copy of world state** (`VersionedDBWrapper`/SQLite
or `LightKVS`/in-memory), kept current by a `Synchronizer` that follows committed blocks and
applies their write-sets. Simulation reads from that local copy. This copy lags the ledger (the
synchronizer is always catching up), and the staleness both adds latency and inflates the MVCC
abort rate. The design's fix: **drop the local world state entirely** and read committed state on
demand from the Fabric-X **query service**, which reads the committer database directly and is
therefore authoritative and current.

### Decisions locked during brainstorming

- **Replace outright.** Remove the production synced-DB + synchronizer read path; the query
  service becomes the only production read path. (The in-memory backend is *reframed*, not deleted
  — see below.)
- **Two-phase, batch-ready executor now.** Build the warm/authoritative two-phase batch executor in
  this slice. A single EOV transaction uses the warm pass only; the batch/sequential machinery is
  in place for the later bundle/OEV slices.
- **Architecture A — fit the existing seam.** Introduce the query-service read path behind the
  existing `execution.ReadStore` / `execution.KVSSnapshotter` ports so `EVMEngine` and
  `core.Endorser` change minimally, and the new batch executor is directly reusable by the OEV
  slice.

## Requirements

- **RQ1 — No local world state, no state synchronizer.** After this slice, an endorser holds no
  local world-state DB and runs no state-following synchronizer. All simulation reads go through the
  query service (production) or an in-memory query service (tests/embedded).
- **RQ2 — Authoritative committed reads.** Simulation reads the current committed state under a
  consistent (`SERIALIZABLE`) snapshot; the resulting read-set carries per-key versions that MVCC
  validates at commit exactly as today.
- **RQ3 — Two-phase batch execution.** A batch of ordered transactions executes so that a later
  transaction observes the writes of earlier ones, emitting a correct merged read/write-set. A
  single transaction takes a single pass with no extra execution overhead.
- **RQ4 — Minimal disturbance to the working EOV path.** `EVMEngine` / `core.Endorser` and the
  gateway's state-query path change as little as possible; the gateway indexer synchronizer
  (`gwSync`) and `Chain` (SQLite + trie) are untouched.
- **RQ5 — Tests and Hardhat features preserved.** Unit/integration suites and the Hardhat test RPCs
  (`evm_snapshot`, `evm_revert`, balance priming) keep working without a live committer, via an
  in-memory query service.
- **RQ6 — Determinism.** The same ordered batch produces a byte-identical read/write-set (needed by
  endorsement today and by OEV co-signing later).

## Non-goals (later slices)

Ordered execution (CFT/BFT OEV), the committer-ignored OEV envelope, the global ordering counter,
atomic sequential-nonce bundling, the single-account-record state layout, and post-mortem
block/gas/state-root work are **out of scope**. This slice only changes *where and how state is
read* during execution; the state key layout, endorsement flow, submission, and indexer are
unchanged.

## Current architecture (the seams we build on)

- **Read port:** `execution.ReadStore` = `Get(namespace, key) (*blocks.WriteRecord, error)` +
  `Close()`. Snapshot factory: `execution.KVSSnapshotter.NewSnapshot(blockNumber uint64)
  (ReadStore, error)` (blockNumber 0 = latest). `EVMEngine` reads all state through this; the
  gateway's `BalanceAt`/`StorageAt`/`CodeAt`/`NonceAt` route through the endorser, so they inherit
  the change for free.
- **Current impls** (`endorser/storage/`): `VersionedDBWrapper` (SQLite), `LightKVS` /
  `RevertibleLightKVS` (in-memory). A `Synchronizer` applies committed write-sets into the KVS.
- **Composition:** `endorser/app/factory.go` (`NewEndorserCore`/`NewEndorser`) picks the KVS and
  creates the synchronizer. `gateway/app/app.go` wires `endorserSyncs` (per embedded endorser — to
  be removed) separately from `gwSync` (indexer — stays).
- **Enabling fact:** `EVMEngine.Execute` already calls `newExecutor(nil)` → reads latest (block-0
  sentinel) and stubs the EVM block context to constants (`BlockNumber=0`, `Time=1_000_000`). The
  endorser needs no "current block number" to execute; the read-set is per-key versions. Removing
  the synchronizer does not break execution. (`docs/COMPATIBILITY.md` already documents
  `block.number`==0 and state reads == latest.)

### Target: the Fabric-X query service

`committerpb.QueryServiceClient` from `fabric-x-common` (already a direct dependency, v0.2.8;
`api/committerpb/query.proto`):

- `BeginView(ViewParameters{iso_level=SERIALIZABLE default, timeout}) → View{id}` — pins a
  consistent snapshot of current committed state.
- `GetRows(Query{view, namespaces:[{ns_id, keys}]}) → Rows{namespaces:[{ns_id, rows:[{key, value,
  version}]}]}` — reads the requested keys under the view; `Row.version` is the MVCC version.
- `EndView(View{id})` — releases the view.

It runs as `committer-query-service` at `:7001` (mTLS) in **both** composes
(`compose.fabric-x.yml`, `compose.fabric-x.full.yaml`); config
`testdata/config/committer-query-service.yaml` (`max-view-timeout: 10s`, `max-request-keys: 10000`,
`max-active-views: 4096`, server-side read batching `min-batch-keys: 1024` / `max-batch-wait:
100ms`).

## Components & interfaces

New package `endorser/query`, plus a batch executor in `endorser/execution`.

### 1. `QueryClient` interface

```go
type Row struct {
    Key     []byte
    Value   []byte
    Version uint64
}

type QueryClient interface {
    BeginView(ctx context.Context) (viewID string, err error)
    GetRows(ctx context.Context, viewID, ns string, keys [][]byte) ([]Row, error)
    EndView(ctx context.Context, viewID string) error
    Close() error
}
```

Two implementations:

- **`grpcClient`** — wraps `committerpb.QueryServiceClient`; dials the configured endpoint with
  mTLS; uses `SERIALIZABLE` isolation and the configured view timeout. Production.
- **`memClient`** — in-memory, backed by the repurposed `LightKVS` versioned map (retaining its
  history/revert). Tests, embedded mode, and the Hardhat test RPCs.

### 2. `View` — implements `execution.ReadStore`

Holds one `viewID`, the namespace, a `QueryClient`, and an in-view read cache
(`map[string]*blocks.WriteRecord`) guarded by a mutex.

- `Get(ns, key)` → cache hit, else one `GetRows` for that key; caches and returns. Maps
  `Row{value, version}` → `blocks.WriteRecord`; a key with no returned row → nil/zero-version
  record (identical to a `VersionedDB` miss).
- `GetBatch(ns, keys)` → one `GetRows` for many keys (used by the warm pass).
- `Close()` → `EndView`.

### 3. `QueryServiceStore` — implements `execution.KVSSnapshotter`

`NewSnapshot(blockNumber)` opens a view (`BeginView`) and returns a `View`. `blockNumber` collapses
to latest; non-latest handling is covered under Error handling.

### 4. `BatchExecutor` — the two-phase engine (`endorser/execution`)

Input: an ordered `[]*types.Transaction` and a `View`. Behavior:

- **N == 1 (EOV default):** one pass — execute the transaction against the `View` with on-demand
  reads; return its read/write-set. Same round-trip count as today; reads just go remote.
- **N > 1 (bundles/OEV, later slices):**
  - **Warm** — execute each transaction speculatively against the shared `View` in parallel to
    populate its cache concurrently (results discarded; only reads/keys matter).
  - **Authoritative** — re-execute sequentially in fixed order against a layered reader
    (`View` cache + an in-memory write overlay); apply each transaction's write-set to the overlay
    before the next. Emit a merged read-set (view-consistent version union) and write-set
    (last-write-wins).

Small refactor in `execution`: extract "run one transaction against a given `ReadStore`" so the
`BatchExecutor` can inject its layered reader (today `EVMEngine.Execute` always calls
`kvs.NewSnapshot` itself).

## Data flow (EOV transaction)

```
Gateway → endorser.Execute(tx)
  BatchExecutor.Run([tx]):
    view = store.NewSnapshot(0)     // BeginView — SERIALIZABLE snapshot of committed state
    defer view.Close()              // EndView
    stateDB = NewStateDB(view)      // reads route through view.Get → GetRows on demand (cached in-view)
    res = evm.run(tx, stateDB)      // SLOAD / account / code reads hit the query service
    return res.RWS                  // per-key Row.version → MVCC read-set
```

No local DB, no synchronizer, no sync-lag: every endorsement reads authoritative committed state
straight from the committer DB via the query service. MVCC validates the recorded per-key versions
at commit exactly as today.

## Wiring, config, removals

- **Config** (`endorser/config`): add `QueryService{ Endpoint, TLS{...}, ViewTimeout, IsoLevel }`.
  `Database` becomes `"query-service"` (gRPC) | `"memory"` (in-memory query service); `"sqlite"`
  removed.
- **`endorser/app/factory.go`**: build `QueryServiceStore` from a `QueryClient`; `NewEndorser` no
  longer creates a `Synchronizer` (its return signature drops it).
- **`gateway/app/app.go`**: stop creating `endorserSyncs` and their `WaitUntilSynced`; keep
  `gwSync` and `Chain` untouched. Endorsers receive the query-service endpoint from config.
- **Gateway configs** (`integration/fabx*.yaml`): add the `committer-query-service` endpoint + TLS.
- **Removed:** `VersionedDBWrapper` and `state.NewWriteDB` usage; endorser `Synchronizer` creation.
- **Repurposed, not deleted:** `LightKVS` / `RevertibleLightKVS` → backing store of `memClient`
  (preserves tests and Hardhat revert).

## Error handling & edge cases

- **Query service unreachable / `BeginView` / `GetRows` fails** → endorsement returns a retryable
  server error; the gateway's existing endorsement retry (today's sync-lag retry) handles it. No
  new client-facing failure.
- **Absent key** → no row returned → nil/zero-version `WriteRecord`; EVM sees an empty
  account/slot, as now.
- **Version mapping (correctness linchpin, verify early)** → `Row.version uint64` must map to the
  same version encoding that the fabric-x endorsement builder (`efabx`) and committer MVCC expect
  (`blocks.WriteRecord`, the `monotonicVersions` path). Verify against real committer output before
  building on it; a mismatch is a small adapter but must be caught first.
- **View timeout (10s cap)** → EOV execution is sub-millisecond, so a single-tx/small-batch view
  closes well inside the window. Large OEV batches that could approach the cap are handled in the
  OEV slice (chunk/refresh); noted, not built here.
- **View concurrency** → one short-lived view per endorsement; concurrent active views ≈ in-flight
  endorsements, which under `-outstanding 10000` can exceed `max-active-views: 4096`. Foundation:
  one view per endorsement, raise `max-active-views` in the eval config if needed. **Open item:** a
  shared, periodically-refreshed view (one snapshot serving many concurrent endorsements — correct
  under MVCC, trading a small abort-rate rise for far fewer `BeginView` calls) is the likely
  optimization if the perf replay shows view churn.
- **Historical-height state reads** (`eth_getBalance` / `eth_getStorageAt` / `eth_call` at a past
  block) → the query service serves only latest committed; `VersionedDB` could answer these today,
  so this is a narrow regression. **Proposal:** non-latest state requests resolve to latest, with a
  `docs/COMPATIBILITY.md` note consistent with the doc's existing stance (`block.number`==0,
  `safe`/`finalized`==latest). Flagged for confirmation at spec review.

## Testing

- **Unit:** `memClient` view semantics + versioning; `View` cache dedupe + absent-key; `BatchExecutor`
  — N=1 single-pass RWS, N>1 warm+authoritative (later tx sees earlier writes, merged RWS
  last-write-wins, read-set = view-consistent union), determinism (same batch → byte-identical RWS).
- **Integration:** in-process harness (`integration/test_helpers.go`) rewired onto the
  `memClient`-backed store with no synchronizer; existing endorser/gateway suites pass; Hardhat
  `evm_snapshot`/`evm_revert`/balance-priming pass via `memClient` history.
- **Perf (acceptance bar):** `TestReplayJSONDataset` on the full network against the real query
  service (`:7001`), both datasets. Conflict-free: throughput ≈ goodput; high-conflict: no worse
  than the synced-DB baseline. Sanity-check that latency has no sync-lag component.

## Success criteria

1. Endorser holds no local world-state DB and runs no state synchronizer; all reads go through the
   query service (production) or the in-memory query service (tests).
2. `TestReplayJSONDataset` passes on the full network for both datasets.
3. Existing unit / integration / Hardhat suites pass on the query-backed store.
4. Two-phase `BatchExecutor` present and unit-tested; the EOV path runs N=1.

## Open items (surface at spec review)

- Confirm the historical-height read policy (resolve-to-latest + COMPATIBILITY note).
- Confirm the `Database` config values (`"query-service"` | `"memory"`; drop `"sqlite"`).
- Whether to raise `max-active-views` in the eval config now, or defer the shared-view optimization
  until the perf replay shows it is needed.
