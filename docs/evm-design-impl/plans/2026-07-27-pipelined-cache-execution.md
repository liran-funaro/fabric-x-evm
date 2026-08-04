# Pipelined Execution over a Bounded Cross-Batch Cache — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pipeline batch execution so the gateway never blocks on a commit, using a bounded cross-batch write cache, per-TxID notifications with a query-service fallback, and suffix cascade re-execution on invalidation.

**Architecture:** A shared `VersionedCache` holds every in-flight (submitted-but-uncommitted) batch's writes, tagged with its committer-TxID and a deterministic speculative MVCC version. The endorser engine reads through a `CachedView` (cache first, then the per-batch query-service view). The gateway executor drains → two-phase-executes → `ApplyWrites` to the cache → registers the TxID with a per-TxID notifier → submits → advances immediately, bounded by an in-flight window. Commit notifications evict cache entries at batch boundaries; timeouts fall back to a query-service status check; invalidation cascades a suffix re-execution.

**Tech Stack:** Go, go-ethereum EVM, Fabric-X SDK (`notification.Notifier`, `blocks`, query `committerpb`).

## Global Constraints

- CFT single-writer model (one gateway is the sole writer of every key). Copied from spec.
- Fabric-X per-key MVCC versions increment deterministically **+1 per write**; first write of an absent key is version **0** (mirror `endorser/storage.LightKVS.Update`).
- Preserve exactly: MVCC read-set/version recording, the deferred read-error abort path (`StateDB.setError`/`Error()` → `Executor.Send`/`ApplyMessage`), revert(201)/`ExecFailure`(460) classification, `MergeResults` (union reads / last-write-wins), and the optimized reusable-executor fast path.
- Eviction and cache mutation happen **only at batch boundaries** (never mid-batch) — the cache is immutable during a batch's execution.
- Register a committer-TxID with the notifier **before** submitting it to the orderer.
- License header on every new file (`Copyright IBM Corp. All Rights Reserved.` / `SPDX-License-Identifier: LGPL-3.0-or-later`).
- All work stays green: `go build ./...`, `go vet ./gateway/... ./endorser/...`, and the covering suites under `-race`.

---

## File Structure

- **Create** `gateway/core/versioned_cache.go` — `VersionedCache`: in-flight write store, spec-version math, batch-boundary eviction. (Task 1)
- **Create** `gateway/core/versioned_cache_test.go` — unit tests. (Task 1)
- **Create** `gateway/core/cached_view.go` — `CachedSnapshotter`/`cachedView`: `execution.KVSSnapshotter` that reads the cache first, then a wrapped snapshotter. (Task 2)
- **Create** `gateway/core/cached_view_test.go` — unit tests. (Task 2)
- **Modify** `endorser/app/factory.go` — build the engine over the cache-backed snapshotter; return the shared cache. (Task 3)
- **Modify** `gateway/app/wiring.go`, `gateway/app/app.go`, `integration/test_helpers.go` — thread the shared cache from endorser build to the gateway. (Task 3)
- **Modify** `gateway/core/api.go` — hold the `*VersionedCache`, the in-flight window, and a per-TxID notifier handle. (Tasks 4, 6)
- **Modify** `gateway/core/executor.go` — pipelined `executeCycle` (ApplyWrites + register + submit, no `awaitCommit`), in-flight window, batch-boundary eviction drain, cascade. (Tasks 4, 5, 7)
- **Create** `gateway/core/notifier.go` — per-TxID notifier adapter (register-then-submit; committed/invalidated/timeout via `notification.Notifier` + query-service fallback). (Task 6)
- **Create** `gateway/core/notifier_test.go` — unit tests. (Task 6)
- **Modify** `integration/test_helpers.go` — wire `notification.Notifier` instead of `AllTxStreamer` for the pipelined path; extend `TestBatchMergedCommit`. (Tasks 6, 8)

---

## Phase 1 — The cache and the cache-backed reader (behavior-neutral)

### Task 1: `VersionedCache`

**Files:**
- Create: `gateway/core/versioned_cache.go`
- Test: `gateway/core/versioned_cache_test.go`

**Interfaces:**
- Produces:
  - `type VersionedCache struct{ ... }`, `func NewVersionedCache() *VersionedCache`
  - `func (c *VersionedCache) Read(key string) (*blocks.WriteRecord, bool)` — in-flight write record (Version = specVersion) if present.
  - `func (c *VersionedCache) ApplyWrites(txID string, rws blocks.ReadWriteSet)` — record each write with a deterministic spec version; base version for a first-seen key comes from `rws.Reads` (else absent⇒0).
  - `func (c *VersionedCache) NoteCommitted(txID string)` — queue a committed TxID for eviction.
  - `func (c *VersionedCache) NoteInvalidated(txID string)` — queue an invalidated TxID (its writes drop without becoming committed).
  - `func (c *VersionedCache) DrainEvictions() (committed, invalidated []string)` — apply all queued evictions (drop cache entries whose `writerTxID` matches) and return the applied sets; call at batch boundary only.
  - `func (c *VersionedCache) Len() int`
- Consumes: `blocks.ReadWriteSet`, `blocks.KVRead`, `blocks.KVWrite`, `blocks.WriteRecord`, `blocks.Version` (`github.com/hyperledger/fabric-x-sdk/blocks`).

- [ ] **Step 1: Write failing tests**

```go
package core

import (
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

func rws(reads []blocks.KVRead, writes []blocks.KVWrite) blocks.ReadWriteSet {
	return blocks.ReadWriteSet{Reads: reads, Writes: writes}
}

// First in-flight write of an ABSENT key (no read version) -> spec version 0.
func TestVersionedCache_FirstWriteAbsentKeyIsVersion0(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	rec, ok := c.Read("k")
	if !ok || rec.Version != 0 || string(rec.Value) != "v1" {
		t.Fatalf("want {v1,ver0}, got %+v ok=%v", rec, ok)
	}
}

// First in-flight write of a key read at committed version v -> spec version v+1.
func TestVersionedCache_FirstWriteExistingKeyIsBasePlus1(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(
		[]blocks.KVRead{{Key: "k", Version: &blocks.Version{BlockNum: 7}}},
		[]blocks.KVWrite{{Key: "k", Value: []byte("v1")}},
	))
	rec, _ := c.Read("k")
	if rec.Version != 8 {
		t.Fatalf("want ver 8, got %d", rec.Version)
	}
}

// A second in-flight batch writing the same key -> cachedSpec+1, retagged.
func TestVersionedCache_SecondInflightWriteIncrements(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}})) // ver 0
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}})) // ver 1
	rec, _ := c.Read("k")
	if rec.Version != 1 || string(rec.Value) != "v2" {
		t.Fatalf("want {v2,ver1}, got %+v", rec)
	}
}

// Committing tx2 (latest writer of k) evicts k; committing only tx1 does not.
func TestVersionedCache_EvictOnLatestWriterCommit(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}}))
	c.NoteCommitted("tx1")
	c.DrainEvictions()
	if _, ok := c.Read("k"); !ok {
		t.Fatal("k evicted by non-latest-writer commit")
	}
	c.NoteCommitted("tx2")
	c.DrainEvictions()
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted after latest-writer commit")
	}
}

// Deletes are recorded (IsDelete) and versioned like writes.
func TestVersionedCache_Delete(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", IsDelete: true}}))
	rec, ok := c.Read("k")
	if !ok || !rec.IsDelete {
		t.Fatalf("want delete record, got %+v ok=%v", rec, ok)
	}
}
```

- [ ] **Step 2: Run — expect FAIL** (`go test ./gateway/core/ -run TestVersionedCache -v`) → undefined `NewVersionedCache`.

- [ ] **Step 3: Implement `versioned_cache.go`**

```go
package core

import (
	"sync"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// entry is one in-flight write: its value, the version it WILL have once its
// writer commits (specVersion), and the committer-TxID that last wrote it.
type entry struct {
	rec      blocks.WriteRecord // Key, Value, IsDelete, Version=specVersion
	writerTx string
}

// VersionedCache holds the writes of submitted-but-uncommitted batches so a
// later batch can execute on them before they commit. It holds ONLY in-flight
// writes (cold committed reads are served by the per-batch query view). Bounded
// by the executor's in-flight window; entries drop when their writer commits.
type VersionedCache struct {
	mu      sync.RWMutex
	entries map[string]entry

	// committed/invalidated TxIDs queued by notification handlers, applied at
	// the next batch boundary by DrainEvictions (never mid-batch).
	evictMu     sync.Mutex
	committed   []string
	invalidated []string
}

func NewVersionedCache() *VersionedCache {
	return &VersionedCache{entries: make(map[string]entry)}
}

func (c *VersionedCache) Read(key string) (*blocks.WriteRecord, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	rec := e.rec // copy
	return &rec, true
}

// ApplyWrites records tx's writes at deterministic spec versions. Base version
// for a first-seen key is taken from tx's read-set (committed version it read);
// absent from the read-set ⇒ treated as a first write of an absent key (v0),
// which is correct for read-modify-write workloads (every written key is read
// first). A key already cached takes cachedSpec+1.
func (c *VersionedCache) ApplyWrites(txID string, r blocks.ReadWriteSet) {
	readVer := make(map[string]*blocks.Version, len(r.Reads))
	for _, rd := range r.Reads {
		readVer[rd.Key] = rd.Version
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range r.Writes {
		var spec uint64
		if cur, ok := c.entries[w.Key]; ok {
			spec = cur.rec.Version + 1
		} else if v, seen := readVer[w.Key]; seen && v != nil {
			spec = v.BlockNum + 1
		} else {
			spec = 0 // first write of an absent key
		}
		c.entries[w.Key] = entry{
			rec:      blocks.WriteRecord{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete, Version: spec},
			writerTx: txID,
		}
	}
}

func (c *VersionedCache) NoteCommitted(txID string) {
	c.evictMu.Lock()
	c.committed = append(c.committed, txID)
	c.evictMu.Unlock()
}

func (c *VersionedCache) NoteInvalidated(txID string) {
	c.evictMu.Lock()
	c.invalidated = append(c.invalidated, txID)
	c.evictMu.Unlock()
}

// DrainEvictions applies all queued committed/invalidated TxIDs: any cache entry
// whose writerTx is in either set is dropped (a committed write is now the real
// committed version; an invalidated write never landed). Returns the applied
// sets. Call ONLY at a batch boundary.
func (c *VersionedCache) DrainEvictions() (committed, invalidated []string) {
	c.evictMu.Lock()
	committed, invalidated = c.committed, c.invalidated
	c.committed, c.invalidated = nil, nil
	c.evictMu.Unlock()
	if len(committed) == 0 && len(invalidated) == 0 {
		return
	}
	drop := make(map[string]struct{}, len(committed)+len(invalidated))
	for _, id := range committed {
		drop[id] = struct{}{}
	}
	for _, id := range invalidated {
		drop[id] = struct{}{}
	}
	c.mu.Lock()
	for k, e := range c.entries {
		if _, ok := drop[e.writerTx]; ok {
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
	return
}

func (c *VersionedCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
```

- [ ] **Step 4: Run — expect PASS** (`go test ./gateway/core/ -run TestVersionedCache -race -v`).
- [ ] **Step 5: Commit** — `git add gateway/core/versioned_cache*.go && git commit -m "feat(gateway): VersionedCache for cross-batch in-flight writes"`

---

### Task 2: `CachedSnapshotter` — cache-first read layer

**Files:**
- Create: `gateway/core/cached_view.go`
- Test: `gateway/core/cached_view_test.go`

**Interfaces:**
- Consumes: `execution.KVSSnapshotter`, `execution.ReadStore` (`endorser/execution`); `*VersionedCache` (Task 1).
- Produces:
  - `func NewCachedSnapshotter(under execution.KVSSnapshotter, cache *VersionedCache) execution.KVSSnapshotter`
  - Internally a `cachedView` implementing `execution.ReadStore`: `Get` returns `cache.Read(key)` when present, else `under.Get(ns, key)`; `Close` closes the underlying view.

- [ ] **Step 1: Write failing tests** — a fake `execution.KVSSnapshotter`/`ReadStore` returning fixed committed records; assert cache hit shadows the underlying value+version, cache miss falls through, and `Close` propagates.

```go
package core

import (
	"testing"

	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

type fakeReader struct {
	data   map[string]*blocks.WriteRecord
	closed bool
}

func (r *fakeReader) Get(_, key string) (*blocks.WriteRecord, error) { return r.data[key], nil }
func (r *fakeReader) Close() error                                   { r.closed = true; return nil }

type fakeSnap struct{ r *fakeReader }

func (s fakeSnap) NewSnapshot(uint64) (execution.ReadStore, error) { return s.r, nil }

func TestCachedView_CacheHitShadowsUnderlying(t *testing.T) {
	under := &fakeReader{data: map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("committed"), Version: 3},
	}}
	cache := NewVersionedCache()
	cache.ApplyWrites("tx1", rws(
		[]blocks.KVRead{{Key: "k", Version: &blocks.Version{BlockNum: 3}}},
		[]blocks.KVWrite{{Key: "k", Value: []byte("inflight")}},
	)) // spec version 4
	snap := NewCachedSnapshotter(fakeSnap{r: under}, cache)
	view, _ := snap.NewSnapshot(0)
	rec, _ := view.Get("ns", "k")
	if string(rec.Value) != "inflight" || rec.Version != 4 {
		t.Fatalf("want {inflight,4}, got %+v", rec)
	}
}

func TestCachedView_MissFallsThroughAndCloses(t *testing.T) {
	under := &fakeReader{data: map[string]*blocks.WriteRecord{"k": {Key: "k", Value: []byte("committed"), Version: 3}}}
	snap := NewCachedSnapshotter(fakeSnap{r: under}, NewVersionedCache())
	view, _ := snap.NewSnapshot(0)
	rec, _ := view.Get("ns", "k")
	if string(rec.Value) != "committed" {
		t.Fatalf("want committed, got %+v", rec)
	}
	_ = view.Close()
	if !under.closed {
		t.Fatal("Close not propagated")
	}
}
```

- [ ] **Step 2: Run — expect FAIL** (undefined `NewCachedSnapshotter`).
- [ ] **Step 3: Implement `cached_view.go`**

```go
package core

import (
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

type cachedSnapshotter struct {
	under execution.KVSSnapshotter
	cache *VersionedCache
}

// NewCachedSnapshotter layers the cross-batch VersionedCache over an underlying
// snapshotter (the query-service Store). Reads hit in-flight writes first, then
// the per-batch committed view. Used as the endorser engine's KVSSnapshotter.
func NewCachedSnapshotter(under execution.KVSSnapshotter, cache *VersionedCache) execution.KVSSnapshotter {
	return &cachedSnapshotter{under: under, cache: cache}
}

func (s *cachedSnapshotter) NewSnapshot(blockNumber uint64) (execution.ReadStore, error) {
	under, err := s.under.NewSnapshot(blockNumber)
	if err != nil {
		return nil, err
	}
	return &cachedView{cache: s.cache, under: under}, nil
}

type cachedView struct {
	cache *VersionedCache
	under execution.ReadStore
}

func (v *cachedView) Get(namespace, key string) (*blocks.WriteRecord, error) {
	if rec, ok := v.cache.Read(key); ok {
		return rec, nil
	}
	return v.under.Get(namespace, key)
}

func (v *cachedView) Close() error { return v.under.Close() }
```

- [ ] **Step 4: Run — expect PASS** (`-race`).
- [ ] **Step 5: Commit** — `feat(gateway): cache-first CachedSnapshotter over the query view`

---

### Task 3: Wire the shared cache into the endorser engine and gateway

**Files:**
- Modify: `endorser/app/factory.go:39-67` (build engine over the cache-backed snapshotter; accept/return the shared cache)
- Modify: `gateway/app/wiring.go:74-93` (`BuildGateway` accepts `*core.VersionedCache`), `gateway/app/app.go:141`, `integration/test_helpers.go:195` (pass it)
- Modify: `gateway/core/api.go` (`Gateway` holds `cache *VersionedCache`; `New` takes it)

**Interfaces:**
- Consumes: `NewCachedSnapshotter` (Task 2), `NewVersionedCache` (Task 1).
- Produces: `Gateway.cache *VersionedCache` accessible to the executor; the endorser engine reads through the cache.

- [ ] **Step 1: Modify the endorser factory** — after building `store = query.NewStore(...)`, wrap it: pass a `*core.VersionedCache` in and build the engine over `core.NewCachedSnapshotter(store, cache)`. Because `endorser/app` importing `gateway/core` would invert the dependency, the cache type lives in `gateway/core`; inject it as an `execution.KVSSnapshotter` built by the caller. Concretely: change the factory to accept a pre-built `execution.KVSSnapshotter` (the cache-wrapped one) rather than building the bare `store`, OR add a `cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter` parameter (default identity). Use the `cacheWrap` parameter approach:

```go
// factory.go signature gains: cacheWrap func(execution.KVSSnapshotter) execution.KVSSnapshotter
// (nil ⇒ identity). Applied to `store` before NewEVMEngine:
snap := execution.KVSSnapshotter(store)
if cacheWrap != nil {
	snap = cacheWrap(snap)
}
engine := execution.NewEVMEngine(namespace, snap, evmConfig, monotonicVersions)
```

- [ ] **Step 2: Create the shared cache in the top-level wiring** — in the code that builds endorsers and the gateway together (`gateway/app` for production; `integration/test_helpers.go` for tests), create `cache := core.NewVersionedCache()`, pass `cacheWrap = func(s) { return core.NewCachedSnapshotter(s, cache) }` to the endorser factory, and pass `cache` to `BuildGateway`/`core.New`.

- [ ] **Step 3: Thread `cache` into `Gateway`** — add `cache *VersionedCache` to the `Gateway` struct and `New(...)`; store it.

- [ ] **Step 4: Run the existing suites — expect PASS unchanged** (`go test ./endorser/... ./gateway/... ./integration/ -run 'TestLocalX|TestBatchMergedCommit' -count=1`). Behavior is neutral: the cache is empty (no `ApplyWrites` yet), so every read falls through to the query view exactly as before.

- [ ] **Step 5: Commit** — `feat: wire shared VersionedCache into endorser reads + gateway`

---

## Phase 2 — Pipelined executor

### Task 4: Apply writes to the cache + drop `awaitCommit` behind the in-flight window

**Files:**
- Modify: `gateway/core/executor.go:60-131` (`executeCycle`)
- Modify: `gateway/core/api.go` (in-flight window state: a `chan struct{}` semaphore sized `maxInflight`, config field)
- Modify: `gateway/config/config.go` (add `MaxInflight int`, default 16)
- Test: `gateway/core/executor_test.go` (extend)

**Interfaces:**
- Consumes: `VersionedCache.ApplyWrites` (Task 1); the endorsement result's merged RWS. NOTE: `EndorsementClient.ExecuteBatch` currently returns `(sdk.Endorsement, included, terminal, error)`; the merged RWS must be recoverable to feed `ApplyWrites`. Add the merged `blocks.ReadWriteSet` to that return (decode from the signed proposal's write-set, same source `chain.go` uses), i.e. `ExecuteBatch(...) (sdk.Endorsement, included, terminal []*types.Transaction, rws blocks.ReadWriteSet, err error)`.
- Produces: a pipelined `executeCycle` that no longer blocks on commit.

- [ ] **Step 1: Write failing test** — drive `executeCycle` (via a test `Gateway` with fakes) for two batches; assert the second cycle starts before the first's commit notification arrives, and that `cache.Len() > 0` between submit and commit. (Model on the existing `gateway/core/executor_test.go` fakes; assert ordering via a channel that the fake submitter signals and the fake notifier withholds.)

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement.** In `executeCycle`, after a successful endorsement and TxID extraction, and BEFORE submit: `g.cache.ApplyWrites(fabricTxID, rws)`. Replace the synchronous `awaitCommit` + `Remove` with: acquire an in-flight slot (`g.inflight <- struct{}{}`, blocking when full), register the TxID with the notifier (Task 6 wires the real one; for now a registration hook), submit, then RETURN (advance to the next cycle). Commit resolution (evict + release the slot) moves to the notification handler (Task 5). Terminal-excluded txs are still evicted from pending immediately (unchanged). Draft:

```go
func (g *Gateway) executeCycle(ctx context.Context) {
	batch := g.pending.DrainUpTo(int(g.maxBatchSize.Load()))
	if len(batch) == 0 { g.waitForWork(ctx); return }

	g.cache.DrainEvictions() // batch boundary: apply queued commit/abort evictions

	end, included, terminal, rws, err := g.endorsers.ExecuteBatch(ctx, batch)
	if err != nil { logger.Errorf("batch endorse failed (%d txs): %v", len(batch), err); g.backoff(ctx); return }
	if len(terminal) > 0 { g.pending.Remove(hashesOf(terminal)) }
	if len(included) == 0 { g.waitForWork(ctx); return }

	fabricTxID, err := committerTxID(end.Proposal)
	if err != nil { logger.Errorf("extract committer tx id: %v", err); g.backoff(ctx); return }

	g.cache.ApplyWrites(fabricTxID, rws)          // in-flight writes visible to the next batch
	select {                                       // in-flight window backpressure
	case g.inflight <- struct{}{}:
	case <-ctx.Done():
		return
	}
	g.trackInflight(fabricTxID, included)          // records included hashes for commit/rollback (Task 5)
	g.notifier.Watch(fabricTxID)                   // REGISTER before submit (Task 6)
	if err := g.SubmitFabricTx(ctx, end); err != nil {
		logger.Errorf("submit failed (tx %s): %v", fabricTxID, err)
		g.resolveInflight(fabricTxID, false)       // release slot; treat as not-committed
		g.backoff(ctx)
		return
	}
	// No awaitCommit: return and let the next cycle run immediately.
}
```

- [ ] **Step 4: Run — expect PASS** + existing gateway tests green.
- [ ] **Step 5: Commit** — `feat(gateway): pipeline executeCycle via in-flight window + cache ApplyWrites`

---

### Task 5: In-flight tracking, commit resolution, and suffix cascade

**Files:**
- Modify: `gateway/core/executor.go` (in-flight registry + resolution + cascade)
- Test: `gateway/core/executor_test.go`

**Interfaces:**
- Consumes: `VersionedCache.NoteCommitted`/`NoteInvalidated` (Task 1); the in-flight window (Task 4).
- Produces:
  - `func (g *Gateway) trackInflight(txID string, included []*types.Transaction)` — record ordered in-flight batch {txID, included hashes}.
  - `func (g *Gateway) resolveInflight(txID string, committed bool)` — on committed: `cache.NoteCommitted(txID)`, `pending.Remove(includedHashes)`, release the slot, drop from the registry. On not-committed (invalidated/timeout-fail): trigger cascade from this txID's position.
  - `func (g *Gateway) cascadeFrom(txID string)` — mark this and all later in-flight batches invalidated (`cache.NoteInvalidated`), return their txs to pending (they re-drain next cycle), release their slots, clear the registry suffix.

- [ ] **Step 1: Write failing tests** — (a) commit resolution removes included from pending, notes committed, releases a slot; (b) invalidation of the first of three in-flight batches cascades all three back to pending + notes them invalidated + releases three slots.

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** the ordered registry (a slice of `{txID string; hashes []common.Hash}` under a mutex), `trackInflight` (append), `resolveInflight` (commit path: NoteCommitted + Remove + release + drop; else cascadeFrom), and `cascadeFrom` (find index; for it and all after: NoteInvalidated, re-Add hashes to pending + signal arrivals, release slot; truncate the registry at the index). Commit resolution and cascade are invoked by the notification handler (Task 6); eviction itself is applied at the next batch boundary via `DrainEvictions`.

- [ ] **Step 4: Run — expect PASS** (`-race`).
- [ ] **Step 5: Commit** — `feat(gateway): in-flight registry, commit resolution, suffix cascade`

---

## Phase 3 — Per-TxID notifications and fallback

### Task 6: Per-TxID notifier adapter (register-then-submit) + query-service fallback

**Files:**
- Create: `gateway/core/notifier.go`
- Test: `gateway/core/notifier_test.go`
- Modify: `gateway/core/api.go` (hold the notifier; `Watch`), `integration/test_helpers.go` (wire `notification.Notifier`, drop `AllTxStreamer` on the pipelined path)

**Interfaces:**
- Consumes: `notification.Notifier`, `notification.NewNotifier`, `notification.NewProcessor`, `notification.TxStatusHandler`, `notification.TxStatusEvent` (`github.com/hyperledger/fabric-x-sdk/notification`); the query `Store` for fallback; `resolveInflight` (Task 5).
- Produces:
  - `type txNotifier struct{ ... }`, `func newTxNotifier(subscribe chan<- []string, timeout time.Duration, fallback func(txID string) (committed bool), resolve func(txID string, committed bool)) *txNotifier`
  - `func (n *txNotifier) Watch(txID string)` — push the TxID onto the subscribe channel (register) and arm a timeout timer.
  - Implements `notification.TxStatusHandler`: `Handle(ctx, events)` → for each event `resolve(event.TxID, event.Valid())`, cancel its timer.
  - On timeout: `resolve(txID, fallback(txID))`.

- [ ] **Step 1: Write failing tests** — (a) `Watch` then a committed `TxStatusEvent` calls `resolve(txID, true)`; (b) `Watch` then no event within the timeout calls `fallback` and resolves with its verdict; (c) register-before-submit ordering: `Watch` pushes onto the subscribe channel synchronously before returning.

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** `txNotifier`: `Watch` sends `[]string{txID}` on the subscribe channel and starts a `time.AfterFunc(timeout, …)` recorded in a `map[string]*time.Timer` under a mutex; `Handle` ranges events, stops+deletes the timer, calls `resolve(txID, ev.Valid())`; the timer callback calls `resolve(txID, fallback(txID))` (fallback = query the committed state for the batch's keys/versions). Wire `notification.NewNotifier(peer, notification.NewProcessor([]TxStatusHandler{txNotifier}, log))` and run `Subscribe(ctx, subscribeCh)` in a goroutine; the gateway's `Watch` feeds `subscribeCh`.

- [ ] **Step 4: Query-service fallback** — implement `fallback(txID)` as: for the in-flight batch's written keys, read current committed versions via the query `Store`; committed iff they reached the batch's spec versions. (Batch spec versions are recoverable from the cache entries tagged `txID` before eviction.)

- [ ] **Step 5: Run — expect PASS** (`-race`), then wire into `test_helpers.go` and drop `AllTxStreamer` for the pipelined harness.
- [ ] **Step 6: Commit** — `feat(gateway): per-TxID notifier (register-then-submit) + query-service fallback`

---

### Task 7: Config + defaults

**Files:**
- Modify: `gateway/config/config.go` (`MaxInflight int` default 16; `NotifyTimeout time.Duration` default from today's 60s backstop), `gateway/app/wiring.go` (thread them).

- [ ] **Step 1: Add fields** with mapstructure/yaml tags mirroring `MaxBatchSize`; defaults applied in `BuildGateway` (`if MaxInflight <= 0 { MaxInflight = 16 }`).
- [ ] **Step 2: Run** `go test ./gateway/config/ -count=1` — PASS.
- [ ] **Step 3: Commit** — `feat(gateway): configurable in-flight window + notify timeout`

---

### Task 8: Integration — pipelined commit, cascade, dropped-notification fallback

**Files:**
- Modify: `integration/batch_test.go` (extend `TestBatchMergedCommit`), `integration/test_helpers.go`

- [ ] **Step 1: Pipelined multi-batch test** — submit enough txs for several batches; assert all commit with correct per-tx receipts and monotonic `transactionIndex`, and that batches overlapped (the executor did not serialize on commit — assert via timing or a hook count).
- [ ] **Step 2: Cascade test** — force a batch invalidation (a fake notifier delivering an invalid `TxStatusEvent` for batch k); assert batch k and later batches re-execute and eventually commit, and earlier batches are untouched.
- [ ] **Step 3: Fallback test** — withhold a notification past the timeout with the committed state actually advanced; assert the fallback resolves it committed (no spurious rollback).
- [ ] **Step 4: Run** all three under `-race`; run `TestLocalX` for regression.
- [ ] **Step 5: Commit** — `test(integration): pipelined commit, cascade, and notification-fallback`

---

## Self-Review

**Spec coverage:** VersionedCache/bounded+spec-versions/eviction → Task 1; cache-first reader → Task 2; wiring → Task 3; two-phase stays (unchanged execution, cache as reader) → Tasks 2–3; pipelined executor + in-flight window → Task 4; commit resolution + cascade → Task 5; per-TxID Notifier register-then-submit + query fallback → Task 6; config (in-flight 16, timeout) → Task 7; integration (pipeline, cascade, fallback) → Task 8. Eviction-only-at-batch-boundary → `DrainEvictions` called at the top of `executeCycle` (Task 4). All spec sections mapped.

**Placeholder scan:** No TBD/TODO. `EndorsementClient.ExecuteBatch` gains an `rws blocks.ReadWriteSet` return (Task 4 Interfaces) — its callers (`executeCycle`) are updated in the same task; the decode source is the signed proposal write-set already parsed by `chain.go`.

**Type consistency:** `VersionedCache` methods (`Read`/`ApplyWrites`/`NoteCommitted`/`NoteInvalidated`/`DrainEvictions`/`Len`) are used with identical signatures in Tasks 2, 4, 5, 6. `NewCachedSnapshotter(execution.KVSSnapshotter, *VersionedCache) execution.KVSSnapshotter` consistent Tasks 2–3. `resolveInflight(string,bool)` / `cascadeFrom(string)` / `trackInflight(string,[]*types.Transaction)` consistent Tasks 4–6. `Watch(string)` consistent Tasks 4, 6.

**Blind-write base version — RESOLVED (do not leave as a risk).** `ApplyWrites` needs a committed base version for every written key that is not already cached. This is guaranteed: the EVM SLOADs before every SSTORE (EIP-2200/2929 gas metering), and `StateDB` reads before writing every account field (`AddBalance`/`SubBalance` call `GetBalance`, the nonce is read in `PrepareMessage`, `SetCode` calls `GetCode`), so the authoritative read-set already carries a versioned read for every written key. To make it airtight rather than rely on geth internals, add a **warm-pass safety read**: ensure the RWS carries a versioned read for every written key even if some future path writes without reading. Implement in Task 1a below; it is safe in the CFT sole-writer model (the added read cannot cause a spurious conflict — no competing writer exists).

### Task 1a: Guarantee a base version for every written key

**Files:** Modify `endorser/execution/statedb.go` (write path / `Result`), Test `endorser/execution/statedb_test.go` (or the flow tests).

- [ ] **Step 1: Failing test** — build a `StateDB` over a reader where key `k` has committed version 5; perform a write to `k` WITHOUT a prior `GetState`; assert `Result().Reads` contains `k` with version 5 (a versioned read was captured for the write).
- [ ] **Step 2: Run — expect FAIL** (blind write leaves `k` out of the read-set).
- [ ] **Step 3: Implement** — in `Result()` (RWS build), for every write key with no read-set entry, fetch its committed record via the store once and add a versioned `KVRead` (dedup against existing reads; a key already read is untouched). Keep the existing `SetState` prev-read behavior; this only ensures the read-set is complete for spec-version derivation.
- [ ] **Step 4: Run — expect PASS**; run `./endorser/execution/... -race` and the flow benchmarks (must stay >100K — this is one guarded store read per otherwise-blind write, ~zero for real read-modify-write txs).
- [ ] **Step 5: Commit** — `feat(execution): ensure every written key carries a versioned read (spec-version base)`
