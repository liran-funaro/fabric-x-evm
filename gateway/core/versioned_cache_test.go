/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

func rws(reads []blocks.KVRead, writes []blocks.KVWrite) blocks.ReadWriteSet {
	return blocks.ReadWriteSet{Reads: reads, Writes: writes}
}

// readEntry exposes a cached key's full entry (value + spec version + the
// committer-TxID it is attributed to) for tests that must assert WHICH in-flight
// batch owns a key's cache entry -- Read only returns the WriteRecord, not the
// writerTx. Same-package test-only helper.
func (c *VersionedCache) readEntry(key string) (entry, bool) {
	e, ok := c.entries[key]
	return e, ok
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
// Also asserts DrainEvictions returns the applied sets, and Len tracks the
// cache size across the eviction.
func TestVersionedCache_EvictOnLatestWriterCommit(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}}))
	c.NoteCommitted("tx1")
	committed, invalidated := c.DrainEvictions()
	if len(committed) != 1 || committed[0] != "tx1" || len(invalidated) != 0 {
		t.Fatalf("want committed=[tx1] invalidated=[], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); !ok {
		t.Fatal("k evicted by non-latest-writer commit")
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("want Len()==1 (k still cached), got %d", got)
	}
	c.NoteCommitted("tx2")
	committed, invalidated = c.DrainEvictions()
	if len(committed) != 1 || committed[0] != "tx2" || len(invalidated) != 0 {
		t.Fatalf("want committed=[tx2] invalidated=[], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted after latest-writer commit")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("want Len()==0 after eviction, got %d", got)
	}
}

// Invalidating tx2 (latest writer of k) evicts k; invalidating only tx1 does
// not. Mirrors TestVersionedCache_EvictOnLatestWriterCommit but exercises the
// invalidated-eviction path, which is otherwise uncovered even though the
// interface promises eviction on "committed OR invalidated".
func TestVersionedCache_EvictOnLatestWriterInvalidate(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}}))
	c.NoteInvalidated("tx1")
	committed, invalidated := c.DrainEvictions()
	if len(invalidated) != 1 || invalidated[0] != "tx1" || len(committed) != 0 {
		t.Fatalf("want committed=[] invalidated=[tx1], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); !ok {
		t.Fatal("k evicted by non-latest-writer invalidate")
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("want Len()==1 (k still cached), got %d", got)
	}
	c.NoteInvalidated("tx2")
	committed, invalidated = c.DrainEvictions()
	if len(invalidated) != 1 || invalidated[0] != "tx2" || len(committed) != 0 {
		t.Fatalf("want committed=[] invalidated=[tx2], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted after latest-writer invalidate")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("want Len()==0 after eviction, got %d", got)
	}
}

// In pipeline mode, a committed batch's writes are held for ONE extra boundary
// so the immediately-following authoritative pass still reads them from the
// write cache. First DrainEvictionsDeferred after the commit keeps k; the
// second drops it. Invalidations are never deferred (see the next test).
func TestVersionedCache_DeferredCommittedEvictionHoldsOneBoundary(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.NoteCommitted("tx1")

	// Boundary 1: tx1 just committed -> its write is HELD, not yet evicted.
	if inv := c.DrainEvictionsDeferred(); len(inv) != 0 {
		t.Fatalf("want no invalidations, got %v", inv)
	}
	if _, ok := c.Read("k"); !ok {
		t.Fatal("k evicted at the commit boundary; must be held one extra boundary")
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("want Len()==1 (k held), got %d", got)
	}

	// Boundary 2: the held committed set is now applied -> k evicted.
	if inv := c.DrainEvictionsDeferred(); len(inv) != 0 {
		t.Fatalf("want no invalidations, got %v", inv)
	}
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted at the boundary after the hold")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("want Len()==0 after deferred eviction, got %d", got)
	}
}

// Invalidated writes must never be visible to auth, so DrainEvictionsDeferred
// drops them on the SAME boundary (no one-boundary hold), and returns them so
// the caller can rebuild the cache from survivors.
func TestVersionedCache_DeferredInvalidationIsImmediate(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.NoteInvalidated("tx1")

	inv := c.DrainEvictionsDeferred()
	if len(inv) != 1 || inv[0] != "tx1" {
		t.Fatalf("want invalidated=[tx1], got %v", inv)
	}
	if _, ok := c.Read("k"); ok {
		t.Fatal("invalidated write must be evicted immediately, not deferred")
	}
}

// SetEvictHoldDepth widens the committed-write hold beyond the default one
// boundary: a committed batch's writes must stay readable in the write cache for
// exactly evictHoldDepth boundaries so the pipelined auth pass keeps serving a
// just-committed key from the cache while the query service's commit-visibility
// lag spans more than one fast boundary (the small-batch case). At depth 2, k
// survives the first TWO deferred drains after the commit and is evicted only on
// the third. The default (depth 1) case is covered by
// TestVersionedCache_DeferredCommittedEvictionHoldsOneBoundary.
func TestVersionedCache_DeferredEvictionHoldsConfiguredDepth(t *testing.T) {
	c := NewVersionedCache()
	c.SetEvictHoldDepth(2)
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.NoteCommitted("tx1")

	// Boundaries 1 and 2: tx1's write is HELD (depth 2), not yet evicted.
	for i := 1; i <= 2; i++ {
		if inv := c.DrainEvictionsDeferred(); len(inv) != 0 {
			t.Fatalf("boundary %d: want no invalidations, got %v", i, inv)
		}
		if _, ok := c.Read("k"); !ok {
			t.Fatalf("k evicted at boundary %d; depth-2 hold must keep it for 2 boundaries", i)
		}
		if got := c.Len(); got != 1 {
			t.Fatalf("boundary %d: want Len()==1 (k held), got %d", i, got)
		}
	}

	// Boundary 3: the depth-2 hold has elapsed -> k evicted.
	if inv := c.DrainEvictionsDeferred(); len(inv) != 0 {
		t.Fatalf("want no invalidations, got %v", inv)
	}
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted at the boundary after the depth-2 hold elapsed")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("want Len()==0 after deferred eviction, got %d", got)
	}
}

// SetEvictHoldDepth clamps a depth below 1 up to 1: the deferred drain always
// holds a committed batch at least one boundary (the read-layering floor), so
// 0 or negative silently becomes the default one-boundary hold rather than the
// serial immediate-eviction behavior (which is DrainEvictions, not this path).
func TestVersionedCache_EvictHoldDepthClampsToOne(t *testing.T) {
	c := NewVersionedCache()
	c.SetEvictHoldDepth(0)
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.NoteCommitted("tx1")

	// Boundary 1: held (clamped to 1, not evicted immediately).
	c.DrainEvictionsDeferred()
	if _, ok := c.Read("k"); !ok {
		t.Fatal("depth 0 must clamp to 1 (hold one boundary), but k was evicted immediately")
	}
	// Boundary 2: evicted after the one-boundary hold.
	c.DrainEvictionsDeferred()
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted at the boundary after the clamped one-boundary hold")
	}
}

// The rolling invalidation window records the keys committed over the last
// invalidateDepth pipeline boundaries, so an auth-view reopen can drop exactly
// those from its inherited clone. Each deferred drain pushes that boundary's
// committed keys; boundaries older than the depth fall out of the window. The
// keys are snapshotted from the still-live (held) entries at drain time.
func TestVersionedCache_RecentlyCommittedKeysWindow(t *testing.T) {
	c := NewVersionedCache()
	c.SetInvalidateDepth(2)

	commitBoundary := func(txID, key string) {
		c.ApplyWrites(txID, rws(nil, []blocks.KVWrite{{Key: key, Value: []byte("v")}}))
		c.NoteCommitted(txID)
		c.DrainEvictionsDeferred()
	}
	has := func(m map[string]struct{}, keys ...string) bool {
		if len(m) != len(keys) {
			return false
		}
		for _, k := range keys {
			if _, ok := m[k]; !ok {
				return false
			}
		}
		return true
	}

	commitBoundary("tx1", "a")
	if got := c.RecentlyCommittedKeys(); !has(got, "a") {
		t.Fatalf("after tx1 window = %v, want {a}", got)
	}
	commitBoundary("tx2", "b")
	if got := c.RecentlyCommittedKeys(); !has(got, "a", "b") {
		t.Fatalf("after tx2 window = %v, want {a,b} (depth 2)", got)
	}
	// Third boundary evicts the oldest (a) -> window holds only the last 2.
	commitBoundary("tx3", "c")
	if got := c.RecentlyCommittedKeys(); !has(got, "b", "c") {
		t.Fatalf("after tx3 window = %v, want {b,c} (a aged out at depth 2)", got)
	}
}

// Depth 0 (the default, clone-only baseline) disables the window entirely: no
// keys are ever recorded, so RecentlyCommittedKeys stays nil and the auth reopen
// carries the whole clone forward.
func TestVersionedCache_InvalidateDepthZeroDisablesWindow(t *testing.T) {
	c := NewVersionedCache() // depth 0 by default
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "a", Value: []byte("v")}}))
	c.NoteCommitted("tx1")
	c.DrainEvictionsDeferred()
	if got := c.RecentlyCommittedKeys(); got != nil {
		t.Fatalf("depth 0 must record nothing, got %v", got)
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

// Rebuild atomically replaces the cache with only the given survivor batches,
// re-applying their writes from EMPTY in submission order using the same
// spec-version math as ApplyWrites. This is the ⚠️ repair at the cache layer:
// batch A writes K=va, batch B overwrites K=vb (so K currently carries B); after
// B is invalidated, rebuilding from just {A} must restore K=va attributed to A
// with A's original spec version -- exactly the state A would have had if B had
// never applied. Keys that only B wrote must be gone.
func TestVersionedCache_RebuildFromSurvivorsRestoresOverwrittenKey(t *testing.T) {
	c := NewVersionedCache()
	// A writes K (read at committed v3 -> spec 4) and J; B overwrites K (spec 5)
	// and writes its own key M.
	aRWS := rws(
		[]blocks.KVRead{{Key: "K", Version: &blocks.Version{BlockNum: 3}}},
		[]blocks.KVWrite{{Key: "K", Value: []byte("va")}, {Key: "J", Value: []byte("ja")}},
	)
	bRWS := rws(nil, []blocks.KVWrite{{Key: "K", Value: []byte("vb")}, {Key: "M", Value: []byte("mb")}})
	c.ApplyWrites("A", aRWS)
	c.ApplyWrites("B", bRWS)

	// Record A's original spec version of K for the deterministic-version check.
	aEntry, ok := c.readEntry("K")
	if !ok || aEntry.writerTx != "B" {
		t.Fatalf("precondition: after both applies K must carry B, got %+v ok=%v", aEntry, ok)
	}
	// Rebuild from survivors = {A} only (B invalidated).
	c.Rebuild([]ReapplySpec{{TxID: "A", RWS: aRWS}})

	rec, ok := c.Read("K")
	if !ok {
		t.Fatal("K absent after rebuild from survivor A -- A's write was lost (naive suffix-drop bug)")
	}
	if string(rec.Value) != "va" {
		t.Fatalf("K value = %q, want va (A's write restored, not B's vb)", rec.Value)
	}
	if rec.Version != 4 {
		t.Fatalf("K spec version = %d, want 4 (A re-applied from empty: read-base 3 + 1)", rec.Version)
	}
	e, _ := c.readEntry("K")
	if e.writerTx != "A" {
		t.Fatalf("K writerTx = %q, want A after rebuild from {A}", e.writerTx)
	}
	// M was only B's write -> gone. J was A's -> present.
	if _, ok := c.Read("M"); ok {
		t.Fatal("M must be gone (only invalidated B wrote it)")
	}
	if _, ok := c.Read("J"); !ok {
		t.Fatal("J must survive (A wrote it)")
	}
	if got := c.Len(); got != 2 {
		t.Fatalf("Len()=%d, want 2 (K,J)", got)
	}
}

// Rebuild from an empty survivor set clears the cache entirely (both writers of
// K departed): the mirror of the repair above.
func TestVersionedCache_RebuildFromNoneClearsCache(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("A", rws(nil, []blocks.KVWrite{{Key: "K", Value: []byte("va")}}))
	c.ApplyWrites("B", rws(nil, []blocks.KVWrite{{Key: "K", Value: []byte("vb")}}))
	c.Rebuild(nil)
	if _, ok := c.Read("K"); ok {
		t.Fatal("K must be absent after rebuild from empty survivor set")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len()=%d, want 0", got)
	}
}

// Concurrent-access stress test reflecting the cache's real concurrency
// contract (see VersionedCache): entries is READ concurrently only DURING a
// batch (many warm-pass workers) and MUTATED only at the batch boundary on the
// single executor goroutine, the two phases separated by a join. Only the
// eviction queue (NoteCommitted/NoteInvalidated) is touched asynchronously by
// notification handlers, so it runs continuously alongside both phases -- those
// handlers touch only the evictMu-guarded queues, never entries. No ordering
// assertions: this is a pure data-race check, meant to be run with -race.
func TestVersionedCache_ConcurrentAccess(t *testing.T) {
	c := NewVersionedCache()
	const readers = 8
	const rounds = 200
	const keySpace = 16

	// Async notification handlers: queue commits/invalidations the whole time,
	// concurrent with both batch reads and boundary mutation.
	stop := make(chan struct{})
	var notifiers sync.WaitGroup
	notifiers.Add(2)
	go func() {
		defer notifiers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				c.NoteCommitted(fmt.Sprintf("tx-c-%d", i))
			}
		}
	}()
	go func() {
		defer notifiers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				c.NoteInvalidated(fmt.Sprintf("tx-i-%d", i))
			}
		}
	}()

	for r := 0; r < rounds; r++ {
		// Batch phase: many concurrent readers, no writer in flight.
		var batch sync.WaitGroup
		batch.Add(readers)
		for g := 0; g < readers; g++ {
			go func() {
				defer batch.Done()
				for i := 0; i < keySpace; i++ {
					c.Read(fmt.Sprintf("k%d", i))
				}
			}()
		}
		batch.Wait() // join warm workers before mutating (mirrors executor's wg.Wait())

		// Boundary phase: single goroutine mutates entries.
		c.DrainEvictions()
		c.ApplyWrites(fmt.Sprintf("tx-w-%d", r),
			rws(nil, []blocks.KVWrite{{Key: fmt.Sprintf("k%d", r%keySpace), Value: []byte("v")}}))
		c.Len()
	}

	close(stop)
	notifiers.Wait()
}

// TestVersionedCache_ConcurrentReadWithBoundaryMutation models the RPC
// transaction-validation read path, which the original "reads happen only on
// warm-pass workers, joined before the boundary" contract overlooked. A
// SendTransaction RPC runs ValidateTx -> Gateway.NonceAt/BalanceAt ->
// cachedView.Get -> VersionedCache.Read on an RPC handler goroutine that is NOT
// synchronized with the executor's batch boundary, so it can call Read at the
// exact instant the executor's ApplyWrites/DrainEvictions is mutating entries.
// Unlike TestVersionedCache_ConcurrentAccess (which joins its readers before the
// boundary phase, mirroring the warm-pass wg.Wait()), this test deliberately
// leaves the reader running with NO join to the writer, so -race observes the
// unguarded entries map access unless entries is mutex-protected.
func TestVersionedCache_ConcurrentReadWithBoundaryMutation(t *testing.T) {
	c := NewVersionedCache()
	const keySpace = 32

	stop := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				c.Read(fmt.Sprintf("k%d", i%keySpace))
			}
		}
	}()

	// Executor goroutine: apply + commit + drain in a tight loop, concurrent
	// with the never-joined reader above.
	for r := 0; r < 2000; r++ {
		txID := fmt.Sprintf("tx-%d", r)
		c.ApplyWrites(txID,
			rws(nil, []blocks.KVWrite{{Key: fmt.Sprintf("k%d", r%keySpace), Value: []byte("v")}}))
		c.NoteCommitted(txID)
		c.DrainEvictions()
		c.Len()
	}

	close(stop)
	reader.Wait()
}
