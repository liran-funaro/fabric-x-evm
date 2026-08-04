/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

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
//
// Concurrency: entries is STRUCTURALLY MUTATED only at the batch boundary on the
// single executor goroutine (ApplyWrites, DrainEvictions/Deferred, Rebuild), but
// it is READ from goroutines that are NOT synchronized with that boundary. Two
// classes of reader exist: warm-pass workers during a batch, AND -- crucially --
// RPC handler goroutines running transaction validation (SendTransaction ->
// ValidateTx -> Gateway.NonceAt/BalanceAt -> cachedView.Get -> Read), which fire
// whenever a client submits and can land at the exact instant the executor is
// mutating entries. Those RPC reads have no wg.Wait()/go-spawn happens-before
// edge to the boundary, so entries is guarded by mu: readers (Read, Len) take
// RLock and the boundary mutators take Lock. The write side is single-writer
// (executor goroutine only), so RLock holders never contend with each other and
// the exclusive Lock is taken only briefly at the boundary. committed/invalidated
// stay under the separate evictMu (queued asynchronously by notification
// handlers); committedHeldRing and roEvictPending are touched only at the boundary
// on the executor goroutine and need no lock.
type VersionedCache struct {
	mu      sync.RWMutex // guards entries against concurrent RPC-validation reads
	entries map[string]entry

	// committed/invalidated TxIDs queued by notification handlers off the
	// executor goroutine, applied at the next batch boundary by DrainEvictions
	// (never mid-batch). These are the only fields touched concurrently, so they
	// keep evictMu.
	evictMu     sync.Mutex
	committed   []string
	invalidated []string

	// committedHeldRing is the delay buffer for committed-write eviction on the
	// pipelined path: a FIFO of one committed-TxID set per pipeline boundary, held
	// so a committed batch's writes stay readable in THIS write cache for
	// evictHoldDepth boundaries before eviction (see DrainEvictionsDeferred and
	// SetEvictHoldDepth). Touched only at the boundary on the executor goroutine, so
	// it needs no lock. evictHoldDepth defaults to 1 (the original one-boundary
	// hold, so auth(N+1) reads N's writes from the cache); a larger depth keeps a
	// just-committed key readable across more boundaries, covering a query-service
	// commit-visibility lag that spans more than one fast boundary (the small-batch
	// case). Set once before Start (EVM_PIPE_EVICT_HOLD_DEPTH via SetEvictHoldDepth).
	committedHeldRing [][]string
	evictHoldDepth    int

	// recentCommittedKeys is a rolling window of the RAW keys whose committed
	// version advanced over the last invalidateDepth pipeline boundaries (a ring
	// of one key-slice per boundary, newest last). On an auth-view reopen the
	// union of this window is invalidated from the inherited query-view clone so
	// auth re-reads those keys at current committed state instead of the stale
	// warm-time value (the eviction-vs-visibility skew). Pushed during the deferred
	// drain and read at reopen -- both at the boundary on the executor goroutine --
	// so it needs no lock. Empty (and never pushed) when invalidateDepth == 0, the
	// clone-only baseline. invalidateDepth is a boot knob (EVM_PIPE_INVALIDATE_DEPTH
	// via SetInvalidateDepth), set once before Start and only read thereafter.
	recentCommittedKeys [][]string
	invalidateDepth     int

	// ro is the optional cross-batch read-only cache for hot, rarely-written
	// committed records (nil unless EnableReadOnlyCache was called). It sits
	// BELOW this in-flight write cache in the read path (write-cache ->
	// read-only-cache -> query view) and is maintained at the same batch
	// boundary. roEvictPending accumulates the keys this gateway writes each
	// cycle (recorded in ApplyWrites) so MaintainReadOnly can evict their
	// now-stale read-only entries at the next boundary. Both ApplyWrites and
	// MaintainReadOnly run at the boundary on the executor goroutine, so
	// roEvictPending needs no lock.
	ro             *ReadOnlyCache
	roEvictPending []string

	// capturePipeline enables the pipelined auth read-layering fix: when true, a
	// warm-pass cachedView records the keys it resolves from THIS in-flight write
	// cache so the reopened authoritative view inherits them (see
	// cachedView.Reopen). Off by default; the gateway flips it on with the
	// pipelined executor (SetCaptureForReopen, called from Gateway.SetPipelined).
	// Set once before Start and only read thereafter (like the gateway's pipelined
	// flag), so the concurrent warm-pass readers that consult it never race a
	// write. Gating on it keeps the serial default path free of any capture cost.
	capturePipeline bool
}

func NewVersionedCache() *VersionedCache {
	return &VersionedCache{entries: make(map[string]entry), evictHoldDepth: 1}
}

// SetCaptureForReopen toggles capture of warm-pass write-cache resolutions for
// the pipelined auth pass to inherit (see the capturePipeline field and
// cachedView.Reopen). Call once before Start, on the same VersionedCache shared
// by the cached snapshotter (read path) and the Gateway; the gateway wires it
// from SetPipelined. No-op cost when disabled.
func (c *VersionedCache) SetCaptureForReopen(enabled bool) {
	c.capturePipeline = enabled
}

// EnableReadOnlyCache attaches a cross-batch read-only cache with the given
// capacity and admission threshold. Call once, before the executor starts, on
// the same VersionedCache handed to both the cached snapshotter (read path) and
// the Gateway (boundary maintenance). A capacity/threshold of 0 falls back to
// the package defaults.
func (c *VersionedCache) EnableReadOnlyCache(capacity int, threshold uint64) {
	c.ro = newReadOnlyCache(capacity, threshold)
}

func (c *VersionedCache) Read(key string) (*blocks.WriteRecord, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
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
	c.mu.Lock()
	c.apply(txID, r)
	c.mu.Unlock()
	// Queue this batch's written keys for read-only-cache eviction at the next
	// boundary: once these commit, their committed version advances, so any
	// read-only entry for them would be stale. Evicting one boundary after the
	// write (well before the commit) only widens the safe margin -- while the
	// write is in-flight it lives in c.entries above and shadows the read-only
	// cache anyway. Over-eviction (e.g. if the batch later aborts) is harmless:
	// it just forces a re-fetch and re-admission.
	if c.ro != nil {
		for _, w := range r.Writes {
			c.roEvictPending = append(c.roEvictPending, w.Key)
		}
	}
}

// readOnlyGet consults the read-only cache (nil-safe). Called on the read path
// AFTER an in-flight write-cache miss and BEFORE the query view.
func (c *VersionedCache) readOnlyGet(key string) (*blocks.WriteRecord, bool) {
	if c.ro == nil {
		return nil, false
	}
	return c.ro.get(key)
}

// readOnlyStage offers a present, non-delete record just read from the query
// view as a read-only-cache admission candidate (nil-safe). Called on the read
// path after a query-view hit, concurrently by warm-pass workers.
func (c *VersionedCache) readOnlyStage(key string, rec *blocks.WriteRecord) {
	if c.ro == nil {
		return
	}
	c.ro.stage(key, rec)
}

// MaintainReadOnly runs the read-only cache's batch-boundary maintenance
// (evict this gateway's freshly-written keys, admit staged candidates, enforce
// MFU capacity). Called once per cycle at the boundary on the executor
// goroutine, so it never overlaps concurrent get/stage. Nil-safe.
func (c *VersionedCache) MaintainReadOnly() {
	if c.ro == nil {
		return
	}
	evict := c.roEvictPending
	c.roEvictPending = nil
	c.ro.maintain(evict)
}

// apply records tx's writes into c.entries at deterministic spec versions. The
// caller MUST hold c.mu for writing (ApplyWrites and Rebuild both do); apply
// mutates and reads c.entries directly and takes no lock itself. See ApplyWrites
// for the spec-version rules.
func (c *VersionedCache) apply(txID string, r blocks.ReadWriteSet) {
	readVer := make(map[string]*blocks.Version, len(r.Reads))
	for _, rd := range r.Reads {
		readVer[rd.Key] = rd.Version
	}
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

// SetInvalidateDepth sets how many recent pipeline boundaries of committed keys
// the auth-view reopen invalidates from the inherited query-view clone (see
// recentCommittedKeys and RecentlyCommittedKeys). 0 (default) disables
// invalidation entirely -- the clone is carried forward whole, the baseline that
// livelocks on hot-key traffic. Call once before Start, on the same VersionedCache
// shared by the read path and the Gateway; the gateway wires it from
// EVM_PIPE_INVALIDATE_DEPTH in SetPipelined.
func (c *VersionedCache) SetInvalidateDepth(d int) {
	if d < 0 {
		d = 0
	}
	c.invalidateDepth = d
}

// SetEvictHoldDepth sets how many pipeline boundaries a committed batch's writes
// are held in the write cache before eviction on the deferred (pipelined) drain
// (see DrainEvictionsDeferred). The default and minimum is 1 -- the original
// one-boundary hold, so auth(N+1) reads N's writes from the cache. A larger depth
// keeps a just-committed key readable across more boundaries, closing the
// eviction-vs-visibility skew when the query-service commit-visibility lag spans
// more than one boundary (the small-batch case, where fast boundaries let a
// committed key be evicted and cold-fetched from the query view before the view
// reflects the commit -- a stale warm read that MVCC-aborts). Call once before
// Start, on the same VersionedCache shared by the read path and the Gateway; the
// gateway wires it from EVM_PIPE_EVICT_HOLD_DEPTH in SetPipelined.
func (c *VersionedCache) SetEvictHoldDepth(d int) {
	if d < 1 {
		d = 1
	}
	c.evictHoldDepth = d
}

// RecentlyCommittedKeys returns the union of the raw keys committed over the last
// invalidateDepth pipeline boundaries, for an auth-view reopen to invalidate from
// its inherited clone. Returns nil when invalidateDepth == 0 (no invalidation) or
// the window is empty. Call at the boundary on the executor goroutine (the same
// goroutine that pushes the window during the deferred drain), so no lock.
func (c *VersionedCache) RecentlyCommittedKeys() map[string]struct{} {
	if c.invalidateDepth == 0 || len(c.recentCommittedKeys) == 0 {
		return nil
	}
	out := make(map[string]struct{})
	for _, keys := range c.recentCommittedKeys {
		for _, k := range keys {
			out[k] = struct{}{}
		}
	}
	return out
}

// pushRecentCommitted records this boundary's committed raw keys into the rolling
// invalidation window, evicting boundaries older than invalidateDepth. No-op when
// invalidateDepth == 0. Called from the deferred drain on the executor goroutine.
func (c *VersionedCache) pushRecentCommitted(keys []string) {
	if c.invalidateDepth == 0 {
		return
	}
	c.recentCommittedKeys = append(c.recentCommittedKeys, keys)
	if len(c.recentCommittedKeys) > c.invalidateDepth {
		c.recentCommittedKeys = c.recentCommittedKeys[len(c.recentCommittedKeys)-c.invalidateDepth:]
	}
}

// keysForWriters returns the raw keys of every live entry whose writerTx is in
// writers. The caller must NOT hold c.mu (this takes the read lock). Used by the
// deferred drain to snapshot the keys whose committed version advanced this
// boundary before their entries are (eventually) evicted.
func (c *VersionedCache) keysForWriters(writers []string) []string {
	if len(writers) == 0 {
		return nil
	}
	want := make(map[string]struct{}, len(writers))
	for _, id := range writers {
		want[id] = struct{}{}
	}
	var keys []string
	c.mu.RLock()
	for k, e := range c.entries {
		if _, ok := want[e.writerTx]; ok {
			keys = append(keys, k)
		}
	}
	c.mu.RUnlock()
	return keys
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
	c.dropByWriter(committed, invalidated)
	return
}

// DrainEvictionsDeferred is the pipelined-path variant of DrainEvictions. It
// applies invalidations immediately (an aborted write must never be visible to
// the authoritative pass), but holds each committed batch's writes for
// evictHoldDepth extra boundaries before evicting them (a FIFO ring, one
// committed-TxID set per boundary; see committedHeldRing and SetEvictHoldDepth).
// This closes the eviction-vs-visibility skew: the executor evicts a committed
// batch on its commit notification, but the query service reflects that commit
// only after a lag. If a just-committed key is evicted from this write cache
// before the query view catches up, a later warm pass cold-fetches it from the
// view at its STALE pre-commit version, and the pipelined auth pass (which
// inherits warm's read via the query-view clone) records that stale version --
// MVCC-aborting once the committer sees the advanced committed version, the
// historic small-batch livelock. Holding the write in the cache across the lag
// window means warm serves it from HERE (fresh: a committed batch's spec version
// equals its committed version) instead of cold-fetching stale; once it ages out
// of the ring the query view is guaranteed to reflect the commit, so the eventual
// cold-fetch is fresh too. depth 1 (the default) covers a lag under one boundary
// -- enough at large batch sizes where boundaries are slow; small batch sizes
// need a larger depth (their fast boundaries let the eviction outrun visibility).
//
// Returns the invalidated set only; committed evictions no longer drive a
// rebuild. Call ONLY at a batch boundary on the executor goroutine.
//
// Cascade interaction (rare, perf-only): committed batches leave the in-flight
// registry immediately (resolveInflight), so a rebuildCacheFromInflight after an
// invalidation reconstructs from survivors and does not restore a held-committed
// batch's writes. auth then re-reads those keys from the query view at their
// committed version -- correct, just a cache miss. Invalidations are ~0 on the
// workloads this targets, so this residual is immaterial.
func (c *VersionedCache) DrainEvictionsDeferred() (invalidated []string) {
	c.evictMu.Lock()
	committedNow := c.committed
	invalidated = c.invalidated
	c.committed, c.invalidated = nil, nil
	c.evictMu.Unlock()
	// Snapshot the keys whose committed version advances this boundary (the
	// committedNow writers, still live in c.entries -- they are only evicted after
	// the hold below) into the rolling invalidation window BEFORE any drop. These
	// are the keys an auth-view reopen must re-read fresh rather than serve from the
	// warm clone. No-op when invalidateDepth == 0.
	c.pushRecentCommitted(c.keysForWriters(committedNow))
	// Enqueue this boundary's committed set at the tail; the set that entered the
	// ring evictHoldDepth boundaries ago now ages out and is dropped alongside all
	// invalidations. A committed batch's writes therefore linger in the cache for
	// exactly evictHoldDepth boundaries after their commit is drained.
	c.committedHeldRing = append(c.committedHeldRing, committedNow)
	var aged []string
	if len(c.committedHeldRing) > c.evictHoldDepth {
		aged = c.committedHeldRing[0]
		c.committedHeldRing = c.committedHeldRing[1:]
	}
	c.dropByWriter(aged, invalidated)
	return invalidated
}

// dropByWriter deletes every cache entry whose writerTx is in any of the given
// TxID sets. Runs at the boundary on the executor goroutine; it takes c.mu for
// writing because concurrent RPC-validation Reads may be in flight (see
// VersionedCache).
func (c *VersionedCache) dropByWriter(sets ...[]string) {
	n := 0
	for _, s := range sets {
		n += len(s)
	}
	if n == 0 {
		return
	}
	drop := make(map[string]struct{}, n)
	for _, s := range sets {
		for _, id := range s {
			drop[id] = struct{}{}
		}
	}
	c.mu.Lock()
	for k, e := range c.entries {
		if _, ok := drop[e.writerTx]; ok {
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
}

// Len reports the number of in-flight entries. Test/observability helper; takes
// the read lock so it is safe to call while RPC-validation reads are in flight.
func (c *VersionedCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// ReapplySpec is one in-flight batch's writes for a cache rebuild, given in
// submission order (oldest first).
type ReapplySpec struct {
	TxID string
	RWS  blocks.ReadWriteSet
}

// Rebuild atomically replaces the cache contents with the writes of the given
// in-flight batches, applied in submission order using the same spec-version
// math as ApplyWrites. Used after a cascade invalidation: dropping an
// invalidated later writer can erase a key an earlier SURVIVING batch also
// wrote, so re-applying only the survivors from empty restores the exact
// speculative state. Because spec versions are a deterministic function of
// (read-set base, prior cached writes) and the survivors are re-applied in their
// original order, the rebuild reproduces exactly the state the survivors would
// have had if the invalidated batches had never applied. The cache only ever
// holds in-flight writes (committed reads come from the query view), so
// rebuilding from empty is complete. Runs at the batch boundary on the executor
// goroutine (like every other entries mutation); it holds c.mu for writing across
// the whole swap-and-reapply so a concurrent RPC-validation reader never observes
// a partially-rebuilt cache.
func (c *VersionedCache) Rebuild(batches []ReapplySpec) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]entry, len(c.entries))
	for _, b := range batches {
		c.apply(b.TxID, b.RWS)
	}
}
