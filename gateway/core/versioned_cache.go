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
// Concurrency contract: entries (and roEvictPending) are STRUCTURALLY MUTATED
// only at the batch boundary on the single executor goroutine -- ApplyWrites,
// DrainEvictions, Rebuild, and MaintainReadOnly all run there, between batches,
// when no warm-pass worker is in flight. They are READ concurrently only during
// a batch (Read, on warm-pass workers). Reads and writes therefore never
// overlap, and the happens-before edge is supplied for free by the executor's
// wg.Wait() (joins all warm workers before the boundary mutates) and the
// go-spawn that starts the next batch (boundary writes -> spawn -> next batch's
// reads). No mutex guards entries; adding a writer OUTSIDE the boundary would
// break this invariant and require reintroducing one. Only committed/invalidated
// (queued asynchronously by notification handlers) need their own lock.
type VersionedCache struct {
	entries map[string]entry

	// committed/invalidated TxIDs queued by notification handlers off the
	// executor goroutine, applied at the next batch boundary by DrainEvictions
	// (never mid-batch). These are the only fields touched concurrently, so they
	// keep evictMu.
	evictMu     sync.Mutex
	committed   []string
	invalidated []string

	// committedHeld is the one-boundary delay buffer for committed-write eviction
	// on the pipelined path (see DrainEvictionsDeferred). Touched only at the
	// boundary on the executor goroutine, so it needs no lock.
	committedHeld []string

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
}

func NewVersionedCache() *VersionedCache {
	return &VersionedCache{entries: make(map[string]entry)}
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
	c.apply(txID, r)
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

// apply records tx's writes into c.entries at deterministic spec versions. Runs
// only at the batch boundary on the executor goroutine -- via ApplyWrites, or
// via Rebuild re-applying survivors -- so it needs no lock (see VersionedCache).
// See ApplyWrites for the spec-version rules.
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
// the authoritative pass), but holds each committed batch's writes for ONE extra
// boundary before evicting them. This preserves the pipeline's read-layering
// invariant: auth(N+1) runs one iteration after warm(N+1), which primed the
// query-view read cache while N's writes were still served from THIS write cache
// (so warm never fetched/primed them). Without the hold, the top-of-iteration
// eviction would drop N's just-committed writes before auth(N+1) reads them,
// forcing a fresh query-service fetch -- a new miss in the auth phase. Holding
// them one boundary lets auth(N+1) read them here. Reading a committed batch's
// entry is identical in effect to reading committed state: its spec version
// equals the committed version, so the recorded MVCC read-version matches
// committed and no abort results.
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
	// Apply the committed set held from the PREVIOUS boundary plus all
	// invalidations now; hold this boundary's committed set for the next call.
	c.dropByWriter(c.committedHeld, invalidated)
	c.committedHeld = committedNow
	return invalidated
}

// dropByWriter deletes every cache entry whose writerTx is in any of the given
// TxID sets. Runs at the boundary on the executor goroutine, so the scan needs
// no lock (see VersionedCache).
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
	for k, e := range c.entries {
		if _, ok := drop[e.writerTx]; ok {
			delete(c.entries, k)
		}
	}
}

// Len reports the number of in-flight entries. Test/observability helper; like
// the rest of the cache it must be called at the boundary (no batch in flight).
func (c *VersionedCache) Len() int {
	return len(c.entries)
}

// SnapshotEntries returns a frozen, independent copy of the current in-flight
// write cache: one WriteRecord per key (value + spec version), detached from the
// live cache so later ApplyWrites/eviction never mutate it. Returns nil when
// empty. Like Read/DrainEvictions it touches c.entries, so it must be called
// only at a batch boundary on the executor goroutine, when no warm-pass worker
// is in flight (see VersionedCache's concurrency contract).
//
// The pipelined executor takes this snapshot at each warm launch and hands it to
// the batch's authoritative pass (see cachedView.warmWrites): it is exactly the
// write-cache view the warm pass could read. By auth time the batches it
// captured may have committed and evicted from the live cache, and warm never
// primed those keys into the query view (it served them from the write cache),
// so the reopened committed view would re-fetch them cold. Replaying them from
// this frozen copy at their spec version -- which equals the committed version
// for a committed batch, so the recorded MVCC read-version matches committed --
// confines pipeline-induced cache misses to the warm phase, never the auth
// phase. (A captured batch that later ABORTS is safe too: the replayed value's
// stale read-version fails MVCC validation at commit, so that batch simply
// re-tries -- never a wrong commit; and its retry re-snapshots a clean cache.
// Invalidations are ~0 on the workloads this targets.)
func (c *VersionedCache) SnapshotEntries() map[string]*blocks.WriteRecord {
	if len(c.entries) == 0 {
		return nil
	}
	m := make(map[string]*blocks.WriteRecord, len(c.entries))
	for k, e := range c.entries {
		rec := e.rec // copy the record; shares the immutable Value bytes (as Read does)
		m[k] = &rec
	}
	return m
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
// goroutine (like every other entries mutation), so no concurrent reader ever
// observes a partially-rebuilt cache.
func (c *VersionedCache) Rebuild(batches []ReapplySpec) {
	c.entries = make(map[string]entry, len(c.entries))
	for _, b := range batches {
		c.apply(b.TxID, b.RWS)
	}
}
