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
	// entries is mutated only here at the boundary on the executor goroutine, so
	// the drop scan needs no lock (see VersionedCache).
	for k, e := range c.entries {
		if _, ok := drop[e.writerTx]; ok {
			delete(c.entries, k)
		}
	}
	return
}

// Len reports the number of in-flight entries. Test/observability helper; like
// the rest of the cache it must be called at the boundary (no batch in flight).
func (c *VersionedCache) Len() int {
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
// goroutine (like every other entries mutation), so no concurrent reader ever
// observes a partially-rebuilt cache.
func (c *VersionedCache) Rebuild(batches []ReapplySpec) {
	c.entries = make(map[string]entry, len(c.entries))
	for _, b := range batches {
		c.apply(b.TxID, b.RWS)
	}
}
