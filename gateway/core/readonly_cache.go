/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// Defaults for the cross-batch read-only cache. Deliberately small: the target
// workload's hot never-written set is tiny (a handful of keys account for the
// bulk of store reads), so a few hundred slots comfortably hold it and MFU
// capacity enforcement is essentially never triggered. The admit threshold is
// the number of times a key must be READ within a single batch to earn
// admission -- large batches read a hot key hundreds of times, while the long
// tail (read once or twice over the whole run) never clears even a low bar.
const (
	DefaultReadOnlyCacheCapacity  = 256
	DefaultReadOnlyAdmitThreshold = 4
)

// roEntry is one admitted hot-read record with its frequency counter. uses is
// bumped on every cache hit -- which happens concurrently on warm-pass workers
// during a batch -- and read only at a batch boundary for MFU capacity
// enforcement, so it is an atomic (the entries map itself is never structurally
// mutated during a batch; see ReadOnlyCache).
type roEntry struct {
	rec  blocks.WriteRecord
	uses atomic.Uint64
}

// stageCand is a candidate observed during the current batch: the record last
// seen for it and how many times it was read this batch (the admission signal).
type stageCand struct {
	rec  blocks.WriteRecord
	seen uint64
}

// ReadOnlyCache is a small cross-batch cache of frequently-read, rarely-written
// committed records. It sits between the in-flight VersionedCache and the
// per-batch query view (write-cache -> read-only-cache -> query). Its retention
// policy is frequency-based (MFU): entries carry a monotonic use counter and,
// when capacity is exceeded, the LEAST-used entries are evicted so the hottest
// keys stay resident -- the opposite of an LRU's recency bias.
//
// Concurrency contract (mirrors VersionedCache): get/stage run concurrently on
// warm-pass workers DURING a batch and only ever read the entries map or append
// to the staging map; the entries map is structurally mutated (admit / evict /
// capacity-trim) ONLY by maintain, called once per cycle at the batch boundary
// on the executor goroutine, when no reader is in flight. Never mutate entries
// mid-batch.
//
// Staleness / correctness: an admitted record is served in place of a query
// read, so its version is journaled into the MVCC read-set exactly as if read
// from the view (get returns a full struct copy, preserving Version/BlockNum/
// TxNum). This is safe only while the key's committed version has not advanced.
// The executor evicts every key this gateway writes at the following boundary
// (see VersionedCache.MaintainReadOnly), which covers self-writes. A key written
// by ANOTHER gateway is not caught here; the stale read then loses its MVCC
// version check and the batch aborts -- never a bad commit, and self-correcting
// on re-execution (the same backstop the VersionedCache relies on).
//
// Known limitation: the use counter is monotonic, so a key that was hot and
// goes permanently cold keeps its rank (classic LFU staleness). The target
// workload's hot set is stable, so this is not exercised; if it ever matters,
// add periodic aging (halving) at the boundary.
type ReadOnlyCache struct {
	capacity  int
	threshold uint64

	mu      sync.RWMutex // guards entries; structural writes only at the boundary
	entries map[string]*roEntry

	stageMu sync.Mutex // guards staged; appended concurrently during a batch
	staged  map[string]*stageCand
}

// newReadOnlyCache builds an empty cache. capacity <= 0 or threshold == 0 fall
// back to the package defaults so a mis-wire cannot silently disable admission.
func newReadOnlyCache(capacity int, threshold uint64) *ReadOnlyCache {
	if capacity <= 0 {
		capacity = DefaultReadOnlyCacheCapacity
	}
	if threshold == 0 {
		threshold = DefaultReadOnlyAdmitThreshold
	}
	return &ReadOnlyCache{
		capacity:  capacity,
		threshold: threshold,
		entries:   make(map[string]*roEntry),
		staged:    make(map[string]*stageCand),
	}
}

// get returns a copy of the cached record for key and records a use, or false
// on a miss. Safe under concurrent callers (read lock + atomic bump).
func (c *ReadOnlyCache) get(key string) (*blocks.WriteRecord, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	e.uses.Add(1)
	rec := e.rec // struct copy: preserves Version/BlockNum/TxNum for the read-set
	return &rec, true
}

// stage records that key (with the present record just read from the view) was
// read this batch, so maintain can decide admission at the boundary. Callers
// pass only non-nil, non-delete records (see cachedView.Get). Concurrency-safe.
func (c *ReadOnlyCache) stage(key string, rec *blocks.WriteRecord) {
	c.stageMu.Lock()
	if cand, ok := c.staged[key]; ok {
		cand.seen++
	} else {
		c.staged[key] = &stageCand{rec: *rec, seen: 1}
	}
	c.stageMu.Unlock()
}

// maintain applies one batch boundary's structural changes, in order:
//  1. evict every key in evictKeys (its committed version just advanced because
//     this gateway wrote it) and drop any staged candidate for it (a candidate
//     seen this batch reflects the pre-write value and must not be admitted);
//  2. admit staged candidates read at least threshold times this batch, seeding
//     their use counter with that count so a freshly-admitted hot key outranks
//     older lukewarm entries in the very next step;
//  3. enforce capacity by evicting the least-used entries (MFU retention).
//
// Called ONLY at the batch boundary on the executor goroutine (no concurrent
// get/stage). Clears the staging map before returning.
func (c *ReadOnlyCache) maintain(evictKeys []string) {
	c.stageMu.Lock()
	staged := c.staged
	c.staged = make(map[string]*stageCand)
	c.stageMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Evictions from self-writes (staleness guard).
	for _, k := range evictKeys {
		delete(c.entries, k)
		delete(staged, k)
	}

	// 2. Admissions.
	for k, cand := range staged {
		if cand.seen < c.threshold {
			continue
		}
		if _, present := c.entries[k]; present {
			continue // already resident (racing re-stage); keep its accrued uses
		}
		e := &roEntry{rec: cand.rec}
		e.uses.Store(cand.seen)
		c.entries[k] = e
	}

	// 3. MFU capacity enforcement: keep the most-frequently-used.
	if len(c.entries) <= c.capacity {
		return
	}
	type ranked struct {
		key  string
		uses uint64
	}
	rows := make([]ranked, 0, len(c.entries))
	for k, e := range c.entries {
		rows = append(rows, ranked{k, e.uses.Load()})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].uses < rows[j].uses })
	for i := 0; i < len(rows)-c.capacity; i++ {
		delete(c.entries, rows[i].key)
	}
}

// len reports the number of resident entries (test/observability helper).
func (c *ReadOnlyCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
