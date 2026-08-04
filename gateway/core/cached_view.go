/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

type cachedSnapshotter struct {
	under execution.KVSSnapshotter
	cache *VersionedCache
}

// NewCachedSnapshotter layers the cross-batch VersionedCache over an underlying
// snapshotter (the query-service Store). Reads hit in-flight writes first, then
// the optional cross-batch read-only cache (hot rarely-written keys), then the
// per-batch committed view. Used as the endorser engine's KVSSnapshotter.
func NewCachedSnapshotter(under execution.KVSSnapshotter, cache *VersionedCache) execution.KVSSnapshotter {
	return &cachedSnapshotter{under: under, cache: cache}
}

func (s *cachedSnapshotter) NewSnapshot(blockNumber uint64) (execution.ReadStore, error) {
	under, err := s.under.NewSnapshot(blockNumber)
	if err != nil {
		return nil, err
	}
	// A warm-pass view captures its write-cache resolutions only when the
	// pipelined auth pass will inherit them (see cachedView.Reopen); the serial
	// default path leaves capturePipeline false and pays nothing.
	return &cachedView{cache: s.cache, under: under, capturing: s.cache.capturePipeline}, nil
}

type cachedView struct {
	cache *VersionedCache
	under execution.ReadStore

	// capturing, when true, makes Get record every key it resolves from the live
	// in-flight write cache into captured, so a reopened (auth) view can inherit
	// those resolutions (see Reopen). Set on a warm view under the pipelined
	// executor; always false on a reopened auth view and on the serial path.
	// captured is guarded by capturedMu because warm-pass workers call Get
	// concurrently; it is lazily allocated on the first write-cache hit.
	capturing  bool
	capturedMu sync.Mutex
	captured   map[string]*blocks.WriteRecord

	// inherited carries the warm view's captured write-cache resolutions forward
	// to the reopened auth view (set by Reopen; nil on warm/serial views). It is
	// consulted AFTER the live write cache and BEFORE the read-only cache / under
	// view, so any key an in-flight or just-committed batch wrote still shadows
	// it. Read-only after construction, so Get reads it without a lock.
	inherited map[string]*blocks.WriteRecord
}

func (v *cachedView) Get(namespace, key string) (*blocks.WriteRecord, error) {
	if rec, ok := v.cache.Read(key); ok {
		v.capture(key, rec) // let a reopened auth view inherit this write-cache resolution
		return rec, nil      // live in-flight write cache
	}
	if v.inherited != nil {
		if rec, ok := v.inherited[key]; ok {
			// A key warm resolved from the write cache whose in-flight writer has
			// since committed and been evicted (the pipeline straddle). Serving the
			// inherited resolution -- a committed batch's spec version equals its
			// committed version -- avoids a fresh query-service round-trip in the
			// auth phase, and is never stale: any key whose committed version could
			// have advanced since warm read it is still shadowed by the live write
			// cache above (DrainEvictionsDeferred's one-boundary hold guarantees the
			// advancing writer is still present at auth time). See Reopen.
			return rec, nil
		}
	}
	if rec, ok := v.cache.readOnlyGet(key); ok {
		return rec, nil // hot rarely-written committed record
	}
	rec, err := v.under.Get(namespace, key)
	// Offer only present, non-delete records as read-only-cache candidates:
	// absent keys read as (nil, nil), and caching a delete tombstone adds edge
	// cases (nil read-set version) for no benefit on this workload. The stale
	// window is closed by write-driven eviction (see VersionedCache.ApplyWrites).
	if err == nil && rec != nil && !rec.IsDelete {
		v.cache.readOnlyStage(key, rec)
	}
	return rec, err
}

// capture records a write-cache resolution for a reopened auth view to inherit.
// No-op unless this is a capturing (pipelined warm) view. rec is the copy Read
// already returned (immutable), so storing the pointer is safe.
func (v *cachedView) capture(key string, rec *blocks.WriteRecord) {
	if !v.capturing {
		return
	}
	v.capturedMu.Lock()
	if v.captured == nil {
		v.captured = make(map[string]*blocks.WriteRecord)
	}
	v.captured[key] = rec
	v.capturedMu.Unlock()
}

// Reopen produces a fresh cachedView layered over a reopened underlying view
// while keeping the SAME live cross-batch cache (in-flight write-cache +
// read-only MFU). It implements execution.ReopenableReadStore for the pipelined
// authoritative pass, so auth resolves its reads through four layers, in order:
// the live write-cache (still shadows in-flight hot keys across the reopen), the
// INHERITED warm write-cache resolutions (this fix -- see below), the
// write-eviction-safe read-only cache (still serves the hot cold reads warm
// staged into it), and finally the reopened underlying view.
//
// Two complementary inheritances make pipelined auth reads identical in COST to
// serial's, closing the two ways warm resolves a read that auth would otherwise
// re-fetch:
//
//   - Cold (query-view) reads: the reopened underlying view gives a key warm
//     never fetched a FRESH committed value (query.View.Reopen begins a new
//     query-service viewID) while carrying warm's already-fetched cold reads
//     forward as in-view cache hits. Dropping that was a ~30x auth-phase slowdown.
//
//   - Write-cache resolutions (this layer): a hot key warm resolved from the live
//     in-flight write cache was never fetched into the underlying view at all, so
//     the reopened view has no cached value for it. If that key's in-flight writer
//     commits and is evicted before auth reads it (the pipeline straddle), auth
//     would find it in neither the write cache nor the reopened view and fire a
//     fresh query-service round-trip -- a NEW miss in the auth phase, the residual
//     that left historic pipeline auth slower than serial even with cold-read
//     inheritance. Reopen carries warm's captured write-cache resolutions into the
//     auth view's `inherited` map so auth serves them as hits.
//
// Both inheritances are safe because this write-cache layer sits ABOVE both the
// inherited map and the underlying view: cachedView.Get consults the live write
// cache FIRST, so any key an in-flight (or just-committed, held) batch wrote is
// shadowed here. A key whose committed version could still advance after warm
// resolved it is exactly a key some batch is writing, and DrainEvictionsDeferred
// holds a just-committed batch's writes one extra boundary -- long enough that
// the advancing writer is still live in the write cache when auth reads at the
// next iteration. So an inherited value is only ever served for a key whose
// committed version cannot have advanced since warm read it, and it equals the
// committed version (a committed batch's spec version == its committed version).
// The net effect is auth reads identical in effect to serial's, at any prefetch
// depth. See query.View.Reopen, TestCachedView_AuthInheritsWarmWriteCacheResolution,
// and TestCachedView_LiveWriteCacheShadowsInheritedResolution. The underlying
// view must be reopenable (query.View is; the production read path always is).
func (v *cachedView) Reopen() (execution.ReadStore, error) {
	ru, ok := v.under.(execution.ReopenableReadStore)
	if !ok {
		return nil, fmt.Errorf("cached view: underlying read store %T is not reopenable", v.under)
	}
	// If the underlying view supports selective invalidation, hand it the keys
	// whose committed version may have advanced over the recent pipeline boundaries
	// so auth re-reads exactly those against the fresh view (see
	// execution.SelectiveReopenable and VersionedCache.RecentlyCommittedKeys). The
	// set is nil (=> a plain full-clone Reopen) whenever invalidation is disabled.
	var freshUnder execution.ReadStore
	var err error
	if sr, ok := v.under.(execution.SelectiveReopenable); ok {
		freshUnder, err = sr.ReopenInvalidating(v.cache.RecentlyCommittedKeys())
	} else {
		freshUnder, err = ru.Reopen()
	}
	if err != nil {
		return nil, err
	}
	// Hand warm's captured write-cache resolutions to the auth view. Warm has
	// fully joined by the time AuthMergedBatch reopens (a barrier separates the
	// warm pass from auth), so captured is complete and no longer mutated; the
	// lock is a cheap publish barrier. The auth view is not itself capturing.
	v.capturedMu.Lock()
	inherited := v.captured
	v.capturedMu.Unlock()
	return &cachedView{cache: v.cache, under: freshUnder, inherited: inherited}, nil
}

func (v *cachedView) Close() error { return v.under.Close() }

var _ execution.ReopenableReadStore = (*cachedView)(nil)
