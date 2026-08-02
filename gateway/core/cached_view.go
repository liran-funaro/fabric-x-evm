/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"fmt"

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
	return &cachedView{cache: s.cache, under: under}, nil
}

type cachedView struct {
	cache *VersionedCache
	under execution.ReadStore

	// warmWrites is the FROZEN in-flight write snapshot the warm pass saw,
	// installed on the pipelined path only (see SetWarmWrites / Reopen and
	// VersionedCache.SnapshotEntries). It sits BELOW the live write cache and
	// ABOVE the read-only cache and query view: it serves only keys that have
	// since left the live cache (their batch committed and evicted), replaying
	// them at their spec == committed version so auth need not re-fetch from the
	// query service. nil on the serial path.
	warmWrites map[string]*blocks.WriteRecord
}

func (v *cachedView) Get(namespace, key string) (*blocks.WriteRecord, error) {
	if rec, ok := v.cache.Read(key); ok {
		return rec, nil // live in-flight write cache
	}
	if v.warmWrites != nil {
		if rec, ok := v.warmWrites[key]; ok && rec != nil {
			cp := *rec // copy; the frozen snapshot is shared across this batch's reads
			return &cp, nil
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

// Reopen produces a fresh cachedView layered over a fresh underlying view
// (latest committed state) while keeping the SAME live cross-batch cache
// (in-flight write-cache + read-only MFU). It implements
// execution.ReopenableReadStore so the pipelined authoritative pass reads
// current committed state instead of warm's now-stale pinned view: the live
// write-cache still shadows in-flight hot keys, the underlying reopened view
// reuses warm's already-fetched cold reads, and any key neither cached nor
// in-flight is re-resolved against the fresh view. The underlying view must be
// reopenable (query.View is; the production read path always is).
func (v *cachedView) Reopen() (execution.ReadStore, error) {
	ru, ok := v.under.(execution.ReopenableReadStore)
	if !ok {
		return nil, fmt.Errorf("cached view: underlying read store %T is not reopenable", v.under)
	}
	freshUnder, err := ru.Reopen()
	if err != nil {
		return nil, err
	}
	// Carry the frozen warm-pass snapshot forward: the authoritative pass reads
	// the REOPENED view, and it is precisely those already-committed-and-evicted
	// keys (which the reopened committed view would re-fetch cold) that the
	// snapshot exists to replay.
	return &cachedView{cache: v.cache, under: freshUnder, warmWrites: v.warmWrites}, nil
}

func (v *cachedView) Close() error { return v.under.Close() }

// SetWarmWrites installs the frozen warm-pass write snapshot as a read fallback
// (see the warmWrites field and execution.WarmWriteReplayer). The pipelined
// executor calls this once per warmed batch, after the warm pass has completed
// and been joined, so the warm workers never observe it -- only the auth pass
// does (via the view Reopen returns). Serial execution never calls it.
func (v *cachedView) SetWarmWrites(w map[string]*blocks.WriteRecord) { v.warmWrites = w }

var (
	_ execution.ReopenableReadStore = (*cachedView)(nil)
	_ execution.WarmWriteReplayer   = (*cachedView)(nil)
)
