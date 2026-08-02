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
}

func (v *cachedView) Get(namespace, key string) (*blocks.WriteRecord, error) {
	if rec, ok := v.cache.Read(key); ok {
		return rec, nil // live in-flight write cache
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
// write-cache still shadows in-flight hot keys, the write-eviction-safe
// read-only cache still serves the hot cold reads warm staged into it, and any
// key in neither is re-resolved against the fresh underlying view. Crucially the
// fresh view carries NO stale read cache of its own (query.View.Reopen opens an
// empty one), so a key advanced by an in-flight commit since warm read it is
// re-fetched at its current committed version -- not served stale. This makes
// pipelined auth read-identical to serial auth at ANY prefetch depth. The
// underlying view must be reopenable (query.View is; the production read path
// always is).
func (v *cachedView) Reopen() (execution.ReadStore, error) {
	ru, ok := v.under.(execution.ReopenableReadStore)
	if !ok {
		return nil, fmt.Errorf("cached view: underlying read store %T is not reopenable", v.under)
	}
	freshUnder, err := ru.Reopen()
	if err != nil {
		return nil, err
	}
	return &cachedView{cache: v.cache, under: freshUnder}, nil
}

func (v *cachedView) Close() error { return v.under.Close() }

var _ execution.ReopenableReadStore = (*cachedView)(nil)
