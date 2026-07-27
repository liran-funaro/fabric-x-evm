/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

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
