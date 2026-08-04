/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"
	"maps"
	"os"
	"sync"

	"github.com/hyperledger/fabric-x-evm/common/pipediag"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// Store opens query-service views and hands out per-view ReadStores. It
// implements execution.KVSSnapshotter, so it drops in where the local KVS used
// to sit. blockNumber is ignored (the query service serves latest committed).
type Store struct {
	client    QueryClient
	namespace string
	// nilView, when set, skips BeginView/EndView entirely: the ReadStores it hands
	// out carry an empty viewID, so every GetRows reads CURRENT committed state on
	// the query service's non-consistent (fresh-connection) path instead of a
	// pinned, aggregation-shared snapshot. This removes the view-aggregation
	// staleness that drove the pipelined-auth abort cascade; snapshot consistency
	// within a pass is instead provided by the read-cache (View) and the write-cache
	// (VersionedCache) layered above. See GRPCClient.GetRows.
	nilView bool
}

// NewStore returns a Store reading namespace ns through client.
func NewStore(client QueryClient, ns string) *Store {
	return &Store{client: client, namespace: ns}
}

// SetNilView enables or disables nil-view mode (see Store.nilView). Call before
// the store hands out any snapshot.
func (s *Store) SetNilView(b bool) { s.nilView = b }

// NewSnapshot returns a ReadStore over committed state. In view mode it begins a
// query-service view (the caller closes it, ending the view); in nil-view mode it
// skips BeginView and returns a store with an empty viewID (reads current
// committed state per call). blockNumber is ignored.
func (s *Store) NewSnapshot(_ uint64) (execution.ReadStore, error) {
	var viewID string
	if !s.nilView {
		var err error
		viewID, err = s.client.BeginView(context.Background())
		if err != nil {
			return nil, err
		}
	}
	return &View{
		client:    s.client,
		viewID:    viewID,
		namespace: s.namespace,
		nilView:   s.nilView,
		cache:     make(map[string]*blocks.WriteRecord),
	}, nil
}

// Close closes the underlying client (call at endorser shutdown, not per read).
func (s *Store) Close() error { return s.client.Close() }

// View is a ReadStore backed by one query-service view. Reads are cached in the
// view so a key is fetched at most once and repeated reads are consistent.
type View struct {
	client    QueryClient
	viewID    string
	namespace string // the store's namespace, so Reopen can build cacheKeys for invalidation
	nilView   bool   // empty viewID: reads current committed state per call (see Store.nilView)

	mu    sync.Mutex
	cache map[string]*blocks.WriteRecord // (namespace, key) -> record; nil value means "known absent"
}

// cacheKey combines namespace and key so reads across namespaces (should the
// same View ever be used for more than one) can't collide.
func cacheKey(namespace, key string) string {
	return namespace + "\x00" + key
}

// Get returns the record for (namespace, key), or (nil, nil) if the key has no
// committed value. namespace must equal the store's namespace.
func (v *View) Get(namespace, key string) (*blocks.WriteRecord, error) {
	ck := cacheKey(namespace, key)

	v.mu.Lock()
	if rec, ok := v.cache[ck]; ok {
		v.mu.Unlock()
		return rec, nil
	}
	v.mu.Unlock()

	rows, err := v.client.GetRows(context.Background(), v.viewID, namespace, [][]byte{[]byte(key)})
	if err != nil {
		return nil, err
	}

	var rec *blocks.WriteRecord
	for _, r := range rows {
		if string(r.Key) == key {
			rec = &blocks.WriteRecord{
				Namespace: namespace,
				Key:       key,
				Version:   r.Version,
				Value:     r.Value,
			}
			break
		}
	}

	v.mu.Lock()
	v.cache[ck] = rec // caches nil for known-absent keys too
	v.mu.Unlock()

	// DIAG (default OFF, EVM_PIPE_DIAG): note this cold query-service fetch so the
	// abort classifier can tell a stale read that came through the QS (H1/H2) from
	// one served by the write cache (H3), and detect a QS version trailing an
	// already-acked commit (H1). Present keys only -- an absent key (nil rec) has
	// no real version. No-op when disabled.
	if rec != nil {
		pipediag.RecordColdFetch(key, rec.Version)
	}
	return rec, nil
}

// Reopen begins a query-service view whose viewID reflects the LATEST committed
// state, INHERITING the reads the receiver already fetched. It implements
// execution.ReopenableReadStore for the pipelined authoritative pass: warm(N+1)
// primed this view one batch boundary ago; by auth(N+1) time in-flight
// predecessors have committed, so the warm view's pinned viewID is stale. Reopen
// gives auth a FRESH viewID -- any key warm never fetched is re-resolved against
// current committed state, exactly what a serial cycle's post-boundary view
// sees -- while carrying warm's already-fetched reads forward so auth serves
// them as in-view cache hits instead of firing fresh query-service round-trips.
// Dropping that cache was the cold-read regression that made pipelined auth ~30x
// slower than serial on hot-key traffic (every auth read missed an empty cache
// and hit the query service cold).
//
// Reusing an inherited read is safe because query.View is only ever reopened
// under the write-cache layer above it (cachedView / VersionedCache -- the sole
// caller), whose read path consults the live in-flight write cache BEFORE this
// view. So warm only ever populated THIS cache on a write-cache miss: a key no
// in-flight batch had written. Such a key's committed version cannot advance
// before auth reads it -- only an in-flight batch committing could advance it,
// and that batch's write would have been a write-cache hit at warm time, so warm
// would never have fetched/cached the key here. Any key an in-flight (or
// just-committed, held) batch wrote is shadowed by the write cache and never
// served from this map. The inherited entries are therefore never stale. See
// ReopenableReadStore and TestCachedView_InflightWriteShadowsInheritedUnderRead.
//
// The clone is a shallow copy of the map (WriteRecord pointers are shared,
// immutable after caching): the returned view owns an independent map guarded by
// its own mutex, so it never races the receiver (which the caller only Closes
// after reopen -- ending its stale viewID, not touching the map). The caller
// must Close the returned view.
func (v *View) Reopen() (execution.ReadStore, error) {
	return v.ReopenInvalidating(nil)
}

// ReopenInvalidating is Reopen with SELECTIVE invalidation of the inherited
// clone (see execution.SelectiveReopenable). It clones the receiver's cache onto
// the fresh viewID, then DELETES every (namespace, key) whose raw key is in
// invalidate, so auth re-resolves exactly those keys against the fresh viewID at
// current committed state while every other warm read stays a cache hit. The
// invalidate set is the keys whose committed version may have advanced since warm
// fetched them (the eviction-vs-visibility skew) -- the only reads a plain clone
// could serve stale. A nil/empty set is identical to a full-clone Reopen.
func (v *View) ReopenInvalidating(invalidate map[string]struct{}) (execution.ReadStore, error) {
	var viewID string
	if !v.nilView {
		// View mode: a fresh viewID reflects the latest committed state, so keys
		// warm never fetched resolve against current state. In nil-view mode every
		// read already reads current committed state, so no new view is needed --
		// the reopen just carries warm's read-cache forward to auth as cache hits.
		var err error
		viewID, err = v.client.BeginView(context.Background())
		if err != nil {
			return nil, err
		}
	}
	v.mu.Lock()
	cache := maps.Clone(v.cache)
	v.mu.Unlock()
	// EXPERIMENT (bisect, remove before final fix): EVM_PIPE_NO_COLD_INHERIT
	// disables cold-read inheritance -- the reopened view starts with an EMPTY
	// cache so every auth read resolves against the fresh viewID. Isolates whether
	// carrying warm's fetched committed reads forward serves a stale value.
	if os.Getenv("EVM_PIPE_NO_COLD_INHERIT") != "" {
		cache = make(map[string]*blocks.WriteRecord)
	} else {
		for raw := range invalidate {
			delete(cache, cacheKey(v.namespace, raw))
		}
	}
	return &View{
		client:    v.client,
		viewID:    viewID,
		namespace: v.namespace,
		nilView:   v.nilView,
		cache:     cache,
	}, nil
}

// Close ends the view.
func (v *View) Close() error {
	return v.client.EndView(context.Background(), v.viewID)
}

var (
	_ execution.ReopenableReadStore = (*View)(nil)
	_ execution.SelectiveReopenable = (*View)(nil)
)
