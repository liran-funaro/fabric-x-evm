/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query

import (
	"context"
	"sync"

	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// Store opens query-service views and hands out per-view ReadStores. It
// implements execution.KVSSnapshotter, so it drops in where the local KVS used
// to sit. blockNumber is ignored (the query service serves latest committed).
type Store struct {
	client    QueryClient
	namespace string
}

// NewStore returns a Store reading namespace ns through client.
func NewStore(client QueryClient, ns string) *Store {
	return &Store{client: client, namespace: ns}
}

// NewSnapshot begins a view and returns it as a ReadStore. The caller closes it
// (which ends the view). blockNumber is ignored.
func (s *Store) NewSnapshot(_ uint64) (execution.ReadStore, error) {
	viewID, err := s.client.BeginView(context.Background())
	if err != nil {
		return nil, err
	}
	return &View{
		client: s.client,
		viewID: viewID,
		cache:  make(map[string]*blocks.WriteRecord),
	}, nil
}

// Close closes the underlying client (call at endorser shutdown, not per read).
func (s *Store) Close() error { return s.client.Close() }

// View is a ReadStore backed by one query-service view. Reads are cached in the
// view so a key is fetched at most once and repeated reads are consistent.
type View struct {
	client QueryClient
	viewID string

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
	return rec, nil
}

// Reopen begins a FRESH query-service view -- reflecting the latest committed
// state -- that SHARES this view's read cache. Cached reads (committed values
// this view already fetched) are reused as-is; only keys not yet cached hit the
// new view. It implements execution.ReopenableReadStore for the pipelined
// authoritative pass: warm(N) primed this view against a snapshot that is now
// one batch-boundary stale, so auth must re-resolve any UNCACHED read against
// current committed state, while still reusing warm's cached cold reads for
// speed. See execution.ReopenableReadStore for why the shared cache is safe.
//
// The returned view SHARES the cache map (not a copy): warm(N) has completed and
// been joined before auth(N) reopens, and warm(N+1) uses its own separate view,
// so auth is the sole accessor of this map -- the fresh view's own mutex guards
// its (single-goroutine) reads and any new admissions. The caller must Close the
// returned view; Close-ing the original ends only the original's (stale) viewID.
func (v *View) Reopen() (execution.ReadStore, error) {
	viewID, err := v.client.BeginView(context.Background())
	if err != nil {
		return nil, err
	}
	return &View{client: v.client, viewID: viewID, cache: v.cache}, nil
}

// Close ends the view.
func (v *View) Close() error {
	return v.client.EndView(context.Background(), v.viewID)
}

var _ execution.ReopenableReadStore = (*View)(nil)
