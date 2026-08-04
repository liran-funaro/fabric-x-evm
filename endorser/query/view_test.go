/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package query_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/endorser/query"
)

// stubClient counts GetRows calls and serves a fixed map.
type stubClient struct {
	data  map[string][]byte
	vers  map[string]uint64
	calls atomic.Int64
	ended atomic.Bool
}

func (s *stubClient) BeginView(context.Context) (string, error) { return "v", nil }
func (s *stubClient) EndView(context.Context, string) error     { s.ended.Store(true); return nil }
func (s *stubClient) Close() error                              { return nil }
func (s *stubClient) GetRows(_ context.Context, _, _ string, keys [][]byte) ([]query.Row, error) {
	s.calls.Add(1)
	var rows []query.Row
	for _, k := range keys {
		if v, ok := s.data[string(k)]; ok {
			rows = append(rows, query.Row{Key: k, Value: v, Version: s.vers[string(k)]})
		}
	}
	return rows, nil
}

// Root-cause characterization of the D=0 baseline. A plain Reopen (no
// invalidation set -- what RecentlyCommittedKeys returns when the depth knob is
// 0) clones warm's cache whole and Get consults it FIRST, so a key warm fetched
// is served at warm's version even after the ledger advances it. In the pipeline,
// warm(N+1) runs one iteration before auth(N+1); a hot key warm cold-fetched at
// committed version V can have its committed version advanced to V' > V by an
// in-flight batch that commits (and whose write the query service reflects)
// before auth reads it. The full clone serves the stale V, auth records read
// version V, the committer sees V' and MVCC-aborts -- the historic pipeline
// livelock (bisect: full clone -> 896/4000 committed, rb=2713; empty -> 4000/4000,
// rb=0). A serial cycle never hits this: warm and auth share one snapshot
// back-to-back with no commit between them.
//
// This test PINS that baseline staleness (auth serves the stale V=5). The FIX is
// NOT to empty the clone (that re-fetches everything -- ~30x slower, and stalls at
// large batch sizes) but to invalidate ONLY the keys whose committed version may
// have advanced, via ReopenInvalidating -- see
// TestViewReopenInvalidatingDropsNamedKeyKeepsOthers, which shows the same k1
// served fresh at V'=6 once it is in the invalidate set.
func TestViewReopenFullCloneCarriesStaleWarmReadBaseline(t *testing.T) {
	c := &stubClient{
		data: map[string][]byte{"k1": []byte("v1")},
		vers: map[string]uint64{"k1": 5},
	}
	store := query.NewStore(c, "evm")
	rs, err := store.NewSnapshot(0)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	// Warm cold-reads k1 at committed version 5, now cached in the warm view.
	rec, err := rs.Get("evm", "k1")
	if err != nil {
		t.Fatalf("warm Get k1: %v", err)
	}
	if rec == nil || rec.Version != 5 {
		t.Fatalf("warm k1 = %+v, want version 5", rec)
	}

	reopenable, ok := rs.(execution.ReopenableReadStore)
	if !ok {
		t.Fatalf("View does not implement ReopenableReadStore")
	}
	auth, err := reopenable.Reopen() // D=0 baseline: full clone, no invalidation
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer auth.Close()

	// Between warm and auth, an in-flight batch that wrote k1 commits and the
	// query service now serves k1 at committed version 6.
	c.vers["k1"] = 6
	c.data["k1"] = []byte("v6")

	// Baseline: auth serves k1 from the full clone at warm's STALE version 5 (not
	// the current 6). This is the read version that MVCC-aborts and livelocks the
	// pipeline -- the very staleness selective invalidation exists to remove.
	got, err := auth.Get("evm", "k1")
	if err != nil {
		t.Fatalf("auth Get k1: %v", err)
	}
	if got == nil || got.Version != 5 || string(got.Value) != "v1" {
		t.Fatalf("auth k1 = %+v, want the STALE {v1,5} baseline (full clone carries "+
			"warm's read forward); the fix invalidates such keys -- see "+
			"TestViewReopenInvalidatingDropsNamedKeyKeepsOthers", got)
	}
}

// ReopenInvalidating is the selective middle ground between the two extremes: it
// carries warm's clone forward (speed) but DROPS the keys named in the invalidate
// set (correctness), so auth re-reads exactly those against the fresh viewID while
// every other warm read stays a cache hit. This is the pipeline fix's mechanism:
// invalidate only the keys whose committed version may have advanced (the
// eviction-vs-visibility skew), not the whole clone.
//
// Warm reads k1 (v5) and k2 (v9). Reopen invalidating {k1}. Between warm and auth
// both keys' committed versions advance in the ledger, but auth must see the
// advanced k1 (invalidated -> fresh re-read) and the inherited k2 (kept -> no
// fetch): a non-invalidated key is trusted, an invalidated key is re-resolved.
func TestViewReopenInvalidatingDropsNamedKeyKeepsOthers(t *testing.T) {
	c := &stubClient{
		data: map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")},
		vers: map[string]uint64{"k1": 5, "k2": 9},
	}
	store := query.NewStore(c, "evm")
	rs, err := store.NewSnapshot(0)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	// Warm cold-reads k1 and k2 (two GetRows), both now cached in the warm view.
	if _, err := rs.Get("evm", "k1"); err != nil {
		t.Fatalf("warm Get k1: %v", err)
	}
	if _, err := rs.Get("evm", "k2"); err != nil {
		t.Fatalf("warm Get k2: %v", err)
	}
	if got := c.calls.Load(); got != 2 {
		t.Fatalf("warm GetRows calls = %d, want 2", got)
	}

	sr, ok := rs.(execution.SelectiveReopenable)
	if !ok {
		t.Fatalf("View does not implement SelectiveReopenable")
	}
	auth, err := sr.ReopenInvalidating(map[string]struct{}{"k1": {}})
	if err != nil {
		t.Fatalf("ReopenInvalidating: %v", err)
	}
	defer auth.Close()

	// Between warm and auth, in-flight batches that wrote k1 and k2 commit; the
	// query service now serves both at advanced committed versions.
	c.vers["k1"], c.data["k1"] = 6, []byte("v6")
	c.vers["k2"], c.data["k2"] = 10, []byte("v10")

	// k1 was invalidated -> auth re-reads it fresh at current committed v6.
	got1, err := auth.Get("evm", "k1")
	if err != nil {
		t.Fatalf("auth Get k1: %v", err)
	}
	if got1 == nil || got1.Version != 6 || string(got1.Value) != "v6" {
		t.Fatalf("auth k1 = %+v, want {v6,6} (invalidated -> fresh re-read)", got1)
	}
	if got := c.calls.Load(); got != 3 {
		t.Fatalf("invalidated k1 GetRows calls = %d, want 3 (one fresh fetch)", got)
	}

	// k2 was NOT invalidated -> auth serves it from the inherited clone, no fetch,
	// at warm's version 9 (its committed version could not have advanced, since the
	// caller only invalidates keys whose version may have advanced).
	got2, err := auth.Get("evm", "k2")
	if err != nil {
		t.Fatalf("auth Get k2: %v", err)
	}
	if got2 == nil || got2.Version != 9 || string(got2.Value) != "v2" {
		t.Fatalf("auth k2 = %+v, want {v2,9} from inherited clone", got2)
	}
	if got := c.calls.Load(); got != 3 {
		t.Fatalf("non-invalidated k2 triggered a fetch (calls=%d, want 3): the clone "+
			"must still serve keys outside the invalidate set", got)
	}
}

// nilViewSpy is a QueryClient that records whether BeginView was ever called and
// the viewID every GetRows was issued under, so a test can prove nil-view mode
// never begins a server-side view and always reads under an empty viewID.
type nilViewSpy struct {
	begins       atomic.Int64
	getRowsViews []string // viewID seen by each GetRows, in call order (test-serial access)
	data         map[string][]byte
	vers         map[string]uint64
}

func (s *nilViewSpy) BeginView(context.Context) (string, error) { s.begins.Add(1); return "v", nil }
func (s *nilViewSpy) EndView(context.Context, string) error     { return nil }
func (s *nilViewSpy) Close() error                              { return nil }
func (s *nilViewSpy) GetRows(_ context.Context, viewID, _ string, keys [][]byte) ([]query.Row, error) {
	s.getRowsViews = append(s.getRowsViews, viewID)
	var rows []query.Row
	for _, k := range keys {
		if v, ok := s.data[string(k)]; ok {
			rows = append(rows, query.Row{Key: k, Value: v, Version: s.vers[string(k)]})
		}
	}
	return rows, nil
}

// In nil-view mode a Store hands out ReadStores that NEVER begin a server-side
// view: NewSnapshot and Reopen both skip BeginView, and every GetRows is issued
// under an empty viewID -- the query service's non-consistent read path, which
// reads CURRENT committed state on a fresh connection instead of a snapshot
// pinned by BeginView and shared across the view-aggregation window. That pinned
// aggregation snapshot is the source of the pipelined-auth stale read; nil-view
// removes it, relying on the read-cache (View) + write-cache (VersionedCache) for
// intra-pass consistency. Reads still resolve and Reopen still inherits warm's
// cache. See Store.nilView and GRPCClient.GetRows.
func TestStoreNilViewSkipsBeginViewAndReadsUnderEmptyViewID(t *testing.T) {
	c := &nilViewSpy{
		data: map[string][]byte{"k1": []byte("v1")},
		vers: map[string]uint64{"k1": 5},
	}
	store := query.NewStore(c, "evm")
	store.SetNilView(true)

	rs, err := store.NewSnapshot(0)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	if got := c.begins.Load(); got != 0 {
		t.Fatalf("nil-view NewSnapshot called BeginView %d times, want 0", got)
	}

	// Warm reads k1 -- served from current committed state under an empty viewID.
	rec, err := rs.Get("evm", "k1")
	if err != nil {
		t.Fatalf("warm Get k1: %v", err)
	}
	if rec == nil || rec.Version != 5 || string(rec.Value) != "v1" {
		t.Fatalf("warm k1 = %+v, want {v1,5}", rec)
	}

	// Reopen for the authoritative pass -- must also skip BeginView (nil-view
	// already reads current committed state, so no fresh view is needed).
	reopenable, ok := rs.(execution.ReopenableReadStore)
	if !ok {
		t.Fatalf("View does not implement ReopenableReadStore")
	}
	auth, err := reopenable.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer auth.Close()
	if got := c.begins.Load(); got != 0 {
		t.Fatalf("nil-view Reopen called BeginView %d times, want 0", got)
	}

	// Auth serves k1 from the inherited warm cache (no new GetRows); a key warm
	// never read resolves fresh -- both still under an empty viewID.
	if _, err := auth.Get("evm", "k1"); err != nil {
		t.Fatalf("auth Get k1: %v", err)
	}
	if _, err := auth.Get("evm", "k2"); err != nil {
		t.Fatalf("auth Get k2: %v", err)
	}

	if len(c.getRowsViews) == 0 {
		t.Fatalf("no GetRows recorded")
	}
	for i, v := range c.getRowsViews {
		if v != "" {
			t.Fatalf("GetRows[%d] viewID = %q, want empty (nil-view reads current committed state)", i, v)
		}
	}
}

func TestViewGetMapsRowAndCachesAndCloses(t *testing.T) {
	c := &stubClient{
		data: map[string][]byte{"k1": []byte("v1")},
		vers: map[string]uint64{"k1": 7},
	}
	store := query.NewStore(c, "evm")
	rs, err := store.NewSnapshot(0)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	rec, err := rs.Get("evm", "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rec == nil || string(rec.Value) != "v1" || rec.Version != 7 {
		t.Fatalf("record = %+v, want value v1 version 7", rec)
	}
	// Second read of the same key must hit the in-view cache, not GetRows again.
	if _, err := rs.Get("evm", "k1"); err != nil {
		t.Fatalf("Get 2: %v", err)
	}
	if got := c.calls.Load(); got != 1 {
		t.Fatalf("GetRows calls = %d, want 1 (cached)", got)
	}
	// Absent key -> nil record, no error.
	rec2, err := rs.Get("evm", "missing")
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if rec2 != nil {
		t.Fatalf("missing key record = %+v, want nil", rec2)
	}
	if err := rs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.ended.Load() {
		t.Fatalf("Close did not call EndView")
	}
}

// A reopened view INHERITS the reads the warm pass already fetched, so the
// pipelined authoritative pass re-reads them as in-view cache hits instead of
// firing fresh query-service round-trips. Dropping the warm cache on reopen was
// the cold-read regression that made pipelined auth ~30x slower than serial
// (auth readtime 1.8s vs serial's ~4ms on hot-key traffic): every auth read
// missed the empty cache and hit the query service cold. A key the warm pass
// never read is still resolved against the reopen's FRESH viewID at current
// committed state, so genuine misses see the latest ledger.
//
// Reusing an inherited read is safe because query.View is only ever reopened
// under the write-cache layer above it (cachedView / VersionedCache -- the sole
// caller): the read path consults that live write cache FIRST, so warm only ever
// populates THIS per-view cache on a write-cache miss, i.e. for a key no
// in-flight batch wrote. Such a key's committed version cannot advance before
// auth reads it (only an in-flight batch committing could advance it, and that
// batch's write would have been a write-cache hit at warm time), so the
// inherited value is never stale. Keys an in-flight (or just-committed) batch
// wrote are shadowed by the write cache, never served from this map. See
// ReopenableReadStore and TestCachedView_InflightWriteShadowsInheritedUnderRead.
//
// RED while Reopen opens an EMPTY cache (auth re-fetches k1 -> GetRows calls==2);
// GREEN once Reopen inherits the warm cache (calls stays 1).
func TestViewReopenInheritsWarmReadsAndIsFreshForUnreadKeys(t *testing.T) {
	c := &stubClient{
		data: map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")},
		vers: map[string]uint64{"k1": 5, "k2": 9},
	}
	store := query.NewStore(c, "evm")
	rs, err := store.NewSnapshot(0)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	// Warm pass cold-reads k1 (one GetRows), now cached in the view.
	if _, err := rs.Get("evm", "k1"); err != nil {
		t.Fatalf("warm Get k1: %v", err)
	}
	if got := c.calls.Load(); got != 1 {
		t.Fatalf("warm GetRows calls = %d, want 1", got)
	}

	reopenable, ok := rs.(execution.ReopenableReadStore)
	if !ok {
		t.Fatalf("View does not implement ReopenableReadStore")
	}
	auth, err := reopenable.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer auth.Close()

	// Auth re-reads k1: it MUST be served from the INHERITED warm cache, not a
	// fresh cold fetch. This is the whole point of the fix.
	rec, err := auth.Get("evm", "k1")
	if err != nil {
		t.Fatalf("auth Get k1: %v", err)
	}
	if rec == nil || rec.Version != 5 || string(rec.Value) != "v1" {
		t.Fatalf("auth k1 = %+v, want {v1,5} from inherited cache", rec)
	}
	if got := c.calls.Load(); got != 1 {
		t.Fatalf("auth re-read of k1 triggered a fresh GetRows (calls=%d, want 1): "+
			"Reopen dropped the warm cache -> cold auth reads -> the ~30x pipeline slowdown", got)
	}

	// A key the warm pass never read is resolved against the reopen's fresh
	// viewID -> exactly one new GetRows.
	rec2, err := auth.Get("evm", "k2")
	if err != nil {
		t.Fatalf("auth Get k2: %v", err)
	}
	if rec2 == nil || rec2.Version != 9 || string(rec2.Value) != "v2" {
		t.Fatalf("auth k2 = %+v, want {v2,9} from fresh viewID", rec2)
	}
	if got := c.calls.Load(); got != 2 {
		t.Fatalf("unread key k2 GetRows calls = %d, want 2 (one fresh fetch)", got)
	}
}
