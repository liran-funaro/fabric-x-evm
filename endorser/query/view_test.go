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

// A reopened view must reflect the LATEST committed state, even for a key the
// original (warm-pass) view already fetched and cached. The pipelined
// authoritative pass reopens the warm view before recording MVCC read-versions;
// if the reopen serves a value the warm view cached BEFORE a concurrent
// in-flight batch committed an advance to that key, auth records a stale
// read-version and the committer aborts it under exact-equality MVCC. The
// authoritative pass must use a NEW view, not the warm pass's cached reads.
//
// Timeline (single hot key, no external/non-EVM traffic):
//  1. warm(N) cold-reads k at the then-committed version 5 and caches it.
//  2. an in-flight predecessor batch commits, advancing k to version 6.
//  3. auth(N) reopens the warm view and reads k -> MUST observe version 6.
//
// RED while Reopen shares the warm view's cache (returns the stale 5); GREEN
// once Reopen opens a fresh view with its own cache and re-resolves k.
func TestViewReopenReflectsLatestCommittedForCachedKey(t *testing.T) {
	c := &stubClient{
		data: map[string][]byte{"k": []byte("v5")},
		vers: map[string]uint64{"k": 5},
	}
	store := query.NewStore(c, "evm")
	rs, err := store.NewSnapshot(0)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	// (1) warm pass cold-reads k at committed version 5 (now cached in the view).
	rec, err := rs.Get("evm", "k")
	if err != nil {
		t.Fatalf("warm Get: %v", err)
	}
	if rec == nil || rec.Version != 5 {
		t.Fatalf("warm record = %+v, want version 5", rec)
	}

	// (2) an in-flight predecessor commits, advancing k to version 6.
	c.data["k"] = []byte("v6")
	c.vers["k"] = 6

	// (3) the authoritative pass reopens the view and reads k. It must see the
	// current committed version 6, not the stale 5 the warm view cached.
	reopenable, ok := rs.(execution.ReopenableReadStore)
	if !ok {
		t.Fatalf("View does not implement ReopenableReadStore")
	}
	auth, err := reopenable.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer auth.Close()

	got, err := auth.Get("evm", "k")
	if err != nil {
		t.Fatalf("auth Get: %v", err)
	}
	if got == nil || got.Version != 6 || string(got.Value) != "v6" {
		t.Fatalf("reopened view served %+v; want value v6 version 6 (current committed). "+
			"A stale version here means the auth pass reused the warm pass's cached read "+
			"instead of a fresh view -> auth records a stale MVCC read-version -> spurious abort", got)
	}
}
