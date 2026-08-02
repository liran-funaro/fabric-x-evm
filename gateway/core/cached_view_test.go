/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"testing"

	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

type fakeReader struct {
	data   map[string]*blocks.WriteRecord
	closed bool
}

func (r *fakeReader) Get(_, key string) (*blocks.WriteRecord, error) { return r.data[key], nil }
func (r *fakeReader) Close() error                                   { r.closed = true; return nil }

type fakeSnap struct{ r *fakeReader }

func (s fakeSnap) NewSnapshot(uint64) (execution.ReadStore, error) { return s.r, nil }

// reopenableFakeReader is a fakeReader that also satisfies
// execution.ReopenableReadStore: Reopen returns a fresh reader over `reopened`,
// simulating the query view advancing to newer committed state after warm's
// pinned snapshot went stale.
type reopenableFakeReader struct {
	*fakeReader
	reopened map[string]*blocks.WriteRecord
}

func (r *reopenableFakeReader) Reopen() (execution.ReadStore, error) {
	return &fakeReader{data: r.reopened}, nil
}

// Reopen re-resolves keys against the FRESH reopened underlying view (latest
// committed state), NOT against warm's now-stale pinned snapshot. This is the
// crux of the pipelined-auth read fix: a key advanced by an in-flight commit
// since warm read it must be re-fetched at its current committed version, so
// pipelined auth is read-identical to serial auth at any prefetch depth. The
// live cross-batch write cache is still shared across Reopen (an in-flight hot
// key stays shadowed); only the per-view stale read cache is dropped.
func TestCachedView_ReopenResolvesAgainstFreshUnder(t *testing.T) {
	under := &reopenableFakeReader{
		fakeReader: &fakeReader{data: map[string]*blocks.WriteRecord{"k": {Key: "k", Value: []byte("v-stale"), Version: 3}}},
		reopened:   map[string]*blocks.WriteRecord{"k": {Key: "k", Value: []byte("v-fresh"), Version: 4}},
	}
	cache := NewVersionedCache()
	cache.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "h", Value: []byte("live-h")}})) // ver 0
	v := &cachedView{cache: cache, under: under}
	fresh, err := v.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	// The shared live write cache still shadows an in-flight hot key across Reopen.
	rec, _ := fresh.Get("ns", "h")
	if rec == nil || string(rec.Value) != "live-h" || rec.Version != 0 {
		t.Fatalf("Reopen must keep the shared live write cache; got %+v", rec)
	}
	// A key not in the live cache resolves against the FRESH reopened under at its
	// current committed version -- never warm's stale pinned value.
	rec, _ = fresh.Get("ns", "k")
	if rec == nil || string(rec.Value) != "v-fresh" || rec.Version != 4 {
		t.Fatalf("want {v-fresh,4} from reopened under, got %+v", rec)
	}
}

func TestCachedView_CacheHitShadowsUnderlying(t *testing.T) {
	under := &fakeReader{data: map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("committed"), Version: 3},
	}}
	cache := NewVersionedCache()
	cache.ApplyWrites("tx1", rws(
		[]blocks.KVRead{{Key: "k", Version: &blocks.Version{BlockNum: 3}}},
		[]blocks.KVWrite{{Key: "k", Value: []byte("inflight")}},
	)) // spec version 4
	snap := NewCachedSnapshotter(fakeSnap{r: under}, cache)
	view, _ := snap.NewSnapshot(0)
	rec, _ := view.Get("ns", "k")
	if string(rec.Value) != "inflight" || rec.Version != 4 {
		t.Fatalf("want {inflight,4}, got %+v", rec)
	}
}

func TestCachedView_MissFallsThroughAndCloses(t *testing.T) {
	under := &fakeReader{data: map[string]*blocks.WriteRecord{"k": {Key: "k", Value: []byte("committed"), Version: 3}}}
	snap := NewCachedSnapshotter(fakeSnap{r: under}, NewVersionedCache())
	view, _ := snap.NewSnapshot(0)
	rec, _ := view.Get("ns", "k")
	if string(rec.Value) != "committed" {
		t.Fatalf("want committed, got %+v", rec)
	}
	_ = view.Close()
	if !under.closed {
		t.Fatal("Close not propagated")
	}
}
