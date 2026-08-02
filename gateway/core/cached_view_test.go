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

// The frozen warm-pass write snapshot is consulted after a live-cache MISS and
// before the underlying view, so auth can replay an in-flight write the warm
// pass saw even once it has committed and evicted from the live cache.
func TestCachedView_WarmWritesServedAfterLiveCacheMiss(t *testing.T) {
	under := &fakeReader{data: map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("committed-old"), Version: 3},
	}}
	v := &cachedView{cache: NewVersionedCache(), under: under} // live cache empty (k evicted)
	v.SetWarmWrites(map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("warm-inflight"), Version: 4},
	})
	rec, _ := v.Get("ns", "k")
	if rec == nil || string(rec.Value) != "warm-inflight" || rec.Version != 4 {
		t.Fatalf("want warmWrites fallback {warm-inflight,4}, got %+v", rec)
	}
}

// The live write cache shadows the frozen snapshot: a key still in flight is
// served from the live cache, never from warmWrites.
func TestCachedView_LiveCacheShadowsWarmWrites(t *testing.T) {
	cache := NewVersionedCache()
	cache.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("live")}})) // ver 0
	v := &cachedView{cache: cache, under: &fakeReader{data: map[string]*blocks.WriteRecord{}}}
	v.SetWarmWrites(map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("stale-warm"), Version: 9},
	})
	rec, _ := v.Get("ns", "k")
	if rec == nil || string(rec.Value) != "live" || rec.Version != 0 {
		t.Fatalf("live cache must shadow warmWrites; got %+v", rec)
	}
}

// A key absent from warmWrites falls through to the underlying view.
func TestCachedView_WarmWritesMissFallsThroughToUnder(t *testing.T) {
	under := &fakeReader{data: map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("committed"), Version: 3},
	}}
	v := &cachedView{cache: NewVersionedCache(), under: under}
	v.SetWarmWrites(map[string]*blocks.WriteRecord{
		"other": {Key: "other", Value: []byte("x"), Version: 1},
	})
	rec, _ := v.Get("ns", "k")
	if rec == nil || string(rec.Value) != "committed" {
		t.Fatalf("want committed from under, got %+v", rec)
	}
}

// Reopen carries the frozen warm-pass snapshot onto the fresh view (so the
// pipelined auth pass, which reads the REOPENED view, still replays it) while
// resolving uncaptured keys against the fresh underlying committed state.
func TestCachedView_ReopenCarriesWarmWrites(t *testing.T) {
	under := &reopenableFakeReader{
		fakeReader: &fakeReader{data: map[string]*blocks.WriteRecord{"k": {Key: "k", Value: []byte("v-stale"), Version: 3}}},
		reopened:   map[string]*blocks.WriteRecord{"k": {Key: "k", Value: []byte("v-fresh"), Version: 3}},
	}
	v := &cachedView{cache: NewVersionedCache(), under: under}
	v.SetWarmWrites(map[string]*blocks.WriteRecord{
		"h": {Key: "h", Value: []byte("warm-h"), Version: 7},
	})
	fresh, err := v.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	// h is carried across Reopen from the frozen snapshot...
	rec, _ := fresh.Get("ns", "h")
	if rec == nil || string(rec.Value) != "warm-h" || rec.Version != 7 {
		t.Fatalf("Reopen must carry warmWrites; got %+v", rec)
	}
	// ...and k resolves against the FRESH reopened underlying view.
	rec, _ = fresh.Get("ns", "k")
	if rec == nil || string(rec.Value) != "v-fresh" {
		t.Fatalf("want v-fresh from reopened under, got %+v", rec)
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
