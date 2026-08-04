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

// Safety backstop for the inherited-cache Reopen (see query.View.Reopen): even
// if the reopened under-view carries a STALE committed read for a key -- as the
// real query.View now does, inheriting warm's fetched reads -- an in-flight
// write to that key shadows it, because cachedView.Get consults the live
// cross-batch write cache BEFORE the under-view. In practice warm never caches
// an in-flight key in the under-view (it hits the write cache first, so it never
// fetches/primes it), but this guards the layering invariant directly: the write
// cache is the source of truth for any key an in-flight or just-committed batch
// wrote, regardless of what the inherited under-view holds.
func TestCachedView_InflightWriteShadowsInheritedUnderRead(t *testing.T) {
	// under.Reopen INHERITS its data (models query.View carrying warm's reads):
	// the reopened reader still holds the stale committed value for k.
	stale := map[string]*blocks.WriteRecord{"k": {Key: "k", Value: []byte("v-stale"), Version: 3}}
	under := &reopenableFakeReader{
		fakeReader: &fakeReader{data: stale},
		reopened:   stale, // same map -> inherited, NOT swapped to fresh committed
	}
	cache := NewVersionedCache()
	cache.ApplyWrites("tx1", rws(
		[]blocks.KVRead{{Key: "k", Version: &blocks.Version{BlockNum: 3}}},
		[]blocks.KVWrite{{Key: "k", Value: []byte("inflight")}},
	)) // spec version 4
	v := &cachedView{cache: cache, under: under}
	fresh, err := v.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	rec, _ := fresh.Get("ns", "k")
	if rec == nil || string(rec.Value) != "inflight" || rec.Version != 4 {
		t.Fatalf("in-flight write must shadow the inherited under read; got %+v", rec)
	}
}

// recordingReader is a ReadStore that records every key it is asked for, so a
// test can prove which reads reached the underlying (cold) view vs were served
// from a higher cache layer.
type recordingReader struct {
	data map[string]*blocks.WriteRecord
	got  []string
}

func (r *recordingReader) Get(_, key string) (*blocks.WriteRecord, error) {
	r.got = append(r.got, key)
	return r.data[key], nil
}
func (r *recordingReader) Close() error { return nil }

func (r *recordingReader) fetched(key string) bool {
	for _, k := range r.got {
		if k == key {
			return true
		}
	}
	return false
}

// reopenToRecording is a reopenable reader whose Reopen returns a fixed
// recordingReader, so the test inspects exactly which keys auth cold-fetched
// from the reopened (fresh) underlying view.
type reopenToRecording struct {
	*fakeReader
	rec *recordingReader
}

func (r *reopenToRecording) Reopen() (execution.ReadStore, error) { return r.rec, nil }

// The pipelined-auth straddle: warm(N+1) resolves a hot key K from the live
// in-flight write cache, so it never fetches K from the underlying query view.
// By auth(N+1) time K's writer has committed and been evicted from the write
// cache (the far edge of DrainEvictionsDeferred's one-boundary hold). Without
// inheriting warm's write-cache resolution, auth would find K in neither the
// write cache nor the (cold -- warm never primed it) reopened view -> a fresh
// query-service round-trip: a NEW miss introduced in the auth phase, the read-
// layering bug that made historic pipeline auth slower than serial. This test
// proves auth instead serves K from the inherited warm resolution and never
// touches the reopened underlying view for K, while a key warm never read still
// resolves cold against the fresh view.
func TestCachedView_AuthInheritsWarmWriteCacheResolution(t *testing.T) {
	// Reopened (fresh) view: has the now-committed K, plus a divergent key d that
	// warm never read. recordingReader tracks which keys auth actually fetches.
	reopened := &recordingReader{data: map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("v5-committed"), Version: 5},
		"d": {Key: "d", Value: []byte("d-cold"), Version: 2},
	}}
	under := &reopenToRecording{
		fakeReader: &fakeReader{data: map[string]*blocks.WriteRecord{}},
		rec:        reopened,
	}
	cache := NewVersionedCache()
	cache.SetCaptureForReopen(true) // pipeline mode: warm views capture their write-cache resolutions
	// In-flight writer of K: read K@4, wrote it -> spec version 5.
	cache.ApplyWrites("txM", rws(
		[]blocks.KVRead{{Key: "k", Version: &blocks.Version{BlockNum: 4}}},
		[]blocks.KVWrite{{Key: "k", Value: []byte("v5")}},
	))

	// WARM: read K -> resolved from the live write cache (spec 5); capture it.
	warm := &cachedView{cache: cache, under: under, capturing: cache.capturePipeline}
	rec, _ := warm.Get("ns", "k")
	if rec == nil || rec.Version != 5 || string(rec.Value) != "v5" {
		t.Fatalf("warm: want write-cache hit {v5,5}, got %+v", rec)
	}

	// Reopen for the AUTH pass (the pipeline-only path).
	authV, err := warm.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}

	// STRADDLE: K's writer commits and is evicted from the write cache before auth
	// reads K (two deferred drains: the first moves txM into the hold, the second
	// evicts it).
	cache.NoteCommitted("txM")
	cache.DrainEvictionsDeferred()
	cache.DrainEvictionsDeferred()
	if _, ok := cache.Read("k"); ok {
		t.Fatal("precondition: txM must be evicted from the write cache")
	}

	// AUTH reads K: served from the inherited warm resolution (spec 5 == committed
	// 5), NOT a cold fetch from the reopened view.
	rec, _ = authV.Get("ns", "k")
	if rec == nil || rec.Version != 5 || string(rec.Value) != "v5" {
		t.Fatalf("auth: want inherited {v5,5}, got %+v", rec)
	}
	if reopened.fetched("k") {
		t.Fatal("auth cold-fetched k from the reopened view; must serve it from the inherited warm resolution")
	}

	// A key warm never read is NOT inherited: auth resolves it cold against the
	// fresh reopened view (correct, and unavoidable).
	rec, _ = authV.Get("ns", "d")
	if rec == nil || string(rec.Value) != "d-cold" {
		t.Fatalf("auth: want cold {d-cold} from reopened view, got %+v", rec)
	}
	if !reopened.fetched("d") {
		t.Fatal("auth must cold-fetch a divergent key warm never read")
	}
}

// Safety of the inherited warm-resolution layer: if warm resolved K from the
// write cache (captured) but an in-flight writer has since ADVANCED K to a newer
// spec version still live in the write cache (DrainEvictionsDeferred's one-
// boundary hold guarantees a just-committed advancing writer is still present at
// auth time), auth must read the FRESH write-cache version, not the stale
// inherited one -- so no stale MVCC read-version and no abort. cachedView.Get
// consults the live write cache BEFORE the inherited layer.
func TestCachedView_LiveWriteCacheShadowsInheritedResolution(t *testing.T) {
	reopened := &recordingReader{data: map[string]*blocks.WriteRecord{
		"k": {Key: "k", Value: []byte("v5-committed"), Version: 5},
	}}
	under := &reopenToRecording{
		fakeReader: &fakeReader{data: map[string]*blocks.WriteRecord{}},
		rec:        reopened,
	}
	cache := NewVersionedCache()
	cache.SetCaptureForReopen(true)
	cache.ApplyWrites("txM", rws(
		[]blocks.KVRead{{Key: "k", Version: &blocks.Version{BlockNum: 4}}},
		[]blocks.KVWrite{{Key: "k", Value: []byte("v5")}},
	)) // spec 5

	warm := &cachedView{cache: cache, under: under, capturing: cache.capturePipeline}
	if _, err := warm.Get("ns", "k"); err != nil { // capture K@5
		t.Fatalf("warm Get: %v", err)
	}

	authV, err := warm.Reopen()
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}

	// An in-flight writer advances K to spec 6 (models the just-committed advancing
	// writer still held live in the write cache at auth time).
	cache.ApplyWrites("txM2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v6")}})) // cachedSpec+1 = 6

	rec, _ := authV.Get("ns", "k")
	if rec == nil || rec.Version != 6 || string(rec.Value) != "v6" {
		t.Fatalf("live write cache must shadow the inherited resolution; want {v6,6}, got %+v", rec)
	}
	if reopened.fetched("k") {
		t.Fatal("auth must not cold-fetch k when the live write cache shadows it")
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
