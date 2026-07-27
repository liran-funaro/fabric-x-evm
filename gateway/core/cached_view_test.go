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
