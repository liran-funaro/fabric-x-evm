/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
)

func rws(reads []blocks.KVRead, writes []blocks.KVWrite) blocks.ReadWriteSet {
	return blocks.ReadWriteSet{Reads: reads, Writes: writes}
}

// First in-flight write of an ABSENT key (no read version) -> spec version 0.
func TestVersionedCache_FirstWriteAbsentKeyIsVersion0(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	rec, ok := c.Read("k")
	if !ok || rec.Version != 0 || string(rec.Value) != "v1" {
		t.Fatalf("want {v1,ver0}, got %+v ok=%v", rec, ok)
	}
}

// First in-flight write of a key read at committed version v -> spec version v+1.
func TestVersionedCache_FirstWriteExistingKeyIsBasePlus1(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(
		[]blocks.KVRead{{Key: "k", Version: &blocks.Version{BlockNum: 7}}},
		[]blocks.KVWrite{{Key: "k", Value: []byte("v1")}},
	))
	rec, _ := c.Read("k")
	if rec.Version != 8 {
		t.Fatalf("want ver 8, got %d", rec.Version)
	}
}

// A second in-flight batch writing the same key -> cachedSpec+1, retagged.
func TestVersionedCache_SecondInflightWriteIncrements(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}})) // ver 0
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}})) // ver 1
	rec, _ := c.Read("k")
	if rec.Version != 1 || string(rec.Value) != "v2" {
		t.Fatalf("want {v2,ver1}, got %+v", rec)
	}
}

// Committing tx2 (latest writer of k) evicts k; committing only tx1 does not.
// Also asserts DrainEvictions returns the applied sets, and Len tracks the
// cache size across the eviction.
func TestVersionedCache_EvictOnLatestWriterCommit(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}}))
	c.NoteCommitted("tx1")
	committed, invalidated := c.DrainEvictions()
	if len(committed) != 1 || committed[0] != "tx1" || len(invalidated) != 0 {
		t.Fatalf("want committed=[tx1] invalidated=[], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); !ok {
		t.Fatal("k evicted by non-latest-writer commit")
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("want Len()==1 (k still cached), got %d", got)
	}
	c.NoteCommitted("tx2")
	committed, invalidated = c.DrainEvictions()
	if len(committed) != 1 || committed[0] != "tx2" || len(invalidated) != 0 {
		t.Fatalf("want committed=[tx2] invalidated=[], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted after latest-writer commit")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("want Len()==0 after eviction, got %d", got)
	}
}

// Invalidating tx2 (latest writer of k) evicts k; invalidating only tx1 does
// not. Mirrors TestVersionedCache_EvictOnLatestWriterCommit but exercises the
// invalidated-eviction path, which is otherwise uncovered even though the
// interface promises eviction on "committed OR invalidated".
func TestVersionedCache_EvictOnLatestWriterInvalidate(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}}))
	c.NoteInvalidated("tx1")
	committed, invalidated := c.DrainEvictions()
	if len(invalidated) != 1 || invalidated[0] != "tx1" || len(committed) != 0 {
		t.Fatalf("want committed=[] invalidated=[tx1], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); !ok {
		t.Fatal("k evicted by non-latest-writer invalidate")
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("want Len()==1 (k still cached), got %d", got)
	}
	c.NoteInvalidated("tx2")
	committed, invalidated = c.DrainEvictions()
	if len(invalidated) != 1 || invalidated[0] != "tx2" || len(committed) != 0 {
		t.Fatalf("want committed=[] invalidated=[tx2], got committed=%v invalidated=%v", committed, invalidated)
	}
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted after latest-writer invalidate")
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("want Len()==0 after eviction, got %d", got)
	}
}

// Deletes are recorded (IsDelete) and versioned like writes.
func TestVersionedCache_Delete(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", IsDelete: true}}))
	rec, ok := c.Read("k")
	if !ok || !rec.IsDelete {
		t.Fatalf("want delete record, got %+v ok=%v", rec, ok)
	}
}

// Concurrent-access stress test: many goroutines hammer Read, ApplyWrites,
// NoteCommitted, NoteInvalidated, DrainEvictions, and Len on one shared cache
// at once. In production, warm-pass workers read the cache concurrently with
// the executor mutating it and notification handlers queueing evictions — this
// mirrors that concurrency shape. No ordering assertions: this is a pure data
// race check, meant to be run with -race.
func TestVersionedCache_ConcurrentAccess(t *testing.T) {
	c := NewVersionedCache()
	const goroutines = 8
	const iterations = 500
	const keySpace = 16

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				txID := fmt.Sprintf("tx-%d-%d", g, i)
				key := fmt.Sprintf("k%d", i%keySpace)
				switch i % 6 {
				case 0:
					c.ApplyWrites(txID, rws(nil, []blocks.KVWrite{{Key: key, Value: []byte("v")}}))
				case 1:
					c.Read(key)
				case 2:
					c.NoteCommitted(txID)
				case 3:
					c.NoteInvalidated(txID)
				case 4:
					c.DrainEvictions()
				case 5:
					c.Len()
				}
			}
		}(g)
	}
	wg.Wait()
}
