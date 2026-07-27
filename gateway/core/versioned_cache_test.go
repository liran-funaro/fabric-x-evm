/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
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
func TestVersionedCache_EvictOnLatestWriterCommit(t *testing.T) {
	c := NewVersionedCache()
	c.ApplyWrites("tx1", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v1")}}))
	c.ApplyWrites("tx2", rws(nil, []blocks.KVWrite{{Key: "k", Value: []byte("v2")}}))
	c.NoteCommitted("tx1")
	c.DrainEvictions()
	if _, ok := c.Read("k"); !ok {
		t.Fatal("k evicted by non-latest-writer commit")
	}
	c.NoteCommitted("tx2")
	c.DrainEvictions()
	if _, ok := c.Read("k"); ok {
		t.Fatal("k not evicted after latest-writer commit")
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
