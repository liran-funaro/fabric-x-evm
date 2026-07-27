/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// mapReader is a ReadStore backed by an in-memory map of committed records,
// keyed by KVS key regardless of namespace (tests here use a single
// namespace). It counts Get calls per key so a test can assert the new
// Result() backfill (see TestResult_*) does not re-fetch a key that already
// has a journaled read.
type mapReader struct {
	records map[string]*blocks.WriteRecord
	calls   map[string]int
}

func newMapReader(records map[string]*blocks.WriteRecord) *mapReader {
	if records == nil {
		records = map[string]*blocks.WriteRecord{}
	}
	return &mapReader{records: records, calls: make(map[string]int)}
}

func (r *mapReader) Get(_, key string) (*blocks.WriteRecord, error) {
	r.calls[key]++
	return r.records[key], nil
}

func (r *mapReader) Close() error { return nil }

// findRead returns a pointer to the KVRead for key, or nil if absent.
func findRead(reads []blocks.KVRead, key string) *blocks.KVRead {
	for i := range reads {
		if reads[i].Key == key {
			return &reads[i]
		}
	}
	return nil
}

// TestResult_BlindWriteBackfillsCommittedVersion is the Task 1a RED/GREEN
// test: a write to key k with NO prior read (a "blind write") must still
// carry a versioned KVRead in Result().Reads, so downstream spec-version
// derivation (VersionedCache.ApplyWrites) has a base version to work from
// instead of silently defaulting to "no dependency" (spec version 0).
func TestResult_BlindWriteBackfillsCommittedVersion(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	slot := common.HexToHash("0x01")
	key := storeKey(addr, slot)

	reader := newMapReader(map[string]*blocks.WriteRecord{
		key: {Namespace: Namespace, Key: key, Version: 5, Value: common.HexToHash("0xAAAA").Bytes()},
	})

	db, err := NewStateDB(t.Context(), reader, Namespace, 1, true)
	if err != nil {
		t.Fatal(err)
	}

	// Blind write: SetState with NO prior GetState call on this key.
	db.SetState(addr, slot, common.HexToHash("0xBBBB"))

	rws := db.Result()

	read := findRead(rws.Reads, key)
	if read == nil {
		t.Fatalf("expected Result().Reads to contain the blindly-written key %q, got %v", key, rws.Reads)
	}
	if read.Version == nil {
		t.Fatalf("expected a non-nil version for key %q (committed record exists), got nil", key)
	}
	if read.Version.BlockNum != 5 {
		t.Errorf("expected version {BlockNum:5}, got %+v", read.Version)
	}
}

// TestResult_BlindWriteAbsentKeyGetsNilVersion covers the "key never
// committed" branch of the backfill: CreateAccount blindly writes bal/nonce
// with no prior read, and the store has nothing for that key, so the
// backfilled KVRead must carry a nil Version (matching getStateFromStore's
// existing "absent key" semantics), not a zero-value stand-in.
func TestResult_BlindWriteAbsentKeyGetsNilVersion(t *testing.T) {
	addr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	reader := newMapReader(nil) // nothing committed anywhere

	db, err := NewStateDB(t.Context(), reader, Namespace, 1, true)
	if err != nil {
		t.Fatal(err)
	}

	db.CreateAccount(addr) // blind writes: bal=0, nonce=0, no prior reads

	rws := db.Result()

	balKey := accKey(addr, "bal")
	read := findRead(rws.Reads, balKey)
	if read == nil {
		t.Fatalf("expected Result().Reads to contain backfilled key %q", balKey)
	}
	if read.Version != nil {
		t.Errorf("expected nil version for never-committed key %q, got %+v", balKey, read.Version)
	}
}

// TestResult_ExistingReadIsNotOverwrittenOrRefetched covers the dedup
// requirement: a key that already has a journaled read (from an explicit
// GetState) must be left untouched by the backfill -- neither its version
// changed, nor the store re-queried a second time for it.
func TestResult_ExistingReadIsNotOverwrittenOrRefetched(t *testing.T) {
	addr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	slot := common.HexToHash("0x01")
	key := storeKey(addr, slot)

	reader := newMapReader(map[string]*blocks.WriteRecord{
		key: {Namespace: Namespace, Key: key, Version: 5, Value: common.HexToHash("0xAAAA").Bytes()},
	})

	db, err := NewStateDB(t.Context(), reader, Namespace, 1, true)
	if err != nil {
		t.Fatal(err)
	}

	_ = db.GetState(addr, slot)                          // journals a read at version 5 (store.Get call #1)
	db.putState(key, common.HexToHash("0xCCCC").Bytes()) // pure blind write to the SAME key, no internal store access

	rws := db.Result()

	read := findRead(rws.Reads, key)
	if read == nil || read.Version == nil || read.Version.BlockNum != 5 {
		t.Fatalf("expected the pre-existing read to survive untouched at version 5, got %v", read)
	}
	if got := reader.calls[key]; got != 1 {
		t.Errorf("expected exactly 1 store fetch for already-read key %q (no backfill re-fetch), got %d", key, got)
	}
}
