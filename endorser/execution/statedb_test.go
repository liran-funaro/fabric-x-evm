/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
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

// TestReadCache_RepeatedReadsHitStoreOnce verifies the per-tx read cache: a key
// read many times within one transaction issues exactly ONE backing-store Get.
// Repeated reads are the norm in EVM execution -- Exist() reads bal/nonce/code,
// GetCodeHash calls Exist()+GetCode, a transfer touches each balance several
// times -- and the pinned view is immutable for the tx's lifetime, so every
// re-read of a key must be served from memory rather than a fresh ~3ms round-trip.
func TestReadCache_RepeatedReadsHitStoreOnce(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	slot := common.HexToHash("0x01")
	skey := storeKey(addr, slot)
	bkey := accKey(addr, "bal")
	absent := storeKey(addr, common.HexToHash("0x02"))

	reader := newMapReader(map[string]*blocks.WriteRecord{
		skey: {Namespace: Namespace, Key: skey, Version: 5, Value: common.HexToHash("0xAAAA").Bytes()},
		bkey: {Namespace: Namespace, Key: bkey, Version: 3, Value: uint256ToBytes(uint256.NewInt(42))},
	})
	db, err := NewStateDB(t.Context(), reader, Namespace, 1, true)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if got := db.GetState(addr, slot); got != common.HexToHash("0xAAAA") {
			t.Fatalf("read %d: storage got %v", i, got)
		}
		if got := db.GetBalance(addr); got.Uint64() != 42 {
			t.Fatalf("read %d: balance got %v", i, got)
		}
		if got := db.GetState(addr, common.HexToHash("0x02")); got != (common.Hash{}) {
			t.Fatalf("read %d: absent slot got %v", i, got)
		}
	}
	if reader.calls[skey] != 1 {
		t.Errorf("storage slot: expected 1 store Get, got %d", reader.calls[skey])
	}
	if reader.calls[bkey] != 1 {
		t.Errorf("balance: expected 1 store Get, got %d", reader.calls[bkey])
	}
	// An absent key must also be memoized -- otherwise every re-read of a
	// not-yet-existing account/slot (common: recipient of a first transfer)
	// pays a full round-trip to learn "still absent".
	if reader.calls[absent] != 1 {
		t.Errorf("absent slot: expected 1 store Get, got %d", reader.calls[absent])
	}
}

// TestReadCache_WriteShadowsCache verifies a write is still observed after a
// prior cached read, and that neither the write's prev-value lookup nor the
// post-write read issues an extra store Get.
func TestReadCache_WriteShadowsCache(t *testing.T) {
	addr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	slot := common.HexToHash("0x01")
	skey := storeKey(addr, slot)

	reader := newMapReader(map[string]*blocks.WriteRecord{
		skey: {Namespace: Namespace, Key: skey, Version: 1, Value: common.HexToHash("0x0001").Bytes()},
	})
	db, err := NewStateDB(t.Context(), reader, Namespace, 1, true)
	if err != nil {
		t.Fatal(err)
	}

	if got := db.GetState(addr, slot); got != common.HexToHash("0x0001") {
		t.Fatalf("committed read got %v", got)
	}
	db.SetState(addr, slot, common.HexToHash("0x0099")) // prev-value lookup served from cache
	if got := db.GetState(addr, slot); got != common.HexToHash("0x0099") {
		t.Fatalf("post-write read got %v (journal must shadow the cache)", got)
	}
	if reader.calls[skey] != 1 {
		t.Errorf("expected exactly 1 store Get (only the first read), got %d", reader.calls[skey])
	}
}

// TestReadCache_SurvivesRevert verifies the cache holds committed values across
// a snapshot/revert: after reverting a write, the key reads back its committed
// value with no additional store Get (the pinned view's value never changed).
func TestReadCache_SurvivesRevert(t *testing.T) {
	addr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	slot := common.HexToHash("0x01")
	skey := storeKey(addr, slot)

	reader := newMapReader(map[string]*blocks.WriteRecord{
		skey: {Namespace: Namespace, Key: skey, Version: 7, Value: common.HexToHash("0x0001").Bytes()},
	})
	db, err := NewStateDB(t.Context(), reader, Namespace, 1, true)
	if err != nil {
		t.Fatal(err)
	}

	if got := db.GetState(addr, slot); got != common.HexToHash("0x0001") {
		t.Fatalf("committed read got %v", got)
	}
	id := db.Snapshot()
	db.SetState(addr, slot, common.HexToHash("0x0099"))
	if got := db.GetState(addr, slot); got != common.HexToHash("0x0099") {
		t.Fatalf("in-snapshot read got %v", got)
	}
	db.RevertToSnapshot(id)
	if got := db.GetState(addr, slot); got != common.HexToHash("0x0001") {
		t.Fatalf("post-revert read got %v (must restore committed value)", got)
	}
	if reader.calls[skey] != 1 {
		t.Errorf("expected exactly 1 store Get across revert, got %d", reader.calls[skey])
	}
}
