/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

// Package endorser implements the EVM state management for Fabric.
//
// UNIFIED STATEDB ARCHITECTURE:
//
// StateDB is a unified implementation that combines ledger state management
// with EVM-specific state tracking. It implements the vm.StateDB interface
// and uses a single journal for all operations.
//
// Key design principles:
// 1. Single journal tracks ALL operations (reads, writes, logs, refunds, selfDestruct)
// 2. Journal is replayed in Result() to build the final read-write set
// 3. Only reads from the underlying ReadStore create MVCC read dependencies
// 4. Blind writes (writes without prior reads) are supported
// 5. Snapshot/revert truncates the journal - simple and efficient
//
// The journal contains different entry types:
// - readEntry: Records a read from the ReadStore (creates MVCC dependency)
// - writeEntry: Records a write operation
// - refundEntry: Records gas refund changes
// - selfDestructEntry: Records SELFDESTRUCT operations
//
// When Result() is called, the journal is replayed to build:
// - Read-write set for Fabric (only reads from ReadStore, all writes)
// - Final state for queries (balance, nonce, code, storage)
//
// GetStateAndCommittedState is handled by querying the ReadStore directly
// for committed state, and replaying the journal for current state.
package execution

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	ethstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/bal"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/holiman/uint256"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// Log is a type of event.
type Log struct {
	Address []byte
	Topics  [][]byte
	Data    []byte
}

// ReadStore is the read interface required to back a StateDB.
type ReadStore interface {
	Get(namespace, key string) (*blocks.WriteRecord, error)
	Close() error
}

// ReopenableReadStore is a ReadStore that can spawn a FRESH snapshot reflecting
// the LATEST committed state, with NO reads carried over from the store it was
// reopened from. The pipelined authoritative pass uses it: warm(N) primed its
// snapshot one batch boundary ago, so by the time auth(N) runs, in-flight
// predecessor batches have committed and advanced the ledger and that snapshot
// is stale. Reopen gives auth a clean view so it re-resolves every read against
// current committed state -- exactly what a serial cycle's post-boundary view
// would see. It must NOT reuse warm's already-fetched reads: a key warm
// cold-read at some version can be advanced by an in-flight commit before auth
// records its MVCC read-version, and reusing the stale value would make the
// committer abort the batch under exact-equality MVCC (the stale-read livelock).
// Safe cross-batch reuse of hot reads is provided elsewhere -- by the live
// in-flight write-cache (which shadows this store and is re-read live per tx) and
// the write-eviction-safe read-only cache -- not by carrying this store's reads
// across a reopen.
type ReopenableReadStore interface {
	ReadStore
	Reopen() (ReadStore, error)
}

// revision represents a snapshot point in the journal: the lengths of the
// reads, writes, and effects slices (plus logs) at Snapshot() time, so
// RevertToSnapshot can truncate each back and reverse-replay the effects since.
type revision struct {
	id          int
	readIndex   int
	writeIndex  int
	effectIndex int
	logIndex    int
}

// accessList tracks addresses and storage slots accessed during transaction execution.
// This is used for EIP-2929 (gas cost increases for state access opcodes)
// and EIP-2930 (optional access lists in transactions).
// The access list is NOT persisted - it's reset at the start of each transaction.
type accessList struct {
	addresses map[common.Address]int
	slots     []map[common.Hash]struct{}
}

// newAccessList creates a new access list.
func newAccessList() *accessList {
	return &accessList{
		addresses: make(map[common.Address]int),
	}
}

// containsAddress checks if an address is in the access list.
func (al *accessList) containsAddress(addr common.Address) bool {
	_, ok := al.addresses[addr]
	return ok
}

// contains checks if a slot is in the access list.
func (al *accessList) contains(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	idx, ok := al.addresses[addr]
	if !ok {
		return false, false
	}
	if idx == -1 {
		// address present but no slots
		return true, false
	}
	_, slotOk = al.slots[idx][slot]
	return true, slotOk
}

// addAddress adds an address to the access list.
func (al *accessList) addAddress(addr common.Address) {
	if _, present := al.addresses[addr]; !present {
		al.addresses[addr] = -1
	}
}

// addSlot adds a slot to the access list.
func (al *accessList) addSlot(addr common.Address, slot common.Hash) {
	idx, addrPresent := al.addresses[addr]
	if !addrPresent || idx == -1 {
		// Address not present, or addr present but no slots
		al.addresses[addr] = len(al.slots)
		slotmap := map[common.Hash]struct{}{slot: {}}
		al.slots = append(al.slots, slotmap)
		return
	}
	// Address already has slots
	al.slots[idx][slot] = struct{}{}
}

// deleteAddress removes an address from the access list.
// This is used when reverting snapshots.
func (al *accessList) deleteAddress(addr common.Address) {
	delete(al.addresses, addr)
}

// reset empties the access list for reuse across transactions, keeping the
// backing map and slot-slice capacity so Prepare no longer allocates a fresh
// access list (newAccessList was a top allocator) on every tx.
func (al *accessList) reset() {
	clear(al.addresses)
	al.slots = al.slots[:0]
}

// deleteSlot removes a storage slot from the access list.
// This is used when reverting snapshots.
func (al *accessList) deleteSlot(addr common.Address, slot common.Hash) {
	idx, addrPresent := al.addresses[addr]
	if !addrPresent {
		return
	}
	if idx == -1 {
		// Address present but no slots - nothing to delete
		return
	}
	delete(al.slots[idx], slot)
}

// The journal was a single []any (boxing every read/write/effect into an
// interface = one heap allocation per entry -- the #2 allocator in the profile).
// It is now three un-boxed, purpose-built value slices:
//
//   - reads/writes: the hot path. Reads and writes have disjoint payloads and
//     are consumed independently (getStateFromJournal only inspects writes;
//     Result folds each into its own map; revert truncates both), so keeping
//     them apart lets each record hold only its own fields -- far leaner than
//     one union struct. The MVCC read version is stored inline (value, not
//     *blocks.Version) so a read no longer allocates a pointer; the single
//     *blocks.Version is materialized once per unique key in Result().
//   - effects: the rare EVM-mechanics entries (refund, SELFDESTRUCT, EIP-6780
//     new-contract, EIP-1153 transient, EIP-2929 access list) that exist only
//     to be undone by RevertToSnapshot. Their mutual order (per kind) is
//     preserved, which is all revert needs; they never interleave with the
//     read/write sets semantically.
//
// A revision records the length of all three slices (plus logs) so a snapshot
// can be reverted by truncating reads/writes/effects and reverse-replaying the
// effects added since.
type effectKind uint8

const (
	effRefund       effectKind = iota // gas refund change (carries prevRefund)
	effSelfDestruct                   // SELFDESTRUCT (carries addr)
	effNewContract                    // address added to newContracts (EIP-6780)
	effTransient                      // transient storage change (EIP-1153)
	effAccessAddr                     // access-list address addition (EIP-2929)
	effAccessSlot                     // access-list slot addition (EIP-2929)
)

// readRec is one journaled read: the KVS key plus its inline MVCC version
// (hasVersion is false when the key was absent, i.e. a nil version in the set).
type readRec struct {
	key        string
	version    blocks.Version
	hasVersion bool
}

// cachedRead is one memoized store.Get result held in StateDB.readCache. rec is
// the record the pinned view returned for the key and may be nil (key absent in
// the view); presence of the entry in the map -- not rec != nil -- is what marks
// the key as already read. See the readCache field for the full rationale.
type cachedRead struct {
	rec *blocks.WriteRecord
}

// writeRec is one journaled write or delete.
type writeRec struct {
	key      string
	value    []byte
	isDelete bool
}

// effectRec is one reversible EVM-mechanics change (see the effects slice above).
type effectRec struct {
	kind       effectKind
	addr       common.Address // selfDestruct / newContract / access / transient account
	hash       common.Hash    // effAccessSlot: slot; effTransient: key
	prevHash   common.Hash    // effTransient: previous value
	prevRefund uint64         // effRefund: previous refund counter
}

// codeHashCache memoizes keccak256(contract code) across an engine's warm-pass
// workers and its serial authoritative pass (and across every batch of a run),
// keyed by address and validated by the code key's committed store version.
// resolveCodeHash calls StateDB.GetCodeHash on essentially every CALL/DELEGATECALL,
// which re-hashes the target's full bytecode -- for the ~15 KB USDC implementation
// (a DELEGATECALL target hit twice per transfer) that keccak dominated the auth
// pass's serial CPU. Contract code is immutable once committed (EIP-6780 forbids
// redeploying live contract code), so a stored entry whose version matches the
// freshly read record is authoritative; a mismatch -- only reachable if the code
// key were rewritten -- recomputes and refreshes the entry, keeping the cache
// self-correcting. It never affects the MVCC read-set: GetCodeHash still journals
// the code read exactly as GetCode did (see journalRead), so only the redundant
// hashing disappears. Safe for concurrent use by the warm pass's per-worker
// StateDBs, which all share one instance owned by the EVMEngine.
type codeHashCache struct {
	mu sync.RWMutex
	m  map[common.Address]codeHashEntry
}

// codeHashEntry is one memoized code hash and the store version it was computed
// under; a read whose version differs invalidates it (see codeHashCache).
type codeHashEntry struct {
	version blocks.Version
	hash    common.Hash
}

func newCodeHashCache() *codeHashCache {
	return &codeHashCache{m: make(map[common.Address]codeHashEntry)}
}

// get returns the memoized keccak256(code) for addr when the stored entry was
// computed under the same version; otherwise it hashes code once, caches it
// under version, and returns it. code MUST be the committed code bytes read
// under version. Concurrency-safe: readers take the RLock (the steady state
// once the handful of contract hashes are warm), the rare miss upgrades to the
// write lock to install the entry.
func (c *codeHashCache) get(addr common.Address, version blocks.Version, code []byte) common.Hash {
	c.mu.RLock()
	e, ok := c.m[addr]
	c.mu.RUnlock()
	if ok && e.version == version {
		return e.hash
	}
	h := crypto.Keccak256Hash(code)
	c.mu.Lock()
	c.m[addr] = codeHashEntry{version: version, hash: h}
	c.mu.Unlock()
	return h
}

// StateDB implements ExtendedStateDB by combining ledger state management
// with EVM-specific state tracking using a single unified journal.
type StateDB struct {
	namespace         string
	store             ReadStore
	logs              []Log
	monotonicVersions bool // if true, KVRead.Version is built from WriteRecord.Version (fabric-x MVCC semantics)

	// readCache memoizes store.Get results for the lifetime of one transaction
	// (one reset cycle). The backing snapshot is a single pinned point-in-time
	// view, so a key's committed record is immutable for the whole tx: the first
	// read is authoritative and every repeat must be served from here rather than
	// re-issuing a ~3ms backend round-trip. EVM execution re-reads the same keys
	// constantly -- Exist() reads bal/nonce/code, GetCodeHash calls Exist()+GetCode,
	// a transfer touches each balance several times -- so this collapses a tx's
	// serial read chain (the dominant cost of the concurrent warm pass) from tens
	// of round-trips down to its distinct-key count. Each StateDB is owned by a
	// single goroutine (per-worker in the warm pass, serial in the authoritative
	// pass), so no locking is needed. It is NOT the MVCC read-set: getStateFromStore
	// still journals every logical read into s.reads, so read dependencies and
	// snapshot/revert truncation are byte-for-byte unchanged -- only the redundant
	// backend fetches disappear. A cached entry may hold a nil record (key absent
	// in the view); presence in the map is what distinguishes "known absent" from
	// "not yet read". Read errors are never cached. Lazily allocated; cleared (not
	// freed) by reset for reuse across txs on a pooled StateDB.
	readCache map[string]cachedRead

	// EVM-specific runtime state
	refund           uint64
	selfDestructed   map[common.Address]struct{}
	newContracts     map[common.Address]struct{}                    // EIP-6780: contracts created in the current transaction
	accessList       *accessList                                    // EIP-2929/2930 access list (not persisted, reset per transaction)
	transientStorage map[common.Address]map[common.Hash]common.Hash // EIP-1153 transient storage (not persisted, reset per transaction)

	// Snapshot/revert support: three un-boxed journals (see the readRec /
	// writeRec / effectRec types) replace the old []any interface journal.
	reads          []readRec
	writes         []writeRec
	effects        []effectRec
	validRevisions []revision
	nextRevisionId int

	// codeHashCache memoizes keccak256(contract code) across the engine (nil in
	// unit tests that build a StateDB directly, which then hash on every call).
	// Shared by every StateDB an EVMEngine constructs -- the warm pass's
	// per-worker DBs and the serial auth pass alike -- so it must be
	// concurrency-safe; see the codeHashCache type. Not cleared by reset: the
	// cache spans txs and batches by design (committed code is immutable).
	codeHashCache *codeHashCache

	// dbErr records the first backing-store read error seen during execution.
	// The go-ethereum vm.StateDB accessors (GetState, GetBalance, GetNonce, ...)
	// return no error, so a failed read cannot be surfaced inline; it is
	// recorded here (returning a zero value to the EVM) and the Executor checks
	// Error() after PrepareMessage and after ApplyMessage, aborting the
	// transaction -- and with it the whole batch -- rather than committing a
	// result computed from missing state. A stale query-service view (its
	// lifetime exceeded by a large batch) is the common trigger; aborting lets
	// the executor retry on a fresh view instead of crashing the endorser.
	dbErr error
}

// setError records the first backing-store read error (see the dbErr field).
func (s *StateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

// Error returns the first backing-store read error recorded during execution,
// or nil if every read succeeded. See the dbErr field.
func (s *StateDB) Error() error { return s.dbErr }

// reset returns the StateDB to its freshly-constructed state while retaining
// every allocated map and slice, so a batch can reuse ONE StateDB across its
// transactions instead of building a fresh one per tx (NewStateDB's maps and the
// access-list map were among the top allocators). store becomes the new backing
// reader -- the authoritative pass swaps the shared snapshot for the write
// overlay between txs. A Result() taken before reset stays valid: it copies keys
// and values into its own maps, so clearing the journals here does not disturb it.
// NOT safe for concurrent use: each warm-pass worker owns its own StateDB.
func (s *StateDB) reset(store ReadStore) {
	s.store = store
	s.refund = 0
	s.dbErr = nil
	s.nextRevisionId = 0
	s.logs = s.logs[:0]
	s.reads = s.reads[:0]
	s.writes = s.writes[:0]
	s.effects = s.effects[:0]
	s.validRevisions = s.validRevisions[:0]
	clear(s.selfDestructed)
	clear(s.newContracts)
	clear(s.transientStorage)
	clear(s.readCache) // per-tx: a new tx sees a fresh (possibly newer) view
	s.accessList.reset()
}

// NewStateDB creates a new StateDB backed by the given ReadStore.
// If blockNum is 0, the current block number is queried from store.
// monotonicVersions controls MVCC semantics: when true, KVRead versions use the per-key
// monotonic version counter (Fabric-X); when false, they use (blockNum, txNum) (standard Fabric).
func NewStateDB(ctx context.Context, store ReadStore, namespace string, blockNum uint64, monotonicVersions bool) (*StateDB, error) {
	return &StateDB{
		namespace:         namespace,
		store:             store,
		monotonicVersions: monotonicVersions,
		selfDestructed:    make(map[common.Address]struct{}),
		newContracts:      make(map[common.Address]struct{}),
		accessList:        newAccessList(),
		transientStorage:  make(map[common.Address]map[common.Hash]common.Hash),
		validRevisions:    make([]revision, 0),
		nextRevisionId:    0,
	}, nil
}

// NewStateDBWithDualState creates a StateDB and wraps it with a DualStateDB for testing.
// This allows tracking both Fabric state and Ethereum trie state evolution.
// If ethStateDB is nil, a new in-memory ethStateDB is created.
func NewStateDBWithDualState(ctx context.Context, store ReadStore, namespace string, blockNum uint64, monotonicVersions bool, ethStateDB *ethstate.StateDB) (ExtendedStateDB, error) {
	fabricStateDB, err := NewStateDB(ctx, store, namespace, blockNum, monotonicVersions)
	if err != nil {
		return nil, err
	}

	// Create an in-memory ethStateDB if not provided
	if ethStateDB == nil {
		memDB := rawdb.NewMemoryDatabase()
		tconf := &triedb.Config{
			Preimages: true,
			HashDB:    hashdb.Defaults,
		}
		trieDB := triedb.NewDatabase(memDB, tconf)
		stateDB := ethstate.NewDatabase(trieDB, nil)
		ethStateDB, err = ethstate.New(types.EmptyRootHash, stateDB)
		if err != nil {
			return nil, fmt.Errorf("failed to create eth StateDB: %w", err)
		}
	}

	return NewDualStateDB(ethStateDB, fabricStateDB), nil
}

// Helper functions for key generation
// accKey / storeKey derive the internal KVS keys for an account field / storage
// slot. They use plain lowercase hex rather than addr.Hex(), which computes an
// EIP-55 keccak256 checksum on every call -- pure waste for an internal,
// non-user-facing key and, per profiling, the single largest allocator and a top
// CPU cost in execution. The downstream parser (gateway/storage/trie) uses
// common.HexToAddress/HexToHash, which are case- and 0x-prefix-insensitive, so
// the keys remain parseable.
//
// Each key is built in a SINGLE allocation: the previous "acc:"+Bytes2Hex(...)+
// ":"+typ form allocated twice (the hex string, then the concatenation) on every
// state access -- together the largest allocator in the profile. Here the exact
// byte length is computed up front, hex is encoded directly into the buffer, and
// unsafe.String hands the finished buffer to the string without a copy (the
// buffer is never mutated or aliased afterward). The output is byte-for-byte
// identical to the old form.
func accKey(addr common.Address, typ string) string {
	const prefix = "acc:"
	buf := make([]byte, len(prefix)+2*len(addr)+1+len(typ))
	n := copy(buf, prefix)
	hex.Encode(buf[n:], addr[:])
	n += 2 * len(addr)
	buf[n] = ':'
	n++
	copy(buf[n:], typ)
	return unsafe.String(&buf[0], len(buf))
}

func storeKey(addr common.Address, slot common.Hash) string {
	const prefix = "str:"
	buf := make([]byte, len(prefix)+2*len(addr)+1+2*len(slot))
	n := copy(buf, prefix)
	hex.Encode(buf[n:], addr[:])
	n += 2 * len(addr)
	buf[n] = ':'
	n++
	hex.Encode(buf[n:], slot[:])
	return unsafe.String(&buf[0], len(buf))
}

// -------------------- Internal state query helpers --------------------

// getStateFromJournal scans the journal backwards to find the latest write for a key.
// Returns the value and true if found, or nil and false if not found.
func (s *StateDB) getStateFromJournal(key string) ([]byte, bool) {
	for i := len(s.writes) - 1; i >= 0; i-- {
		if w := &s.writes[i]; w.key == key {
			if w.isDelete {
				return nil, true
			}
			return w.value, true
		}
	}
	return nil, false
}

// viewGet returns the pinned view's record for key, memoized for this tx (see
// the readCache field). The view is immutable for the tx's lifetime, so the
// first read's result is authoritative and reused for every repeat -- this is
// where the redundant backend round-trips are eliminated. It does NOT touch the
// MVCC read-set (s.reads); callers that create a read dependency journal the
// read themselves (see getStateFromStore). Read errors are propagated, never
// cached, so a transient failure can be retried by a later read.
func (s *StateDB) viewGet(key string) (*blocks.WriteRecord, error) {
	if c, ok := s.readCache[key]; ok {
		return c.rec, nil
	}
	rec, err := s.store.Get(s.namespace, key)
	if err != nil {
		return nil, err
	}
	if s.readCache == nil {
		s.readCache = make(map[string]cachedRead)
	}
	s.readCache[key] = cachedRead{rec: rec}
	return rec, nil
}

// journalRead records an MVCC read dependency on key for the pinned view's
// record (nil when the key is absent in the view) and returns the record's
// value together with the readRec it appended. It is the single place that maps
// a WriteRecord's version onto the read-set's Version, so getStateFromStore and
// GetCodeHash journal reads identically -- the latter also needs the returned
// readRec's version to key its code-hash cache without recomputing it.
func (s *StateDB) journalRead(key string, record *blocks.WriteRecord) ([]byte, readRec) {
	var val []byte
	r := readRec{key: key}
	if record != nil {
		// Use the value from the record, even if IsDelete is true
		// (IsDelete: true with non-nil Value happens during snapshot revert)
		val = record.Value

		// Only set version in read set if the key is not marked as deleted
		// When IsDelete is true, we want a nil version in the read set for MVCC
		if !record.IsDelete {
			r.hasVersion = true
			if s.monotonicVersions {
				r.version = blocks.Version{BlockNum: record.Version}
			} else {
				r.version = blocks.Version{
					BlockNum: record.BlockNum,
					TxNum:    record.TxNum,
				}
			}
		}
	}

	// Journal the read - this creates an MVCC dependency
	s.reads = append(s.reads, r)
	return val, r
}

// getStateFromStore reads from the underlying ReadStore (via the per-tx read
// cache) and journals the read. This creates an MVCC read dependency: every
// call appends to s.reads exactly as before, so read-set contents and
// snapshot/revert semantics are unchanged whether or not the value was cached.
func (s *StateDB) getStateFromStore(key string) ([]byte, error) {
	record, err := s.viewGet(key)
	if err != nil {
		return nil, err
	}
	val, _ := s.journalRead(key, record)
	return val, nil
}

// getState returns the current value for a key, checking journal first, then store.
// This creates an MVCC read dependency if the value is not in the journal.
func (s *StateDB) getState(key string) ([]byte, error) {
	// Check journal first
	if val, found := s.getStateFromJournal(key); found {
		return val, nil
	}

	// Not in journal, read from store (creates MVCC dependency)
	return s.getStateFromStore(key)
}

// putState writes a value to the journal (blind write - no read dependency).
// Empty values are treated as deletes (standard Fabric behavior).
func (s *StateDB) putState(key string, value []byte) {
	w := writeRec{key: key}
	if len(value) == 0 {
		w.isDelete = true
	} else {
		w.value = value
	}
	s.writes = append(s.writes, w)
}

// -------------------- vm.StateDB interface implementation --------------------

// CreateAccount creates an account with zero balance and nonce.
func (s *StateDB) CreateAccount(addr common.Address) {
	s.putState(accKey(addr, "bal"), uint256ToBytes(uint256.NewInt(0)))
	s.putState(accKey(addr, "nonce"), uint64ToBytes(0))
	s.markNewContract(addr)
}

// CreateContract creates a contract account with empty code.
func (s *StateDB) CreateContract(addr common.Address) {
	s.putState(accKey(addr, "code"), []byte{})
	s.markNewContract(addr)
}

// markNewContract records addr in newContracts for EIP-6780; idempotent.
func (s *StateDB) markNewContract(addr common.Address) {
	if _, exists := s.newContracts[addr]; exists {
		return
	}
	s.effects = append(s.effects, effectRec{kind: effNewContract, addr: addr})
	s.newContracts[addr] = struct{}{}
}

// GetBalance returns the balance of an account.
func (s *StateDB) GetBalance(addr common.Address) *uint256.Int {
	val, err := s.getState(accKey(addr, "bal"))
	if err != nil {
		s.setError(fmt.Errorf("GetBalance failed: %w", err))
		return uint256.NewInt(0)
	}
	return bytesToUint256(val)
}

// AddBalance adds balance to an account.
func (s *StateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	if amount.IsZero() {
		return *uint256.NewInt(0)
	}
	prev := s.GetBalance(addr)
	newBal := new(uint256.Int).Add(prev, amount)
	s.putState(accKey(addr, "bal"), uint256ToBytes(newBal))
	return *prev
}

// SubBalance subtracts balance from an account.
func (s *StateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	if amount.IsZero() {
		return *uint256.NewInt(0)
	}
	prev := s.GetBalance(addr)
	newBal := new(uint256.Int).Sub(prev, amount)
	s.putState(accKey(addr, "bal"), uint256ToBytes(newBal))
	return *prev
}

// GetNonce returns the nonce of an account.
func (s *StateDB) GetNonce(addr common.Address) uint64 {
	val, err := s.getState(accKey(addr, "nonce"))
	if err != nil {
		s.setError(fmt.Errorf("GetNonce failed: %w", err))
		return 0
	}
	return bytesToUint64(val)
}

// SetNonce sets the nonce of an account.
func (s *StateDB) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	s.putState(accKey(addr, "nonce"), uint64ToBytes(nonce))
}

// GetCode returns the code of an account.
func (s *StateDB) GetCode(addr common.Address) []byte {
	val, err := s.getState(accKey(addr, "code"))
	if err != nil {
		s.setError(fmt.Errorf("GetCode failed: %w", err))
		return nil
	}
	return val
}

// GetCodeHash returns the code hash of an account.
// Returns zero hash if account doesn't exist.
// Returns empty code hash (keccak256 of nil) if account exists but has no code.
// Returns keccak256 of code if account has code.
func (s *StateDB) GetCodeHash(addr common.Address) common.Hash {
	if !s.Exist(addr) {
		return common.Hash{}
	}
	codeKey := accKey(addr, "code")

	// Code written earlier in this tx (contract creation): hash the pending
	// value directly, journaling no read -- exactly as the old Exist()+GetCode()
	// path did, where GetCode's getState found the write in the journal.
	if val, found := s.getStateFromJournal(codeKey); found {
		return crypto.Keccak256Hash(val)
	}

	// Committed code. Read it through the pinned view and journal the read via
	// journalRead, so the MVCC read-set is byte-for-byte identical to the old
	// GetCode path (same key, same version). The only change is that the
	// keccak256 of immutable contract code is memoized in the engine-shared
	// cache -- keyed by address, validated by the read's version -- rather than
	// recomputed on every call. EOAs and accounts with no code have an absent
	// code key (record == nil => !hasVersion), so they skip the cache and hash
	// nil, yielding the empty-code hash just as before.
	record, err := s.viewGet(codeKey)
	if err != nil {
		s.setError(fmt.Errorf("GetCodeHash failed: %w", err))
		return common.Hash{}
	}
	code, r := s.journalRead(codeKey, record)
	if s.codeHashCache != nil && r.hasVersion {
		return s.codeHashCache.get(addr, r.version, code)
	}
	return crypto.Keccak256Hash(code)
}

// GetCodeSize returns the code size of an account.
func (s *StateDB) GetCodeSize(addr common.Address) int {
	code := s.GetCode(addr)
	return len(code)
}

// SetCode sets the code of an account.
func (s *StateDB) SetCode(addr common.Address, code []byte, reason tracing.CodeChangeReason) []byte {
	prev := s.GetCode(addr)
	s.putState(accKey(addr, "code"), code)
	return prev
}

// GetState returns the current storage value for a slot.
func (s *StateDB) GetState(addr common.Address, slot common.Hash) common.Hash {
	key := storeKey(addr, slot)
	val, err := s.getState(key)
	if err != nil {
		s.setError(fmt.Errorf("GetState failed: %w", err))
		return common.Hash{}
	}
	if len(val) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(val)
}

// GetStateAndCommittedState returns both current and committed state.
// Current state is from the journal, committed state is from the ReadStore.
// This DOES create a read dependency for the committed state.
func (s *StateDB) GetStateAndCommittedState(addr common.Address, slot common.Hash) (common.Hash, common.Hash) {
	key := storeKey(addr, slot)

	// Get current state from journal (no store read)
	currentVal, found := s.getStateFromJournal(key)
	var current common.Hash
	if found && len(currentVal) > 0 {
		current = common.BytesToHash(currentVal)
	}

	// Get committed state from store and journal the read (creates MVCC dependency)
	committedVal, err := s.getStateFromStore(key)
	if err != nil {
		s.setError(fmt.Errorf("GetStateAndCommittedState failed: %w", err))
		return current, common.Hash{}
	}

	var committed common.Hash
	if len(committedVal) > 0 {
		committed = common.BytesToHash(committedVal)
	}

	// If not found in journal, current equals committed
	if !found {
		current = committed
	}

	return current, committed
}

// SetState sets the storage value for a slot.
// This is a blind write - it does NOT create a read dependency.
// The previous value is returned from the journal if it exists, otherwise from the store.
func (s *StateDB) SetState(addr common.Address, slot common.Hash, value common.Hash) common.Hash {
	key := storeKey(addr, slot)

	// Get previous value from journal first (no read dependency)
	prevVal, found := s.getStateFromJournal(key)
	var prev common.Hash
	if found {
		if len(prevVal) > 0 {
			prev = common.BytesToHash(prevVal)
		}
	} else {
		// Not in journal, read directly from store WITHOUT creating a read
		// dependency. Served from the per-tx read cache (viewGet) when the key
		// was already read -- so the SLOAD that typically precedes an SSTORE
		// isn't paid twice -- and viewGet deliberately leaves s.reads untouched,
		// preserving the "blind write" no-dependency contract.
		record, err := s.viewGet(key)
		if err != nil {
			// Record the read failure and proceed with a zero previous value;
			// the Executor aborts the tx on Error() so this write is discarded.
			s.setError(fmt.Errorf("SetState failed to get previous value: %w", err))
		} else if record != nil && !record.IsDelete && len(record.Value) > 0 {
			prev = common.BytesToHash(record.Value)
		}
	}

	// Write new value
	// If value is zero, write empty bytes to trigger deletion
	if value == (common.Hash{}) {
		s.putState(key, nil)
	} else {
		s.putState(key, value.Bytes())
	}

	return prev
}

// GetStorageRoot returns the storage root (stub for now).
func (s *StateDB) GetStorageRoot(addr common.Address) common.Hash {
	return common.Hash{}
}

// SelfDestruct implements SELFDESTRUCT with EIP-6780 semantics: only fully
// destroys the account if it was created in the current transaction.
func (s *StateDB) SelfDestruct(addr common.Address) {
	if prevBalance := s.GetBalance(addr); !prevBalance.IsZero() {
		s.putState(accKey(addr, "bal"), uint256ToBytes(uint256.NewInt(0)))
	}
	if _, isNew := s.newContracts[addr]; isNew {
		s.effects = append(s.effects, effectRec{kind: effSelfDestruct, addr: addr})
		s.selfDestructed[addr] = struct{}{}
	}
}

// HasSelfDestructed checks if an account has self-destructed.
func (s *StateDB) HasSelfDestructed(addr common.Address) bool {
	_, ok := s.selfDestructed[addr]
	return ok
}

// Exist checks if an account exists.
func (s *StateDB) Exist(addr common.Address) bool {
	// Check if any account field exists
	if val, err := s.getState(accKey(addr, "bal")); err == nil && val != nil {
		return true
	}
	if val, err := s.getState(accKey(addr, "nonce")); err == nil && val != nil {
		return true
	}
	if val, err := s.getState(accKey(addr, "code")); err == nil && val != nil {
		return true
	}
	return false
}

// Empty checks if an account is empty (EIP-161).
func (s *StateDB) Empty(addr common.Address) bool {
	balance := s.GetBalance(addr)
	if balance != nil && !balance.IsZero() {
		return false
	}
	nonce := s.GetNonce(addr)
	if nonce > 0 {
		return false
	}
	codeSize := s.GetCodeSize(addr)
	return codeSize == 0
}

// GetRefund returns the current gas refund counter.
func (s *StateDB) GetRefund() uint64 {
	return s.refund
}

// AddRefund adds to the gas refund counter.
func (s *StateDB) AddRefund(gas uint64) {
	s.effects = append(s.effects, effectRec{kind: effRefund, prevRefund: s.refund})
	s.refund += gas
}

// SubRefund subtracts from the gas refund counter.
func (s *StateDB) SubRefund(gas uint64) {
	if gas > s.refund {
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund))
	}
	s.effects = append(s.effects, effectRec{kind: effRefund, prevRefund: s.refund})
	s.refund -= gas
}

// AddLog adds a log entry.
func (s *StateDB) AddLog(log *types.Log) {
	topics := make([][]byte, len(log.Topics))
	for i, t := range log.Topics {
		topics[i] = t.Bytes()
	}
	s.logs = append(s.logs, Log{
		Address: log.Address.Bytes(),
		Topics:  topics,
		Data:    log.Data,
	})
}

// Snapshot creates a snapshot of the current state.
func (s *StateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{
		id:          id,
		readIndex:   len(s.reads),
		writeIndex:  len(s.writes),
		effectIndex: len(s.effects),
		logIndex:    len(s.logs),
	})
	return id
}

// RevertToSnapshot reverts to a previous snapshot.
func (s *StateDB) RevertToSnapshot(revid int) {
	// Find the snapshot
	idx := -1
	for i, rev := range s.validRevisions {
		if rev.id == revid {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}

	snapshot := s.validRevisions[idx]

	// Undo reversible EVM-mechanics changes by replaying effects in reverse.
	// Reads/writes carry no side effects beyond their slices, so truncation
	// (below) fully reverts them.
	for i := len(s.effects) - 1; i >= snapshot.effectIndex; i-- {
		e := &s.effects[i]
		switch e.kind {
		case effRefund:
			s.refund = e.prevRefund
		case effSelfDestruct:
			delete(s.selfDestructed, e.addr)
		case effNewContract:
			delete(s.newContracts, e.addr)
		case effTransient:
			// Revert transient storage to previous value
			if s.transientStorage[e.addr] == nil {
				s.transientStorage[e.addr] = make(map[common.Hash]common.Hash)
			}
			s.transientStorage[e.addr][e.hash] = e.prevHash
		case effAccessAddr:
			// Revert access list address addition
			s.accessList.deleteAddress(e.addr)
		case effAccessSlot:
			// Revert access list slot addition
			s.accessList.deleteSlot(e.addr, e.hash)
		}
	}

	// Truncate reads, writes, effects, logs, and revisions
	s.reads = s.reads[:snapshot.readIndex]
	s.writes = s.writes[:snapshot.writeIndex]
	s.effects = s.effects[:snapshot.effectIndex]
	s.logs = s.logs[:snapshot.logIndex]
	s.validRevisions = s.validRevisions[:idx]
}

// toKVRead converts a journaled read record into a blocks.KVRead, materializing
// the *blocks.Version at most once (nil when the key was absent or deleted).
func toKVRead(key string, r *readRec) blocks.KVRead {
	var vptr *blocks.Version
	if r.hasVersion {
		v := r.version
		vptr = &v
	}
	return blocks.KVRead{Key: key, Version: vptr}
}

// Result returns the read-write set containing all non-reverted operations.
//
// Every written key is guaranteed a read-set entry: VersionedCache.ApplyWrites
// (the cross-batch cache, gateway/core/versioned_cache.go) derives each
// written key's speculative MVCC version from its READ in this same RWS -- a
// write with no matching read looks like a from-genesis write (spec version
// 0) even if the key already has committed history. The EVM's SLOAD-before-
// SSTORE (EIP-2929) and this StateDB's read-before-write balance/nonce/code
// helpers make that hold today, but SetState's blind write and CreateAccount's
// zeroing writes do NOT journal a read -- so below, any write key still
// missing from the read set is backfilled with ONE store fetch, recorded
// exactly as getStateFromStore would record it. Keys already read (the common
// read-modify-write case) are left untouched and are not re-fetched.
func (s *StateDB) Result() blocks.ReadWriteSet {
	reads := make(map[string]blocks.KVRead)
	writes := make(map[string]blocks.KVWrite)

	for i := range s.reads {
		r := &s.reads[i]
		// First-seen wins: every read of a key under one view carries the same
		// version, so this matches the old last-seen assignment while allocating
		// the *blocks.Version at most once per unique key.
		if _, ok := reads[r.key]; !ok {
			reads[r.key] = toKVRead(r.key, r)
		}
	}
	for i := range s.writes {
		w := &s.writes[i]
		writes[w.key] = blocks.KVWrite{Key: w.key, IsDelete: w.isDelete, Value: w.value}
	}

	// Backfill a base version for every write key with no read-set entry (see
	// the doc comment above). getStateFromStore appends the fetched record to
	// s.reads too; harmless here since Result() is the last thing done with a
	// StateDB before it is either discarded or reset (which truncates s.reads
	// back to empty) for the next tx.
	for key := range writes {
		if _, ok := reads[key]; ok {
			continue
		}
		if _, err := s.getStateFromStore(key); err != nil {
			s.setError(fmt.Errorf("Result failed to backfill read for write key %q: %w", key, err))
			continue
		}
		reads[key] = toKVRead(key, &s.reads[len(s.reads)-1])
	}

	rws := blocks.ReadWriteSet{
		Reads:  make([]blocks.KVRead, 0, len(reads)),
		Writes: make([]blocks.KVWrite, 0, len(writes)),
	}
	for _, r := range reads {
		rws.Reads = append(rws.Reads, r)
	}
	for _, w := range writes {
		rws.Writes = append(rws.Writes, w)
	}
	return rws
}

// Logs returns all non-reverted logs.
func (s *StateDB) Logs() []Log {
	return s.logs
}

// -------------------- Stub implementations for unused methods --------------------

func (s *StateDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	if storage, ok := s.transientStorage[addr]; ok {
		return storage[key]
	}
	return common.Hash{}
}

func (s *StateDB) SetTransientState(addr common.Address, key, value common.Hash) {
	// Get previous value for journal
	prev := s.GetTransientState(addr, key)

	// Create journal entry to track the change
	s.effects = append(s.effects, effectRec{
		kind:     effTransient,
		addr:     addr,
		hash:     key,
		prevHash: prev,
	})

	// Set the new value
	if s.transientStorage[addr] == nil {
		s.transientStorage[addr] = make(map[common.Hash]common.Hash)
	}
	s.transientStorage[addr][key] = value
}

func (s *StateDB) AddPreimage(hash common.Hash, preimage []byte) {}

// AddressInAccessList checks if an address is in the access list.
func (s *StateDB) AddressInAccessList(addr common.Address) bool {
	return s.accessList.containsAddress(addr)
}

// SlotInAccessList checks if a storage slot is in the access list.
func (s *StateDB) SlotInAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	return s.accessList.contains(addr, slot)
}

// AddAddressToAccessList adds an address to the access list.
func (s *StateDB) AddAddressToAccessList(addr common.Address) {
	if s.accessList.containsAddress(addr) {
		return
	}
	s.effects = append(s.effects, effectRec{kind: effAccessAddr, addr: addr})
	s.accessList.addAddress(addr)
}

// AddSlotToAccessList adds a storage slot to the access list.
func (s *StateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	addrOk, slotOk := s.accessList.contains(addr, slot)
	if addrOk && slotOk {
		return
	}
	// Add address journal entry if address is not present
	if !addrOk {
		s.effects = append(s.effects, effectRec{kind: effAccessAddr, addr: addr})
	}
	// Always add slot journal entry
	s.effects = append(s.effects, effectRec{kind: effAccessSlot, addr: addr, hash: slot})
	s.accessList.addSlot(addr, slot)
}

// Prepare initializes the access list and transient storage for a new transaction.
// This follows EIP-2929 (Berlin) and EIP-2930 semantics.
func (s *StateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList) {
	// Reset access list for new transaction (EIP-2929). Reusing the existing
	// list's map (rather than newAccessList) avoids a per-tx allocation.
	s.accessList.reset()

	// Add sender to access list
	s.accessList.addAddress(sender)

	// Add destination if present (not for contract creation)
	if dest != nil {
		s.accessList.addAddress(*dest)
	}

	// Add precompiled contracts
	for _, addr := range precompiles {
		s.accessList.addAddress(addr)
	}

	// Add transaction access list entries (EIP-2930)
	for _, el := range txAccesses {
		s.accessList.addAddress(el.Address)
		for _, key := range el.StorageKeys {
			s.accessList.addSlot(el.Address, key)
		}
	}

	// Add coinbase if Shanghai rules apply (EIP-3651)
	if rules.IsShanghai {
		s.accessList.addAddress(coinbase)
	}
}

func (s *StateDB) Witness() *stateless.Witness { return nil }

func (s *StateDB) AccessEvents() *ethstate.AccessEvents { return nil }

func (s *StateDB) Finalise(deleteEmptyObjects bool) *bal.StateAccessList { return nil }

func (s *StateDB) Touch(addr common.Address) {
	// It doesn't affect the control flow, so we don't add a read dependency to the read/write set.
}

// IsNewContract reports whether addr was created in the current transaction.
func (s *StateDB) IsNewContract(addr common.Address) bool {
	_, ok := s.newContracts[addr]
	return ok
}

// LogsForBurnAccounts returns logs emitted by burn accounts during the current transaction.
// Not tracked separately in this implementation.
func (s *StateDB) LogsForBurnAccounts() []*types.Log { return nil }

// -------------------- Helper functions --------------------

func uint256ToBytes(u *uint256.Int) []byte {
	if u == nil {
		return nil
	}
	return u.ToBig().Bytes()
}

func bytesToUint256(b []byte) *uint256.Int {
	if len(b) == 0 {
		return new(uint256.Int)
	}
	u, _ := uint256.FromBig(new(big.Int).SetBytes(b))
	return u
}

func uint64ToBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return b
}

func bytesToUint64(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
