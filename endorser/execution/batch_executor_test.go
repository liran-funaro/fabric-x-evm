/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"reflect"
	"sort"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/state"
	_ "modernc.org/sqlite"
)

// newTestKey generates a fresh ECDSA key and its derived address, for signing
// transactions in these tests.
func newTestKey(t *testing.T) (*ecdsa.PrivateKey, ethcommon.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return key, crypto.PubkeyToAddress(key.PublicKey)
}

// newTransferTx builds and signs a plain ETH-value-transfer legacy transaction.
// It signs with types.MakeSigner(cfg, block 0, time 1_000_000) — the exact
// signer NewExecutor's BlockCtx implies for a nil blockNumber (see
// NewExecutor's defaultBlockTime) — so Executor.PrepareMessage recovers the
// same sender that signed it.
func newTransferTx(t *testing.T, cfg *params.ChainConfig, key *ecdsa.PrivateKey, to ethcommon.Address, value *big.Int, nonce uint64) *types.Transaction {
	t.Helper()
	signer := types.MakeSigner(cfg, big.NewInt(0), 1_000_000)
	tx, err := types.SignNewTx(key, signer, &types.LegacyTx{
		Nonce: nonce,
		Gas:   21_000,
		To:    &to,
		Value: value,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// newFailingCreationTx builds and signs a contract-creation transaction whose
// init code is the single INVALID opcode (0xfe). geth's core.ApplyMessage
// accepts the message (100_000 gas is well above the ~53000 intrinsic gas for
// contract creation with 1-byte init code, and free gas -- see Executor.execute
// -- means buyGas never requires sender balance), but the EVM interpreter
// faults on INVALID with vm.ErrInvalidOpCode, which Executor.ApplyMessage
// wraps as *ExecFailure -- NOT *TxRejected, and NOT a revert (ErrExecutionReverted).
// This is the deterministic trigger for FIX C1's committed-but-faulted path.
func newFailingCreationTx(t *testing.T, cfg *params.ChainConfig, key *ecdsa.PrivateKey, nonce uint64) *types.Transaction {
	t.Helper()
	signer := types.MakeSigner(cfg, big.NewInt(0), 1_000_000)
	tx, err := types.SignNewTx(key, signer, &types.LegacyTx{
		Nonce: nonce,
		Gas:   100_000,
		To:    nil,          // contract creation
		Data:  []byte{0xfe}, // INVALID opcode
	})
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// assertSameRWS compares two ReadWriteSets for equality ignoring slice order:
// StateDB.Result() iterates Go maps when building Reads/Writes, so two
// independent simulations of the same operations may return them in a
// different order despite representing the same set.
func assertSameRWS(t *testing.T, label string, got, want blocks.ReadWriteSet) {
	t.Helper()
	sortRWS(&got)
	sortRWS(&want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: RWS differ:\n got  = %+v\n want = %+v", label, got, want)
	}
}

func sortRWS(rws *blocks.ReadWriteSet) {
	sort.Slice(rws.Reads, func(i, j int) bool { return rws.Reads[i].Key < rws.Reads[j].Key })
	sort.Slice(rws.Writes, func(i, j int) bool { return rws.Writes[i].Key < rws.Writes[j].Key })
}

// seedAccount commits addr's balance as block 0, so later snapshots taken at
// "latest" observe it. It mirrors the setup pattern in state_test.go's
// TestSnapshotRevertRWS (CreateAccount + AddBalance, then UpdateWorldState).
func seedAccounts(t *testing.T, backend *state.VersionedDB, balances map[ethcommon.Address]int64) {
	t.Helper()
	setup := snapshotDB(t, backend, 0)
	for addr, bal := range balances {
		setup.CreateAccount(addr)
		setup.AddBalance(addr, uint256.NewInt(uint64(bal)), tracing.BalanceChangeUnspecified)
	}

	err := backend.UpdateWorldState(t.Context(), blocks.Block{
		Number: 0,
		Transactions: []blocks.Transaction{{
			ID: "setup", Number: 0, Valid: true,
			NsRWS: []blocks.NsReadWriteSet{{Namespace: Namespace, RWS: setup.Result()}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestExecuteBatchSequentialDependency seeds accounts A and B, then runs an
// ordered batch [A->B transfer, B->C transfer] where the second spends what the
// first delivered. It asserts both succeed and B's final balance reflects tx1
// applied before tx2 (i.e. the authoritative pass saw tx1's write).
func TestExecuteBatchSequentialDependency(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:batch_seq_dep?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	keyA, addrA := newTestKey(t)
	keyB, addrB := newTestKey(t)
	_, addrC := newTestKey(t)

	const (
		initialA = 1_000
		initialB = 50  // alone, insufficient to cover amount2 below
		amount1  = 300 // tx1: A -> B
		amount2  = 100 // tx2: B -> C; needs tx1's delivery (50+300=350 >= 100)
	)
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: initialA, addrB: initialB})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)

	tx1 := newTransferTx(t, cfg.ChainConfig, keyA, addrB, big.NewInt(amount1), 0)
	tx2 := newTransferTx(t, cfg.ChainConfig, keyB, addrC, big.NewInt(amount2), 0)

	results, err := engine.ExecuteBatch(context.Background(), []*types.Transaction{tx1, tx2})
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Status != 200 {
		t.Fatalf("tx1: status = %d, message = %q, want 200 (OK)", results[0].Status, results[0].Message)
	}
	if results[1].Status != 200 {
		t.Fatalf("tx2: status = %d, message = %q, want 200 (OK) — tx2 must see tx1's write via the overlay", results[1].Status, results[1].Message)
	}

	// tx2's own RWS carries B's post-transfer balance: initialB + amount1 - amount2.
	// This value is only reachable if tx2 observed tx1's write through the overlay
	// (standalone, B only has initialB=50 < amount2=100 and tx2 would be rejected).
	key := accKey(addrB, "bal")
	var wrote bool
	for _, w := range results[1].RWS.Writes {
		if w.Key != key {
			continue
		}
		wrote = true
		got := bytesToUint256(w.Value)
		want := uint256.NewInt(uint64(initialB + amount1 - amount2))
		if got.Cmp(want) != 0 {
			t.Errorf("B balance after tx2 = %s, want %s (initialB=%d + amount1=%d - amount2=%d)",
				got, want, initialB, amount1, amount2)
		}
	}
	if !wrote {
		t.Fatalf("expected tx2's RWS to include a write to B's balance key %q; writes = %+v", key, results[1].RWS.Writes)
	}
}

// TestExecuteBatchFreshKeyCreatedMidBatch exercises the overlayReader.Get branch
// where a key has NO committed pre-batch value (under == nil) but is written by
// an earlier tx in the batch and read by a later one. Only account A is seeded;
// B is created by tx1 (A->B), then spent by tx2 (B->C). tx2 can only succeed if
// it sees tx1's fresh write to B through the overlay. It also asserts tx2's read
// of B's balance carries a nil MVCC version (recorded as "absent", matching B's
// true pre-batch ledger state) — the correctness property of the fresh-key path.
func TestExecuteBatchFreshKeyCreatedMidBatch(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:batch_fresh_key?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	keyA, addrA := newTestKey(t)
	keyB, addrB := newTestKey(t)
	_, addrC := newTestKey(t)

	const (
		initialA = 1_000
		amount1  = 300 // tx1: A -> B (B has no pre-batch value; tx1 creates it)
		amount2  = 100 // tx2: B -> C; only possible if B's fresh balance is visible
	)
	// Seed ONLY A. B is absent from committed state until tx1 writes it.
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: initialA})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)

	tx1 := newTransferTx(t, cfg.ChainConfig, keyA, addrB, big.NewInt(amount1), 0)
	tx2 := newTransferTx(t, cfg.ChainConfig, keyB, addrC, big.NewInt(amount2), 0)

	results, err := engine.ExecuteBatch(context.Background(), []*types.Transaction{tx1, tx2})
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Status != 200 {
		t.Fatalf("tx1: status = %d, message = %q, want 200 (OK)", results[0].Status, results[0].Message)
	}
	if results[1].Status != 200 {
		t.Fatalf("tx2: status = %d, message = %q, want 200 (OK) — tx2 must see B's fresh balance from tx1", results[1].Status, results[1].Message)
	}

	balKey := accKey(addrB, "bal")

	// tx2's write reflects B's freshly-created balance minus amount2.
	var wrote bool
	for _, w := range results[1].RWS.Writes {
		if w.Key != balKey {
			continue
		}
		wrote = true
		got := bytesToUint256(w.Value)
		want := uint256.NewInt(uint64(amount1 - amount2))
		if got.Cmp(want) != 0 {
			t.Errorf("B balance after tx2 = %s, want %s (amount1=%d - amount2=%d)", got, want, amount1, amount2)
		}
	}
	if !wrote {
		t.Fatalf("expected tx2's RWS to write B's balance key %q; writes = %+v", balKey, results[1].RWS.Writes)
	}

	// B had no committed pre-batch value, so tx2's read dependency on B's balance
	// must be recorded with a nil version ("absent"), not a fabricated version.
	var readFound bool
	for _, r := range results[1].RWS.Reads {
		if r.Key != balKey {
			continue
		}
		readFound = true
		if r.Version != nil {
			t.Errorf("B balance read version = %+v, want nil (B was absent pre-batch)", r.Version)
		}
	}
	if !readFound {
		t.Fatalf("expected tx2's RWS to read B's balance key %q; reads = %+v", balKey, results[1].RWS.Reads)
	}
}

// TestExecuteBatchExcludesNonceGap seeds accounts A and B, then runs a 3-tx
// batch [A->B transfer, D's nonce-gap tx (nonce 1 with no nonce-0 tx for D, so
// it is rejected before execution), B->C transfer] and asserts:
//   - ExecuteBatch does NOT abort the whole batch on the middle tx's rejection
//     (no error, 3 results back).
//   - The middle result is excluded: Status 400 (common.StatusTxRejected) with
//     an EMPTY RWS (no reads, no writes) -- its rejection must not leak any
//     state into the batch.
//   - The other two txs still execute normally, in order: the third tx's own
//     RWS proves it observed the first tx's write via the overlay (exactly
//     like TestExecuteBatchSequentialDependency), and the merged RWS (via
//     MergeResults, which the exclusion design leaves unchanged) carries both
//     of their writes but nothing from the excluded middle tx.
func TestExecuteBatchExcludesNonceGap(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:batch_nonce_gap?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	keyA, addrA := newTestKey(t)
	keyB, addrB := newTestKey(t)
	_, addrC := newTestKey(t)
	keyD, _ := newTestKey(t) // D is never seeded; it has ledger nonce 0.

	const (
		initialA = 1_000
		initialB = 50  // alone, insufficient to cover amount2 below
		amount1  = 300 // tx1: A -> B
		amount2  = 100 // tx3: B -> C; needs tx1's delivery (50+300=350 >= 100)
	)
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: initialA, addrB: initialB})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)

	tx1 := newTransferTx(t, cfg.ChainConfig, keyA, addrB, big.NewInt(amount1), 0)
	// D's ledger nonce is 0 (never seeded/written), but this tx carries nonce 1:
	// a gap -- rejected with ErrNonceTooHigh before any execution/state read.
	gapTx := newTransferTx(t, cfg.ChainConfig, keyD, addrC, big.NewInt(1), 1)
	tx3 := newTransferTx(t, cfg.ChainConfig, keyB, addrC, big.NewInt(amount2), 0)

	results, err := engine.ExecuteBatch(context.Background(), []*types.Transaction{tx1, gapTx, tx3})
	if err != nil {
		t.Fatalf("ExecuteBatch must exclude the rejected tx, not abort the batch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if results[0].Status != 200 {
		t.Fatalf("tx1: status = %d, message = %q, want 200 (OK)", results[0].Status, results[0].Message)
	}

	if results[1].Status != common.StatusTxRejected {
		t.Fatalf("gapTx: status = %d, want %d (StatusTxRejected)", results[1].Status, common.StatusTxRejected)
	}
	if len(results[1].RWS.Reads) != 0 || len(results[1].RWS.Writes) != 0 {
		t.Fatalf("gapTx: RWS = %+v, want empty (excluded tx must not contribute reads or writes)", results[1].RWS)
	}

	if results[2].Status != 200 {
		t.Fatalf("tx3: status = %d, message = %q, want 200 (OK) — tx3 must see tx1's write via the overlay despite the excluded middle tx", results[2].Status, results[2].Message)
	}

	balKey := accKey(addrB, "bal")
	var tx3Wrote bool
	for _, w := range results[2].RWS.Writes {
		if w.Key != balKey {
			continue
		}
		tx3Wrote = true
		got := bytesToUint256(w.Value)
		want := uint256.NewInt(uint64(initialB + amount1 - amount2))
		if got.Cmp(want) != 0 {
			t.Errorf("B balance after tx3 = %s, want %s (initialB=%d + amount1=%d - amount2=%d)",
				got, want, initialB, amount1, amount2)
		}
	}
	if !tx3Wrote {
		t.Fatalf("expected tx3's RWS to include a write to B's balance key %q; writes = %+v", balKey, results[2].RWS.Writes)
	}

	// The merged RWS (what actually lands in the committed Fabric tx) carries
	// both included txs' writes and nothing from the excluded gap tx.
	merged, events := MergeResults(results)
	if len(events) != 3 {
		t.Fatalf("expected 3 events (one per result, index = sub-index), got %d", len(events))
	}
	mergedWrites := map[string][]byte{}
	for _, w := range merged.Writes {
		mergedWrites[w.Key] = w.Value
	}
	if _, ok := mergedWrites[accKey(addrA, "bal")]; !ok {
		t.Errorf("merged RWS missing A's balance write from tx1; writes = %+v", merged.Writes)
	}
	if _, ok := mergedWrites[balKey]; !ok {
		t.Errorf("merged RWS missing B's balance write from tx3; writes = %+v", merged.Writes)
	}
	// D never appears: the excluded tx contributed nothing to the merged set.
	if _, ok := mergedWrites[accKey(addrC, "bal")]; !ok {
		t.Errorf("merged RWS missing C's balance write from tx3; writes = %+v", merged.Writes)
	}
}

// TestExecuteBatchExecFailureDoesNotAbort seeds accounts A and B, then runs a
// 3-tx batch [A->B transfer, F's contract-creation tx whose init code faults
// with an EVM-level error (INVALID opcode; see newFailingCreationTx), B->C
// transfer] and asserts (FIX C1):
//   - ExecuteBatch does NOT abort the whole batch on the middle tx's EVM
//     fault (no error, 3 results back). Unlike a pre-execution rejection
//     (*TxRejected), a committed-but-faulted outcome (*ExecFailure) must be
//     endorsed, not treated as a batch-aborting Go error.
//   - The middle result's Status is common.StatusExecFailure (460), and its
//     own RWS carries F's nonce bump (0 -> 1): geth's core.ApplyMessage
//     commits the nonce increment (and gas deduction) for a faulted-but-applied
//     message exactly as it would for a success -- only a pre-execution
//     rejection skips this and reverts the snapshot.
//   - The other two txs still execute normally and succeed (Status 200), with
//     tx3 observing tx1's write via the overlay exactly as in
//     TestExecuteBatchExcludesNonceGap.
//   - The merged RWS (via MergeResults) carries both included txs' writes AND
//     the failed tx's nonce-bump write.
func TestExecuteBatchExecFailureDoesNotAbort(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:batch_exec_failure?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	keyA, addrA := newTestKey(t)
	keyB, addrB := newTestKey(t)
	_, addrC := newTestKey(t)
	keyF, addrF := newTestKey(t) // F is never seeded; ledger nonce/balance both 0.

	const (
		initialA = 1_000
		initialB = 50  // alone, insufficient to cover amount2 below
		amount1  = 300 // tx1: A -> B
		amount2  = 100 // tx3: B -> C; needs tx1's delivery (50+300=350 >= 100)
	)
	seedAccounts(t, backend, map[ethcommon.Address]int64{addrA: initialA, addrB: initialB})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)

	tx1 := newTransferTx(t, cfg.ChainConfig, keyA, addrB, big.NewInt(amount1), 0)
	failTx := newFailingCreationTx(t, cfg.ChainConfig, keyF, 0)
	tx3 := newTransferTx(t, cfg.ChainConfig, keyB, addrC, big.NewInt(amount2), 0)

	results, err := engine.ExecuteBatch(context.Background(), []*types.Transaction{tx1, failTx, tx3})
	if err != nil {
		t.Fatalf("ExecuteBatch must endorse an ExecFailure as a committed outcome, not abort the batch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if results[0].Status != 200 {
		t.Fatalf("tx1: status = %d, message = %q, want 200 (OK)", results[0].Status, results[0].Message)
	}

	if results[1].Status != common.StatusExecFailure {
		t.Fatalf("failTx: status = %d, message = %q, want %d (StatusExecFailure)", results[1].Status, results[1].Message, common.StatusExecFailure)
	}

	nonceKey := accKey(addrF, "nonce")
	var nonceWritten bool
	for _, w := range results[1].RWS.Writes {
		if w.Key != nonceKey {
			continue
		}
		nonceWritten = true
		got := bytesToUint64(w.Value)
		if got != 1 {
			t.Errorf("F's nonce after failTx = %d, want 1 (geth bumps the nonce for a committed-but-faulted message)", got)
		}
	}
	if !nonceWritten {
		t.Fatalf("expected failTx's RWS to include a nonce bump for F (key %q); writes = %+v", nonceKey, results[1].RWS.Writes)
	}

	if results[2].Status != 200 {
		t.Fatalf("tx3: status = %d, message = %q, want 200 (OK) — tx3 must still execute despite the middle tx's EVM fault", results[2].Status, results[2].Message)
	}

	balKey := accKey(addrB, "bal")
	var tx3Wrote bool
	for _, w := range results[2].RWS.Writes {
		if w.Key != balKey {
			continue
		}
		tx3Wrote = true
		got := bytesToUint256(w.Value)
		want := uint256.NewInt(uint64(initialB + amount1 - amount2))
		if got.Cmp(want) != 0 {
			t.Errorf("B balance after tx3 = %s, want %s (initialB=%d + amount1=%d - amount2=%d)",
				got, want, initialB, amount1, amount2)
		}
	}
	if !tx3Wrote {
		t.Fatalf("expected tx3's RWS to include a write to B's balance key %q; writes = %+v", balKey, results[2].RWS.Writes)
	}

	// The merged RWS (what actually lands in the committed Fabric tx) carries
	// both included txs' writes AND the failed tx's nonce bump.
	merged, events := MergeResults(results)
	if len(events) != 3 {
		t.Fatalf("expected 3 events (one per result, index = sub-index), got %d", len(events))
	}
	mergedWrites := map[string][]byte{}
	for _, w := range merged.Writes {
		mergedWrites[w.Key] = w.Value
	}
	if _, ok := mergedWrites[accKey(addrA, "bal")]; !ok {
		t.Errorf("merged RWS missing A's balance write from tx1; writes = %+v", merged.Writes)
	}
	if _, ok := mergedWrites[balKey]; !ok {
		t.Errorf("merged RWS missing B's balance write from tx3; writes = %+v", merged.Writes)
	}
	if got, ok := mergedWrites[nonceKey]; !ok {
		t.Errorf("merged RWS missing F's nonce bump from failTx; writes = %+v", merged.Writes)
	} else if bytesToUint64(got) != 1 {
		t.Errorf("merged RWS F's nonce = %d, want 1", bytesToUint64(got))
	}
}

// TestExecuteBatchSingleMatchesExecute asserts ExecuteBatch([tx]) returns the
// same RWS as Execute(tx) for one transaction: the N==1 path must be
// indistinguishable from today's Execute.
func TestExecuteBatchSingleMatchesExecute(t *testing.T) {
	backend, err := state.NewWriteDB(Channel, "file:batch_single_matches?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	key, addr := newTestKey(t)
	_, recipient := newTestKey(t)
	seedAccounts(t, backend, map[ethcommon.Address]int64{addr: 1_000})

	kvs := &testVersionedDBSnapshotter{db: backend}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	engine := NewEVMEngine(Namespace, kvs, cfg, false)

	tx := newTransferTx(t, cfg.ChainConfig, key, recipient, big.NewInt(250), 0)

	wantRes, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if wantRes.Status != 200 {
		t.Fatalf("Execute: status = %d, message = %q, want 200 (OK)", wantRes.Status, wantRes.Message)
	}

	gotResults, err := engine.ExecuteBatch(context.Background(), []*types.Transaction{tx})
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if len(gotResults) != 1 {
		t.Fatalf("expected 1 result, got %d", len(gotResults))
	}
	got := gotResults[0]

	if got.Status != wantRes.Status {
		t.Errorf("Status = %d, want %d", got.Status, wantRes.Status)
	}
	if got.Message != wantRes.Message {
		t.Errorf("Message = %q, want %q", got.Message, wantRes.Message)
	}
	if !bytes.Equal(got.Event, wantRes.Event) {
		t.Errorf("Event = %x, want %x", got.Event, wantRes.Event)
	}
	if !bytes.Equal(got.Payload, wantRes.Payload) {
		t.Errorf("Payload = %x, want %x", got.Payload, wantRes.Payload)
	}
	assertSameRWS(t, "ExecuteBatch([tx]) vs Execute(tx)", got.RWS, wantRes.RWS)
}
