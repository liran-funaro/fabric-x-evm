/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"errors"
	"math/big"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// failingReader is a ReadStore whose every Get fails, simulating a stale
// query-service view ("invalid or stale view", FailedPrecondition) whose
// lifetime a batch exceeded mid-execution.
type failingReader struct{ err error }

func (r *failingReader) Get(string, string) (*blocks.WriteRecord, error) { return nil, r.err }
func (r *failingReader) Close() error                                    { return nil }

// failingSnapshotter hands out failingReaders.
type failingSnapshotter struct{ err error }

func (s *failingSnapshotter) NewSnapshot(uint64) (ReadStore, error) {
	return &failingReader{err: s.err}, nil
}

// TestExecuteReadErrorDoesNotPanic verifies that a backing-store read failure
// surfaces as an error from Execute rather than panicking the endorser. Before
// the deferred-error fix, the vm.StateDB accessors panicked on a read error
// (they cannot return one inline), which crashed the whole process when a
// query-service view went stale. Now StateDB.setError records it and the
// Executor's Error() checks abort the tx. The read fails at the very first read
// (the sender nonce in PrepareMessage).
func TestExecuteReadErrorDoesNotPanic(t *testing.T) {
	readErr := errors.New("invalid or stale view")
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	eng := NewEVMEngine(Namespace, &failingSnapshotter{err: readErr}, cfg, false)

	key, _ := newTestKey(t)
	to := ethcommon.HexToAddress("0xdead")
	tx := newTransferTx(t, cfg.ChainConfig, key, to, big.NewInt(1), 0)

	_, err := eng.Execute(t.Context(), tx) // must not panic
	if err == nil {
		t.Fatal("expected a read error from Execute, got nil")
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("expected the error to wrap the read error %v, got %v", readErr, err)
	}
}

// TestExecuteBatchReadErrorDoesNotPanic is the batch-path counterpart: a read
// failure during the authoritative pass must abort ExecuteBatch with the read
// error (a genuine server fault, so the whole batch retries on a fresh view) --
// NOT panic, and NOT be misclassified as a per-tx *TxRejected exclusion.
func TestExecuteBatchReadErrorDoesNotPanic(t *testing.T) {
	readErr := errors.New("invalid or stale view")
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	eng := NewEVMEngine(Namespace, &failingSnapshotter{err: readErr}, cfg, false)

	key, _ := newTestKey(t)
	to := ethcommon.HexToAddress("0xdead")
	txs := []*types.Transaction{
		newTransferTx(t, cfg.ChainConfig, key, to, big.NewInt(1), 0),
		newTransferTx(t, cfg.ChainConfig, key, to, big.NewInt(1), 1),
	}

	_, err := eng.ExecuteBatch(t.Context(), txs) // must not panic
	if err == nil {
		t.Fatal("expected a read error from ExecuteBatch, got nil")
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("expected the error to wrap the read error %v, got %v", readErr, err)
	}
}

// backfillFailReader is a ReadStore where every key succeeds (absent) EXCEPT
// failKey, which fails with err. Unlike failingReader (every key fails, so the
// very first read -- the sender's nonce in PrepareMessage -- already aborts),
// this lets a tx run its normal reads to completion and fail ONLY on the one
// key StateDB.Result()'s read-set backfill (Task 1a) queries -- the scenario
// TestExecute_BackfillReadErrorAborts / TestExecuteBatch_BackfillReadErrorAborts
// below specifically target.
type backfillFailReader struct {
	failKey string
	err     error
}

func (r *backfillFailReader) Get(_, key string) (*blocks.WriteRecord, error) {
	if key == r.failKey {
		return nil, r.err
	}
	return nil, nil
}
func (r *backfillFailReader) Close() error { return nil }

// backfillFailSnapshotter hands out a single shared backfillFailReader.
type backfillFailSnapshotter struct{ reader *backfillFailReader }

func (s *backfillFailSnapshotter) NewSnapshot(uint64) (ReadStore, error) { return s.reader, nil }

// blindWriteDecorator returns a StateDB decorator that journals a pure blind
// write to key -- via the unexported putState, NOT SetState -- BEFORE the
// tx's own EVM execution runs. putState (unlike SetState) never touches the
// store itself, so this key has NO journaled read at all going into
// Result(): the exact shape StateDB.Result()'s backfill exists to complete,
// and the only way to make the backfill's store fetch the FIRST and ONLY
// query for that key (a SetState call would itself hit the store for the
// previous value, tripping the earlier Send/ApplyMessage Error() checkpoints
// instead of the one this test targets).
func blindWriteDecorator(key string, value []byte) func(ExtendedStateDB, *types.Transaction) ExtendedStateDB {
	return func(state ExtendedStateDB, _ *types.Transaction) ExtendedStateDB {
		if sdb, ok := state.(*StateDB); ok {
			sdb.putState(key, value)
		}
		return state
	}
}

// TestExecute_BackfillReadErrorAborts covers the gap flagged in Task 1a review:
// StateDB.Result()'s read-set backfill (added to guarantee a base version for
// every written key, for VersionedCache.ApplyWrites) performs its own fallible
// store read. If that read fails (e.g. the same stale query-service view
// Send/ApplyMessage already guard against, just hit one read later), classify
// must abort the tx via state.Error() -- NOT silently endorse a Result() whose
// read-set is missing that key, which would flow a wrong spec-version base
// into VersionedCache.
func TestExecute_BackfillReadErrorAborts(t *testing.T) {
	blindAddr := ethcommon.HexToAddress("0xb11nd")
	blindSlot := ethcommon.HexToHash("0x01")
	blindKey := storeKey(blindAddr, blindSlot)
	backfillErr := errors.New("invalid or stale view (backfill)")

	reader := &backfillFailReader{failKey: blindKey, err: backfillErr}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	eng := NewEVMEngine(Namespace, &backfillFailSnapshotter{reader: reader}, cfg, true)
	eng.SetStateDecorator(blindWriteDecorator(blindKey, ethcommon.HexToHash("0xCAFE").Bytes()))

	key, _ := newTestKey(t)
	to := ethcommon.HexToAddress("0xdead")
	tx := newTransferTx(t, cfg.ChainConfig, key, to, big.NewInt(0), 0)

	_, err := eng.Execute(t.Context(), tx)
	if err == nil {
		t.Fatal("expected Execute to abort on the backfill read failure, not silently succeed with an incomplete RWS")
	}
	if !errors.Is(err, backfillErr) {
		t.Fatalf("expected the error to wrap the backfill read error %v, got %v", backfillErr, err)
	}
}

// TestExecuteBatch_BackfillReadErrorAborts is the batch-path counterpart of
// TestExecute_BackfillReadErrorAborts: the authoritative pass must abort the
// whole batch (a genuine server fault, not a *TxRejected exclusion) when a
// tx's Result() backfill read fails.
func TestExecuteBatch_BackfillReadErrorAborts(t *testing.T) {
	blindAddr := ethcommon.HexToAddress("0xb11nd")
	blindSlot := ethcommon.HexToHash("0x01")
	blindKey := storeKey(blindAddr, blindSlot)
	backfillErr := errors.New("invalid or stale view (backfill)")

	reader := &backfillFailReader{failKey: blindKey, err: backfillErr}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	eng := NewEVMEngine(Namespace, &backfillFailSnapshotter{reader: reader}, cfg, true)
	eng.SetStateDecorator(blindWriteDecorator(blindKey, ethcommon.HexToHash("0xCAFE").Bytes()))

	key, _ := newTestKey(t)
	to := ethcommon.HexToAddress("0xdead")
	txs := []*types.Transaction{
		newTransferTx(t, cfg.ChainConfig, key, to, big.NewInt(0), 0),
		newTransferTx(t, cfg.ChainConfig, key, to, big.NewInt(0), 1),
	}

	_, err := eng.ExecuteBatch(t.Context(), txs)
	if err == nil {
		t.Fatal("expected ExecuteBatch to abort on the backfill read failure, not silently succeed with an incomplete RWS")
	}
	if !errors.Is(err, backfillErr) {
		t.Fatalf("expected the error to wrap the backfill read error %v, got %v", backfillErr, err)
	}
}
