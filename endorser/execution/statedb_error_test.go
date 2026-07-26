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
