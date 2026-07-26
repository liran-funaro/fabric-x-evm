/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package testimpl

import (
	"context"
	"math/big"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	fxcommon "github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/endorser/storage"
)

// TestWrapperBatchAppliesNoncePriming reproduces the perf-harness wiring in
// isolation: base engine -> EVMEngineWrapper -> SetBalancePriming, then call the
// WRAPPER's ExecuteMergedBatch (which promotes to the embedded base engine's
// method). A tx with a high nonce on a fresh chain is nonce-too-high and would
// be excluded unless the balance/nonce-priming decorator is applied in the
// batch path. Asserts it is NOT excluded.
func TestWrapperBatchAppliesNoncePriming(t *testing.T) {
	kvs := storage.NewLightKVS(8)
	cfg := execution.EVMConfig{ChainConfig: fxcommon.BuildChainConfig(4011)}
	base := execution.NewEVMEngine("evm", kvs, cfg, false)

	wrapped := NewEVMEngineWrapper("evm", kvs, cfg, false, base)
	wrapped.SetBalancePriming(&BalancePrimingConfig{
		Enabled:         true,
		ContractAddress: ethcommon.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		MappingPosition: 9,
	})

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	to := ethcommon.HexToAddress("0xdead")
	signer := types.MakeSigner(cfg.ChainConfig, big.NewInt(0), 1_000_000)
	tx, err := types.SignNewTx(key, signer, &types.LegacyTx{
		Nonce: 100, // high nonce on a fresh chain: only nonce priming lets it through
		Gas:   21_000,
		To:    &to,
		Value: big.NewInt(0),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, outcomes, err := wrapped.ExecuteMergedBatch(context.Background(), []*types.Transaction{tx})
	if err != nil {
		t.Fatalf("ExecuteMergedBatch error: %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("want 1 outcome, got %d", len(outcomes))
	}
	if fxcommon.IsExcludedOutcome(outcomes[0].Status) {
		t.Fatalf("tx EXCLUDED (status %d): balance/nonce priming decorator NOT applied in the batch path",
			outcomes[0].Status)
	}
}

// TestWrapperMultiBatchAppliesNoncePriming is the N>1 counterpart: the live perf
// harness runs batches of many txs through the two-phase (warm + authoritative)
// path, with monotonicVersions=true (fabric-x). Every high-nonce tx must be
// included, not excluded, i.e. the decorator applies in BOTH passes.
func TestWrapperMultiBatchAppliesNoncePriming(t *testing.T) {
	kvs := storage.NewLightKVS(8)
	cfg := execution.EVMConfig{ChainConfig: fxcommon.BuildChainConfig(4011)}
	base := execution.NewEVMEngine("evm", kvs, cfg, true) // monotonicVersions=true, as in fabric-x

	wrapped := NewEVMEngineWrapper("evm", kvs, cfg, true, base)
	wrapped.SetBalancePriming(&BalancePrimingConfig{
		Enabled:         true,
		ContractAddress: ethcommon.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		MappingPosition: 9,
	})

	signer := types.MakeSigner(cfg.ChainConfig, big.NewInt(0), 1_000_000)
	to := ethcommon.HexToAddress("0xdead")
	var txs []*types.Transaction
	for range 5 {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		tx, err := types.SignNewTx(key, signer, &types.LegacyTx{
			Nonce: 100, // high nonce, distinct fresh sender each
			Gas:   21_000,
			To:    &to,
			Value: big.NewInt(0),
		})
		if err != nil {
			t.Fatal(err)
		}
		txs = append(txs, tx)
	}

	_, outcomes, err := wrapped.ExecuteMergedBatch(context.Background(), txs)
	if err != nil {
		t.Fatalf("ExecuteMergedBatch error: %v", err)
	}
	if len(outcomes) != len(txs) {
		t.Fatalf("want %d outcomes, got %d", len(txs), len(outcomes))
	}
	for i, o := range outcomes {
		if fxcommon.IsExcludedOutcome(o.Status) {
			t.Fatalf("sub-tx %d EXCLUDED (status %d): decorator not applied in the N>1 two-phase path", i, o.Status)
		}
	}
}
