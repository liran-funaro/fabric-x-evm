/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"crypto/ecdsa"
	"errors"
	"math/big"
	"sort"
	"sync"
	"testing"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// newBatchSender generates a fresh EOA key for these tests.
func newBatchSender(t *testing.T) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return priv, crypto.PubkeyToAddress(priv.PublicKey)
}

// signedValueTransfer builds and signs a plain ETH-value-transfer transaction
// (no contract call needed) from priv to `to`. Uses the same signer
// construction as EthClient.TxForCall/txForDeploy (see ethclient.go) so it
// passes the gateway's protected-tx (EIP-155) check.
func signedValueTransfer(t *testing.T, chainConfig *params.ChainConfig, priv *ecdsa.PrivateKey, nonce uint64, to common.Address, value *big.Int) *types.Transaction {
	t.Helper()
	tx := types.NewTx(&types.LegacyTx{
		Nonce: nonce,
		To:    &to,
		Value: value,
		Gas:   defaultGas,
	})
	bn, bt := GetCtxForSigner()
	signer := types.MakeSigner(chainConfig, bn, bt)
	signedTx, err := types.SignTx(tx, signer, priv)
	if err != nil {
		t.Fatal(err)
	}
	return signedTx
}

// TestBatchMergedCommit proves the drain-all two-phase merged-batch executor
// end-to-end, in-process (no docker, memory-mode endorser): several
// independent-sender txs plus a same-sender consecutive-nonce pair commit
// (whether merged into one Fabric tx or not) and are each individually
// addressable through the normal eth_* RPCs with their own receipt and a
// distinct, contiguous transactionIndex; and a nonce-gap tx is excluded from
// commit until its predecessor fills the gap.
func TestBatchMergedCommit(t *testing.T) {
	t.Run("independent_and_sequential_senders_get_distinct_receipts", func(t *testing.T) {
		th, err := NewLocalTestHarness(t, TestLogger{T: t}, evmConfig(""), "", "fabric-x", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { th.Stop() })

		node := th.Gateways[0]
		ec, err := NewNativeEthClient(node)
		if err != nil {
			t.Fatal(err)
		}

		_, recipient := newBatchSender(t)

		// 3 independent senders (one tx each, nonce 0) + 1 sender contributing
		// two consecutive-nonce txs (0, 1): 5 EVM txs total, from 4 distinct EOAs.
		type sender struct {
			priv *ecdsa.PrivateKey
			addr common.Address
		}
		senders := make([]sender, 4)
		primer, err := th.NewStatePrimer()
		if err != nil {
			t.Fatalf("NewStatePrimer: %v", err)
		}
		for i := range senders {
			priv, addr := newBatchSender(t)
			senders[i] = sender{priv: priv, addr: addr}
			primer.SetBalance(addr, big.NewInt(1_000_000))
		}
		if err := primer.Commit(t.Context(), true); err != nil {
			t.Fatalf("fund senders: %v", err)
		}

		value := big.NewInt(1000)
		txA := signedValueTransfer(t, th.ethChainConfig, senders[0].priv, 0, recipient, value)
		txB := signedValueTransfer(t, th.ethChainConfig, senders[1].priv, 0, recipient, value)
		txC := signedValueTransfer(t, th.ethChainConfig, senders[2].priv, 0, recipient, value)
		txD0 := signedValueTransfer(t, th.ethChainConfig, senders[3].priv, 0, recipient, value)
		txD1 := signedValueTransfer(t, th.ethChainConfig, senders[3].priv, 1, recipient, value)

		allTxs := []*types.Transaction{txA, txB, txC, txD0, txD1}

		// Submit concurrently to maximize the chance that the drain-all
		// executor picks all of them up in a single cycle (one merged Fabric
		// tx) -- but keep D's two txs in program order within their own
		// goroutine: the executor's authoritative pass runs a batch strictly
		// in pending-pool insertion order (no dependency reordering, see
		// PendingPool), so D0 must be added before D1 or D1 would see a
		// nonce gap and be excluded from that cycle (it would still commit
		// later, on its own, but that would defeat the point of this
		// sub-test: proving both land via the SAME admission sequence).
		var wg sync.WaitGroup
		submit := func(tx *types.Transaction) {
			defer wg.Done()
			if err := ec.SendTransaction(t.Context(), tx); err != nil {
				t.Errorf("SendTransaction(%s): %v", tx.Hash(), err)
			}
		}
		wg.Add(4)
		go submit(txA)
		go submit(txB)
		go submit(txC)
		go func() {
			defer wg.Done()
			if err := ec.SendTransaction(t.Context(), txD0); err != nil {
				t.Errorf("SendTransaction(D0): %v", err)
				return
			}
			if err := ec.SendTransaction(t.Context(), txD1); err != nil {
				t.Errorf("SendTransaction(D1): %v", err)
			}
		}()
		wg.Wait()

		for _, tx := range allTxs {
			waitForCommitT(t, ec, tx)
		}

		type blockIndex struct {
			block uint64
			index uint
		}
		seen := map[blockIndex]common.Hash{}
		byBlock := map[uint64][]uint{}

		for _, tx := range allTxs {
			receipt, err := ec.TransactionReceipt(t.Context(), tx.Hash())
			if err != nil {
				t.Fatalf("TransactionReceipt(%s): %v", tx.Hash(), err)
			}
			if receipt.Status != types.ReceiptStatusSuccessful {
				t.Errorf("tx %s: receipt.Status = %d, want 1 (success)", tx.Hash(), receipt.Status)
			}

			gotTx, isPending, err := ec.TransactionByHash(t.Context(), tx.Hash())
			if err != nil {
				t.Fatalf("TransactionByHash(%s): %v", tx.Hash(), err)
			}
			if gotTx == nil || isPending {
				t.Errorf("tx %s: expected resolved+committed, got tx=%v isPending=%v", tx.Hash(), gotTx, isPending)
			}

			k := blockIndex{block: receipt.BlockNumber.Uint64(), index: receipt.TransactionIndex}
			if prev, dup := seen[k]; dup {
				t.Fatalf("transactionIndex collision: block %d index %d used by both %s and %s",
					k.block, k.index, prev, tx.Hash())
			}
			seen[k] = tx.Hash()
			byBlock[k.block] = append(byBlock[k.block], k.index)
		}

		// Distinct, contiguous transactionIndex per block: proves the flat
		// block-global counter (see chain.go's ConvertToDomain) rather than a
		// shared/collided index across a merged batch's sub-txs.
		mergedBatchObserved := false
		for block, indices := range byBlock {
			sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
			for i := 1; i < len(indices); i++ {
				if indices[i] != indices[i-1]+1 {
					t.Errorf("block %d: transactionIndex values %v are not contiguous", block, indices)
				}
			}
			if len(indices) > 1 {
				mergedBatchObserved = true
			}
		}
		if !mergedBatchObserved {
			// Not a correctness failure: it means every one of the 5 txs
			// happened to be drained into its own singleton-batch Fabric tx
			// in this run (the drain-all executor's cycle can, in principle,
			// complete faster than this goroutine-based submission spreads
			// the 5 SendTransaction calls out). The per-tx assertions above
			// (status, resolution, index uniqueness, nonce bookkeeping) still
			// hold either way; only the "sub-txs share one Fabric tx" shape
			// goes unexercised this run.
			t.Log("all 5 txs landed in singleton batches this run -- merged-batch " +
				"(>1 EVM tx sharing one Fabric tx) sub-index behavior was not exercised")
		}

		finalNonce, err := ec.NonceAt(t.Context(), senders[3].addr, nil)
		if err != nil {
			t.Fatalf("NonceAt(D): %v", err)
		}
		if finalNonce != 2 {
			t.Errorf("sender D final nonce = %d, want 2 (two committed txs)", finalNonce)
		}
	})

	t.Run("nonce_gap_excluded_until_predecessor_commits", func(t *testing.T) {
		th, err := NewLocalTestHarness(t, TestLogger{T: t}, evmConfig(""), "", "fabric-x", nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { th.Stop() })

		node := th.Gateways[0]
		ec, err := NewNativeEthClient(node)
		if err != nil {
			t.Fatal(err)
		}

		_, recipient := newBatchSender(t)
		priv, addr := newBatchSender(t)
		primer, err := th.NewStatePrimer()
		if err != nil {
			t.Fatalf("NewStatePrimer: %v", err)
		}
		if err := primer.SetBalance(addr, big.NewInt(1_000_000)).Commit(t.Context(), true); err != nil {
			t.Fatalf("fund sender: %v", err)
		}

		value := big.NewInt(1000)
		txGap := signedValueTransfer(t, th.ethChainConfig, priv, 1, recipient, value) // nonce 1, no nonce 0 yet
		txFiller := signedValueTransfer(t, th.ethChainConfig, priv, 0, recipient, value)

		if err := ec.SendTransaction(t.Context(), txGap); err != nil {
			t.Fatalf("SendTransaction(gap tx): %v", err)
		}

		// This is a logical guarantee, not a timing race: txGap (nonce 1) can
		// never commit without nonce 0 present -- EVMEngine.ExecuteBatch's
		// authoritative pass excludes a nonce-gap tx rather than committing
		// it (see endorser/execution/batch_executor.go) -- and we have not
		// yet submitted nonce 0. So asserting "not committed" here cannot
		// flake regardless of how many drain cycles the executor runs before
		// we check.
		if _, err := ec.TransactionReceipt(t.Context(), txGap.Hash()); !errors.Is(err, ethereum.NotFound) {
			t.Fatalf("gap tx: TransactionReceipt error = %v, want ethereum.NotFound (must still be pending)", err)
		}
		gotTx, isPending, err := ec.TransactionByHash(t.Context(), txGap.Hash())
		if err != nil {
			t.Fatalf("TransactionByHash(gap tx): %v", err)
		}
		if gotTx == nil || !isPending {
			t.Fatalf("gap tx: expected isPending=true before its predecessor is submitted, got tx=%v isPending=%v", gotTx, isPending)
		}

		if err := ec.SendTransaction(t.Context(), txFiller); err != nil {
			t.Fatalf("SendTransaction(filler tx): %v", err)
		}

		// Both eventually commit. The filler may land in the same drain
		// cycle as the (still-pending) gap tx -- in which case the gap tx is
		// excluded from THAT batch too, since the authoritative pass runs in
		// pending-pool insertion order rather than nonce order -- or the
		// filler may commit alone first. Either way, once the filler's
		// nonce-0 write lands, a later drain cycle re-admits the gap tx and
		// it now succeeds. waitForCommit's 10s ceiling covers however many
		// cycles that takes.
		waitForCommitT(t, ec, txFiller)
		waitForCommitT(t, ec, txGap)

		for _, tx := range []*types.Transaction{txFiller, txGap} {
			receipt, err := ec.TransactionReceipt(t.Context(), tx.Hash())
			if err != nil {
				t.Fatalf("TransactionReceipt(%s): %v", tx.Hash(), err)
			}
			if receipt.Status != types.ReceiptStatusSuccessful {
				t.Errorf("tx %s: receipt.Status = %d, want 1 (success)", tx.Hash(), receipt.Status)
			}
		}

		finalNonce, err := ec.NonceAt(t.Context(), addr, nil)
		if err != nil {
			t.Fatalf("NonceAt: %v", err)
		}
		if finalNonce != 2 {
			t.Errorf("final nonce = %d, want 2 (two committed txs)", finalNonce)
		}
	})
}
