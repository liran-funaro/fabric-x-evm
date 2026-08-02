/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math"
	"math/big"
	"sort"
	"sync"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/notification"
	"github.com/stretchr/testify/require"
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

// blockIndex identifies a committed tx's (block, transactionIndex) position, used
// to assert distinct, contiguous per-block indices across a pipelined run.
type blockIndex struct {
	block uint64
	index uint
}

// TestBatchPipelinedCommit exercises the pipelined executor + cross-batch cache
// (Tasks 4-7) end-to-end through the gateway:
//   - 1a proves N sequential-nonce txs pipeline into N committer batches that all
//     commit with distinct, contiguous receipts (block-sync).
//   - 1b proves DETERMINISTICALLY (notifier mode, no verdicts delivered) that the
//     executor submits later batches before earlier ones resolve, bounded by the
//     in-flight window -- i.e. it does not serialize on commit.
//   - 2 proves a false-abort verdict cascades: the suffix re-executes, is absorbed
//     as nonce-too-low, all txs stay committed, earlier batches are untouched.
//   - 3 (+ sibling) proves a withheld notification falls back to committed via the
//     query-service reader (client-timer branch + explicit sidecar-timeout branch).
func TestBatchPipelinedCommit(t *testing.T) {
	// SubmitterCount==1 gives these tests a single, ordered submission stream to the
	// orderer. The pipelined executor endorses nonce k+1 against nonce k's still-
	// uncommitted cache write, so the committer must validate them in nonce order
	// (block k before block k+1) for the read-version chain to hold. The default 16
	// BatchSubmitter workers drain the endorsement channel concurrently and can
	// deliver dependent txs to the orderer out of nonce order, which on a real ledger
	// self-corrects via the abort->cascade->re-execute path but makes these
	// clean-commit assertions non-deterministic. One worker still exercises full
	// pipelining: the executor never blocks on commit, so multiple batches remain
	// in-flight (MaxInflightObserved > 1) regardless of the worker count.
	orderedSubmit := map[string]any{"Gateway.SubmitterCount": 1}

	// Step 1a: pipelined batches commit with distinct receipts (block-sync).
	t.Run("pipelined_batches_commit_with_distinct_receipts", func(t *testing.T) {
		th, err := NewLocalTestHarness(t, TestLogger{T: t}, evmConfig(""), "", "fabric-x", orderedSubmit)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { th.Stop() })

		node := th.Gateways[0]
		node.SetMaxBatchSize(1) // one EVM tx per committer batch -> N txs == N batches
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
		if err := primer.SetBalance(addr, big.NewInt(1_000_000_000)).Commit(t.Context(), true); err != nil {
			t.Fatalf("fund sender: %v", err)
		}

		const n = 16
		value := big.NewInt(1000)
		txs := make([]*types.Transaction, n)
		for i := 0; i < n; i++ {
			txs[i] = signedValueTransfer(t, th.ethChainConfig, priv, uint64(i), recipient, value)
		}

		// Submit in nonce order so no drain cycle ever sees a gap.
		for _, tx := range txs {
			if err := ec.SendTransaction(t.Context(), tx); err != nil {
				t.Fatalf("SendTransaction(nonce %d): %v", tx.Nonce(), err)
			}
		}
		for _, tx := range txs {
			waitForCommitT(t, ec, tx)
		}

		seen := map[blockIndex]common.Hash{}
		byBlock := map[uint64][]uint{}
		for _, tx := range txs {
			receipt, err := ec.TransactionReceipt(t.Context(), tx.Hash())
			if err != nil {
				t.Fatalf("TransactionReceipt(%s): %v", tx.Hash(), err)
			}
			if receipt.Status != types.ReceiptStatusSuccessful {
				t.Errorf("tx %s: receipt.Status = %d, want 1 (success)", tx.Hash(), receipt.Status)
			}
			k := blockIndex{block: receipt.BlockNumber.Uint64(), index: receipt.TransactionIndex}
			if prev, dup := seen[k]; dup {
				t.Fatalf("transactionIndex collision: block %d index %d used by both %s and %s",
					k.block, k.index, prev, tx.Hash())
			}
			seen[k] = tx.Hash()
			byBlock[k.block] = append(byBlock[k.block], k.index)
		}
		// Distinct, contiguous transactionIndex per block.
		for block, indices := range byBlock {
			sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
			for i := 1; i < len(indices); i++ {
				if indices[i] != indices[i-1]+1 {
					t.Errorf("block %d: transactionIndex values %v are not contiguous", block, indices)
				}
			}
		}

		finalNonce, err := ec.NonceAt(t.Context(), addr, nil)
		if err != nil {
			t.Fatalf("NonceAt: %v", err)
		}
		if finalNonce != n {
			t.Errorf("final nonce = %d, want %d", finalNonce, n)
		}

		// Pipelining hook count: > 1 in-flight batch proves the executor did not
		// serialize on commit. (Deterministically re-proven in 1b.)
		if got := node.MaxInflightObserved(); got < 2 {
			t.Errorf("MaxInflightObserved() = %d, want >= 2 (executor must not serialize on commit)", got)
		}
	})

	// Step 1b: executor does not serialize on commit (notifier mode, deterministic).
	t.Run("executor_does_not_serialize_on_commit", func(t *testing.T) {
		th, ctl, err := NewLocalTestHarnessWithNotifier(t, TestLogger{T: t}, evmConfig(""), "fabric-x",
			map[string]any{"Gateway.MaxInflight": 4, "Gateway.SubmitterCount": 1}) // bound the window + ordered submit before Start
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { th.Stop() })

		node := th.Gateways[0]
		node.SetMaxBatchSize(1)
		ec, err := NewNativeEthClient(node)
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()

		_, recipient := newBatchSender(t)
		priv, _ := newBatchSender(t)
		primer, err := th.NewStatePrimer()
		if err != nil {
			t.Fatalf("NewStatePrimer: %v", err)
		}
		if err := primer.SetBalance(crypto.PubkeyToAddress(priv.PublicKey), big.NewInt(1_000_000_000)).Commit(ctx, true); err != nil {
			t.Fatalf("fund sender: %v", err)
		}

		const n = 6
		value := big.NewInt(1000)
		txs := make([]*types.Transaction, n)
		for i := 0; i < n; i++ {
			txs[i] = signedValueTransfer(t, th.ethChainConfig, priv, uint64(i), recipient, value)
			if err := ec.SendTransaction(ctx, txs[i]); err != nil {
				t.Fatalf("SendTransaction(nonce %d): %v", i, err)
			}
		}

		// Deliver NO verdicts: the window (4) fills, then the 5th/6th batches block
		// on the semaphore BEFORE they can Watch, so exactly 4 TxIDs are registered.
		require.Eventually(t, func() bool { return len(ctl.Watched()) == 4 },
			10*time.Second, 5*time.Millisecond, "expected exactly 4 in-flight batches watched")
		require.Equal(t, 4, node.MaxInflightObserved(), "peak in-flight must reach the window bound (4)")

		// Backpressure plateau: with no verdicts delivered, no 5th batch is ever
		// watched. A serialize-on-commit executor would submit 1 and block forever
		// waiting for a commit that never comes (watermark would stay 1).
		require.Never(t, func() bool { return len(ctl.Watched()) > 4 },
			300*time.Millisecond, 20*time.Millisecond,
			"window is full; no further batch may be submitted until a slot frees")

		// Cleanup: free the window so the remaining txs drain and the registry
		// empties (keeps the run clean under -race -- no blocked executor at exit).
		for _, id := range ctl.Watched() {
			require.NoError(t, ctl.Handler.Handle(ctx, []notification.TxStatusEvent{
				{TxID: id, Status: committerpb.Status_COMMITTED},
			}))
		}
		for _, tx := range txs {
			waitForCommitT(t, ec, tx) // the freed slots admit nonce 4 & 5
		}
		require.Eventually(t, func() bool { return len(ctl.Watched()) == n },
			10*time.Second, 5*time.Millisecond)
		for _, id := range ctl.Watched() {
			require.NoError(t, ctl.Handler.Handle(ctx, []notification.TxStatusEvent{
				{TxID: id, Status: committerpb.Status_COMMITTED},
			}))
		}
		require.Eventually(t, func() bool { return node.InflightLen() == 0 },
			10*time.Second, 5*time.Millisecond, "in-flight registry must drain")
	})

	// Step 2: cascade re-executes the suffix and stays consistent (notifier mode).
	t.Run("cascade_reexecutes_suffix_and_stays_consistent", func(t *testing.T) {
		th, ctl, err := NewLocalTestHarnessWithNotifier(t, TestLogger{T: t}, evmConfig(""), "fabric-x", orderedSubmit)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { th.Stop() })

		node := th.Gateways[0]
		node.SetMaxBatchSize(1) // default (long) MaxInflight/NotifyTimeout: the test drives all resolution
		ec, err := NewNativeEthClient(node)
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()

		_, recipient := newBatchSender(t)
		priv, addr := newBatchSender(t)
		primer, err := th.NewStatePrimer()
		if err != nil {
			t.Fatalf("NewStatePrimer: %v", err)
		}
		if err := primer.SetBalance(addr, big.NewInt(1_000_000_000)).Commit(ctx, true); err != nil {
			t.Fatalf("fund sender: %v", err)
		}

		value := big.NewInt(1000)
		txs := make([]*types.Transaction, 3)
		for i := range txs {
			txs[i] = signedValueTransfer(t, th.ethChainConfig, priv, uint64(i), recipient, value)
			if err := ec.SendTransaction(ctx, txs[i]); err != nil {
				t.Fatalf("SendTransaction(nonce %d): %v", i, err)
			}
		}
		// They commit on the in-process ledger and sync to the endorser DB
		// regardless of notifier resolution (gateway is not the block handler).
		for _, tx := range txs {
			waitForCommitT(t, ec, tx)
		}
		require.Eventually(t, func() bool { return len(ctl.Watched()) == 3 },
			10*time.Second, 5*time.Millisecond)
		b := ctl.Watched() // b[0..2] == tx0..tx2 batches

		// Resolve b[0] committed, then deliver a FALSE abort for the MIDDLE batch.
		// This drives resolveInflight(b[1], false) -> cascadeFrom(b[1]): detach
		// b[1] and b[2] (suffix), rebuild the cache from the survivor (b[0]),
		// re-queue tx1 & tx2. Because tx1 & tx2 already committed AND synced to the
		// endorser DB, re-execution sees their nonces bumped -> excluded as
		// nonce-too-low -> NO new in-flight batch. (See task-8-supplement Part D.)
		require.NoError(t, ctl.Handler.Handle(ctx, []notification.TxStatusEvent{
			{TxID: b[0], Status: committerpb.Status_COMMITTED},
		}))
		require.NoError(t, ctl.Handler.Handle(ctx, []notification.TxStatusEvent{
			{TxID: b[1], Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
		}))

		require.Eventually(t, func() bool { return node.CascadeCount() == 1 },
			10*time.Second, 5*time.Millisecond, "exactly one real cascade must fire")
		require.Eventually(t, func() bool { return node.InflightLen() == 0 },
			10*time.Second, 5*time.Millisecond, "registry must drain (no re-submission, no deadlock)")
		require.Equal(t, 1, node.CascadeCount(), "exactly one cascade")

		// All three keep valid receipts (tx0 untouched; tx1/tx2 keep their original
		// commit receipts -- the re-execution never re-committed them).
		for _, tx := range txs {
			receipt, err := ec.TransactionReceipt(ctx, tx.Hash())
			if err != nil {
				t.Fatalf("TransactionReceipt(%s): %v", tx.Hash(), err)
			}
			if receipt.Status != types.ReceiptStatusSuccessful {
				t.Errorf("tx %s: receipt.Status = %d, want 1 (success)", tx.Hash(), receipt.Status)
			}
		}
		finalNonce, err := ec.NonceAt(ctx, addr, nil)
		if err != nil {
			t.Fatalf("NonceAt: %v", err)
		}
		if finalNonce != 3 {
			t.Errorf("final nonce = %d, want 3", finalNonce)
		}
	})

	// Step 3: a dropped notification falls back to committed via the client timer.
	t.Run("dropped_notification_falls_back_to_committed", func(t *testing.T) {
		th, _, err := NewLocalTestHarnessWithNotifier(t, TestLogger{T: t}, evmConfig(""), "fabric-x",
			map[string]any{"Gateway.NotifyTimeout": 300 * time.Millisecond, "Gateway.SubmitterCount": 1}) // client timer fires quickly; ordered submit
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { th.Stop() })

		node := th.Gateways[0]
		node.SetMaxBatchSize(1)
		ec, err := NewNativeEthClient(node)
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()

		// Committed-version reader that reports state fully advanced (>= any spec
		// version) so the timeout fallback resolves the batch COMMITTED, not rolled
		// back. Set before submitting: the write happens-before the executor's
		// Watch (which arms the timer whose fire later reads it).
		node.SetCommittedVersionReader(func(_ context.Context, _ string) (uint64, bool, error) {
			return math.MaxUint64, true, nil
		})

		_, recipient := newBatchSender(t)
		priv, addr := newBatchSender(t)
		primer, err := th.NewStatePrimer()
		if err != nil {
			t.Fatalf("NewStatePrimer: %v", err)
		}
		if err := primer.SetBalance(addr, big.NewInt(1_000_000_000)).Commit(ctx, true); err != nil {
			t.Fatalf("fund sender: %v", err)
		}

		tx0 := signedValueTransfer(t, th.ethChainConfig, priv, 0, recipient, big.NewInt(1000))
		if err := ec.SendTransaction(ctx, tx0); err != nil {
			t.Fatalf("SendTransaction: %v", err)
		}
		waitForCommitT(t, ec, tx0) // committed on the ledger

		// Withhold the notification: the 300ms client timer fires -> onTimeout ->
		// fallback (reader >= spec) -> resolveInflight(committed). No rollback.
		require.Eventually(t, func() bool { return node.InflightLen() == 0 },
			10*time.Second, 5*time.Millisecond, "the fallback must resolve the batch committed")
		require.Equal(t, 0, node.CascadeCount(), "a committed fallback must not roll back")

		receipt, err := ec.TransactionReceipt(ctx, tx0.Hash())
		if err != nil {
			t.Fatalf("TransactionReceipt: %v", err)
		}
		if receipt.Status != types.ReceiptStatusSuccessful {
			t.Errorf("tx0 receipt.Status = %d, want 1 (success)", receipt.Status)
		}
		finalNonce, err := ec.NonceAt(ctx, addr, nil)
		if err != nil {
			t.Fatalf("NonceAt: %v", err)
		}
		if finalNonce != 1 {
			t.Errorf("final nonce = %d, want 1", finalNonce)
		}
	})

	// Step 3 sibling: an explicit STATUS_UNSPECIFIED (sidecar-timeout) event also
	// falls back to committed -- exercising the Handle branch (vs the client-timer
	// branch above). Long NotifyTimeout so only the explicit event resolves.
	t.Run("sidecar_timeout_event_falls_back_to_committed", func(t *testing.T) {
		th, ctl, err := NewLocalTestHarnessWithNotifier(t, TestLogger{T: t}, evmConfig(""), "fabric-x", orderedSubmit)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { th.Stop() })

		node := th.Gateways[0]
		node.SetMaxBatchSize(1)
		ec, err := NewNativeEthClient(node)
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()

		node.SetCommittedVersionReader(func(_ context.Context, _ string) (uint64, bool, error) {
			return math.MaxUint64, true, nil
		})

		_, recipient := newBatchSender(t)
		priv, addr := newBatchSender(t)
		primer, err := th.NewStatePrimer()
		if err != nil {
			t.Fatalf("NewStatePrimer: %v", err)
		}
		if err := primer.SetBalance(addr, big.NewInt(1_000_000_000)).Commit(ctx, true); err != nil {
			t.Fatalf("fund sender: %v", err)
		}

		tx0 := signedValueTransfer(t, th.ethChainConfig, priv, 0, recipient, big.NewInt(1000))
		if err := ec.SendTransaction(ctx, tx0); err != nil {
			t.Fatalf("SendTransaction: %v", err)
		}
		waitForCommitT(t, ec, tx0)

		require.Eventually(t, func() bool { return len(ctl.Watched()) == 1 },
			10*time.Second, 5*time.Millisecond)
		b := ctl.Watched()

		// Deliver the sidecar's own timeout sentinel -> Handle defers to the query
		// fallback (reader >= spec) -> resolveInflight(committed).
		require.NoError(t, ctl.Handler.Handle(ctx, []notification.TxStatusEvent{
			{TxID: b[0], Status: committerpb.Status_STATUS_UNSPECIFIED},
		}))

		require.Eventually(t, func() bool { return node.InflightLen() == 0 },
			10*time.Second, 5*time.Millisecond)
		require.Equal(t, 0, node.CascadeCount(), "a committed fallback must not roll back")

		receipt, err := ec.TransactionReceipt(ctx, tx0.Hash())
		if err != nil {
			t.Fatalf("TransactionReceipt: %v", err)
		}
		if receipt.Status != types.ReceiptStatusSuccessful {
			t.Errorf("tx0 receipt.Status = %d, want 1 (success)", receipt.Status)
		}
		finalNonce, err := ec.NonceAt(ctx, addr, nil)
		if err != nil {
			t.Fatalf("NonceAt: %v", err)
		}
		if finalNonce != 1 {
			t.Errorf("final nonce = %d, want 1", finalNonce)
		}
	})
}

// TestBatchGenuineAbort proves the pipelined executor RECOVERS from a genuine
// committer MVCC abort caused by out-of-order orderer submission: batch k+1 (endorsed
// against k's uncommitted cache write) is submitted to the orderer BEFORE k, so the
// committer aborts k+1; the gateway cascades k+1 back to pending, k commits, k+1
// re-executes against committed state into a NEW committer batch, and that re-commits.
// Both txs end committed exactly once; exactly one cascade fires. (Distinct from
// TestBatchPipelinedCommit's FALSE-abort case, where re-execution is absorbed as
// nonce-too-low and produces no new batch.) Also regression-guards the production
// SubmitterCount=1 clamp by injecting the exact reordering it prevents.
func TestBatchGenuineAbort(t *testing.T) {
	th, ctl, err := NewLocalTestHarnessWithSubmitterControl(t, TestLogger{T: t}, evmConfig(""), "fabric-x", 2, nil)
	require.NoError(t, err)
	t.Cleanup(func() { th.Stop() })

	node := th.Gateways[0]
	node.SetMaxBatchSize(1) // one EVM tx per committer batch
	ec, err := NewNativeEthClient(node)
	require.NoError(t, err)
	ctx := t.Context()

	_, recipient := newBatchSender(t)
	priv, addr := newBatchSender(t)
	primer, err := th.NewStatePrimer()
	require.NoError(t, err)
	// Pre-commit the sender's account so its keys exist at a committed version
	// (removes the fresh-absent-key MVCC ambiguity -- the read-version mismatch on
	// reorder is then unambiguous).
	require.NoError(t, primer.SetBalance(addr, big.NewInt(1_000_000_000)).Commit(ctx, true))

	// Arm the controllable submitter only now: priming's own Commit(ctx, true) call
	// above submits through this same wrapped submitter (see SubmitterControl's doc
	// comment), and unarmed Submit calls pass straight through so that blocking commit
	// wait can actually complete.
	ctl.Arm()

	value := big.NewInt(1000)
	tx0 := signedValueTransfer(t, th.ethChainConfig, priv, 0, recipient, value)
	tx1 := signedValueTransfer(t, th.ethChainConfig, priv, 1, recipient, value)

	// Submit in nonce order so the executor drains tx0 (batch k) then tx1 (batch k+1),
	// endorsing k+1 against k's cache write. Both are buffered by the controllable
	// submitter (Submit returns nil), so both register in-flight without reaching the
	// orderer yet.
	require.NoError(t, ec.SendTransaction(ctx, tx0))
	require.NoError(t, ec.SendTransaction(ctx, tx1))

	require.Eventually(t, func() bool { return ctl.BufferedLen() == 2 },
		10*time.Second, 5*time.Millisecond, "both batches must be buffered before release")

	// Forward k+1 BEFORE k -> committer MVCC-aborts k+1 -> cascade -> k commits ->
	// tx1 re-executes into k+1' -> passes through the submitter -> re-commits.
	require.NoError(t, ctl.ReleaseReversed(ctx))

	// Both txs end committed (tx1 via its re-execution).
	waitForCommitT(t, ec, tx0)
	waitForCommitT(t, ec, tx1)

	// Exactly one cascade fired (k+1's genuine abort); the re-execution commits cleanly.
	require.Eventually(t, func() bool { return node.CascadeCount() == 1 },
		10*time.Second, 5*time.Millisecond, "exactly one genuine cascade must fire")
	require.Eventually(t, func() bool { return node.InflightLen() == 0 },
		10*time.Second, 5*time.Millisecond, "in-flight registry must drain")
	require.Equal(t, 1, node.CascadeCount(), "exactly one cascade")

	// Both receipts successful; each applied EXACTLY once despite the abort+re-exec.
	for _, tx := range []*types.Transaction{tx0, tx1} {
		receipt, err := ec.TransactionReceipt(ctx, tx.Hash())
		require.NoErrorf(t, err, "TransactionReceipt(%s)", tx.Hash())
		require.Equalf(t, types.ReceiptStatusSuccessful, receipt.Status, "tx %s status", tx.Hash())
	}

	finalNonce, err := ec.NonceAt(ctx, addr, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), finalNonce, "sender nonce = 2 (both txs applied once)")

	// Recipient received exactly 2*value -- proves tx1's transfer applied ONCE (the
	// aborted attempt k+1 left no committed effect; the re-execution k+1' applied it).
	recipientBal, err := ec.BalanceAt(ctx, recipient, nil)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(0).Mul(value, big.NewInt(2)), recipientBal, "recipient got exactly 2*value")
}

// TestBatchPipelinedHotKeyNoStaleReadCascade is the end-to-end guard for the
// pipelined-auth read fix. It drives a HOT-KEY, IN-ORDER workload -- N sequential
// value transfers from one funded sender to one recipient, so every batch reads
// and writes the same sender and recipient accounts -- through the pipelined
// executor at the smallest batch size (one EVM tx per committer batch), with the
// production single-submitter ordering. Because submission is in nonce order and
// there is NO external (non-EVM) traffic, the authoritative pass must always read
// state consistent with what the committer validates against, so NOT ONE batch
// may MVCC-abort: node.CascadeCount() must stay 0 while real pipelining occurs
// (MaxInflightObserved >= 2, i.e. the executor overlaps warm(N+1) with auth(N)
// and does not serialize on commit).
//
// Scope: this is the end-to-end assertion that the SHIPPED pipeline does not
// self-abort on hot-key in-order traffic. It is NOT the deterministic reproduction
// of the stale-read bug: in a tight in-order chain the live in-flight write cache
// shadows the hot keys during the warm pass, so the async-commit-window timing
// that surfaces a stale reopened read is not forced in-process (verified: this
// test stays green even with the View.Reopen shared-cache bug reintroduced). The
// deterministic root-cause reproduction lives at the unit layer
// (endorser/query.TestViewReopenReflectsLatestCommittedForCachedKey, which is RED
// on that bug); the definitive scale proof is the ec2 replay (all batch sizes
// commit fully with 0 rollbacks). The fix makes auth reopen onto a FRESH committed
// view carrying no stale reads, so pipelined auth is read-identical to serial auth
// at any prefetch depth (see execution.AuthMergedBatch / ReopenableReadStore,
// query.View.Reopen).
func TestBatchPipelinedHotKeyNoStaleReadCascade(t *testing.T) {
	// Pipelined executor + single ordered submitter (the production clamp): warm(N+1)
	// overlaps auth(N), and dependent batches reach the orderer in nonce order.
	th, err := NewLocalTestHarness(t, TestLogger{T: t}, evmConfig(""), "", "fabric-x",
		map[string]any{"Gateway.Pipelined": true, "Gateway.SubmitterCount": 1})
	require.NoError(t, err)
	t.Cleanup(func() { th.Stop() })

	node := th.Gateways[0]
	node.SetMaxBatchSize(1) // one EVM tx per committer batch -> N txs == N pipelined batches
	ec, err := NewNativeEthClient(node)
	require.NoError(t, err)
	ctx := t.Context()

	_, recipient := newBatchSender(t)
	priv, addr := newBatchSender(t)
	primer, err := th.NewStatePrimer()
	require.NoError(t, err)
	require.NoError(t, primer.SetBalance(addr, big.NewInt(1_000_000_000)).Commit(ctx, true))

	const n = 40
	value := big.NewInt(1000)
	txs := make([]*types.Transaction, n)
	for i := range txs {
		txs[i] = signedValueTransfer(t, th.ethChainConfig, priv, uint64(i), recipient, value)
	}

	// Submit all in nonce order WITHOUT awaiting each commit, so the executor keeps
	// several batches in flight (the async commit window) -- the regime where a
	// stale-read auth would abort. Then wait for every tx to commit.
	for _, tx := range txs {
		require.NoErrorf(t, ec.SendTransaction(ctx, tx), "SendTransaction(nonce %d)", tx.Nonce())
	}
	for _, tx := range txs {
		waitForCommitT(t, ec, tx)
	}

	// The core invariant: no batch was MVCC-aborted, so the gateway never cascaded.
	require.Equal(t, 0, node.CascadeCount(),
		"in-order hot-key pipelined traffic must not produce a single stale-read abort")
	require.Eventually(t, func() bool { return node.InflightLen() == 0 },
		10*time.Second, 5*time.Millisecond, "in-flight registry must drain")

	// Real pipelining actually happened (otherwise the 0-cascade result is vacuous).
	require.GreaterOrEqualf(t, node.MaxInflightObserved(), 2,
		"executor must overlap batches (MaxInflightObserved=%d)", node.MaxInflightObserved())

	// Every transfer applied exactly once: all receipts successful, nonce advanced to
	// n, and the recipient received exactly n*value.
	for _, tx := range txs {
		receipt, err := ec.TransactionReceipt(ctx, tx.Hash())
		require.NoErrorf(t, err, "TransactionReceipt(%s)", tx.Hash())
		require.Equalf(t, types.ReceiptStatusSuccessful, receipt.Status, "tx %s status", tx.Hash())
	}
	finalNonce, err := ec.NonceAt(ctx, addr, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(n), finalNonce, "sender nonce must advance to n (every tx applied once)")

	recipientBal, err := ec.BalanceAt(ctx, recipient, nil)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(0).Mul(value, big.NewInt(n)), recipientBal, "recipient got exactly n*value")
}
