/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// emptyReader is a trivial in-memory ReadStore where every key is absent -- the
// exact committed state a batch of fresh, nonce-0 senders reads against. Using it
// (instead of storage.LightKVS, which would form an import cycle: storage ->
// execution) keeps these benchmarks pure-CPU: they measure the execution
// machinery with zero backing-store latency, isolating whether the code itself
// (not the query service) can sustain throughput.
type emptyReader struct{}

func (emptyReader) Get(string, string) (*blocks.WriteRecord, error) { return nil, nil }
func (emptyReader) Close() error                                    { return nil }

type emptySnapshotter struct{}

func (emptySnapshotter) NewSnapshot(uint64) (ReadStore, error) { return emptyReader{}, nil }

// benchTransfer signs a nonce-0, value-0 legacy transfer from a fresh sender, so
// a batch of them all pass the ledger-nonce check against fresh state and execute.
func benchTransfer(b *testing.B, cfg *params.ChainConfig, key *ecdsa.PrivateKey) *types.Transaction {
	to := ethcommon.HexToAddress("0xdead")
	signer := types.MakeSigner(cfg, big.NewInt(0), 1_000_000)
	tx, err := types.SignNewTx(key, signer, &types.LegacyTx{Nonce: 0, Gas: 21_000, To: &to, Value: big.NewInt(0)})
	if err != nil {
		b.Fatal(err)
	}
	// Warm the sender cache so the benchmark measures execution, not one-off ECDSA
	// recovery (geth caches tx.from; real fresh txs pay recovery once too).
	if _, err := types.Sender(signer, tx); err != nil {
		b.Fatal(err)
	}
	return tx
}

func benchBatch(b *testing.B, cfg *params.ChainConfig, n int) []*types.Transaction {
	txs := make([]*types.Transaction, n)
	for i := range txs {
		k, err := crypto.GenerateKey()
		if err != nil {
			b.Fatal(err)
		}
		txs[i] = benchTransfer(b, cfg, k)
	}
	return txs
}

// perTxMetric reports throughput in tx/s given n txs processed per iteration.
func perTxMetric(b *testing.B, nPerIter int) {
	b.ReportMetric(float64(nPerIter*b.N)/b.Elapsed().Seconds(), "tx/s")
}

// BenchmarkExecuteBatch_128 measures the two-phase (warm + authoritative) execution
// of a 128-tx batch over an in-memory KVS -- the dominant per-cycle cost. Value
// transfers isolate the framework overhead (snapshot, StateDB, EVM apply x2 passes,
// overlay, merge); real ERC-20 calls add EVM compute on top.
func BenchmarkExecuteBatch_128(b *testing.B) {
	kvs := emptySnapshotter{}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	eng := NewEVMEngine("evm", kvs, cfg, true)
	txs := benchBatch(b, cfg.ChainConfig, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := eng.ExecuteBatch(context.Background(), txs); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	perTxMetric(b, 128)
}

// BenchmarkExecute_1 is the single-tx path (Execute), for per-tx comparison.
func BenchmarkExecute_1(b *testing.B) {
	kvs := emptySnapshotter{}
	cfg := EVMConfig{ChainConfig: common.BuildChainConfig(4011)}
	eng := NewEVMEngine("evm", kvs, cfg, true)
	k, _ := crypto.GenerateKey()
	tx := benchTransfer(b, cfg.ChainConfig, k)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := eng.Execute(context.Background(), tx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	perTxMetric(b, 1)
}

// BenchmarkMergeResults_128 measures folding 128 per-tx results (each ~2 reads +
// 2 writes, distinct keys) into one merged read/write-set.
func BenchmarkMergeResults_128(b *testing.B) {
	results := make([]endorsement.ExecutionResult, 128)
	for i := range results {
		results[i] = endorsement.ExecutionResult{RWS: blocks.ReadWriteSet{
			Reads: []blocks.KVRead{
				{Key: fmt.Sprintf("acct/%d/nonce", i), Version: &blocks.Version{BlockNum: 1}},
				{Key: fmt.Sprintf("acct/%d/bal", i), Version: &blocks.Version{BlockNum: 1}},
			},
			Writes: []blocks.KVWrite{
				{Key: fmt.Sprintf("acct/%d/nonce", i), Value: []byte{1}},
				{Key: fmt.Sprintf("acct/%d/bal", i), Value: []byte{2}},
			},
		}}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = MergeResults(results)
	}
	b.StopTimer()
	perTxMetric(b, 128)
}

// BenchmarkStateDB_TxOps measures a realistic per-tx read/write mix against a
// FRESH StateDB each iteration (as newState builds per sub-tx): a few reads then
// a few writes, so the journal stays small -- not the O(N²) artifact of reusing
// one StateDB across a huge loop.
func BenchmarkStateDB_TxOps(b *testing.B) {
	reader := emptyReader{}
	addr := ethcommon.HexToAddress("0xbeef")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := NewStateDB(context.Background(), reader, "evm", 0, true)
		if err != nil {
			b.Fatal(err)
		}
		// ~transfer-shaped: read nonce+2 balances, write nonce+2 balances.
		_ = s.GetNonce(addr)
		_ = s.GetState(addr, ethcommon.HexToHash("0x1"))
		_ = s.GetState(addr, ethcommon.HexToHash("0x2"))
		s.SetNonce(addr, 1, 0)
		s.SetState(addr, ethcommon.HexToHash("0x1"), ethcommon.HexToHash("0xa"))
		s.SetState(addr, ethcommon.HexToHash("0x2"), ethcommon.HexToHash("0xb"))
	}
	b.StopTimer()
	perTxMetric(b, 1) // 1 "tx-shaped" op set per iteration
}
