/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"math/big"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func benchTxs(b *testing.B, n int) []*types.Transaction {
	signer := types.LatestSignerForChainID(big.NewInt(4011))
	to := ethcommon.HexToAddress("0xdead")
	txs := make([]*types.Transaction, n)
	for i := range txs {
		k, err := crypto.GenerateKey()
		if err != nil {
			b.Fatal(err)
		}
		tx, err := types.SignNewTx(k, signer, &types.LegacyTx{Nonce: 0, Gas: 21_000, To: &to, Value: big.NewInt(0)})
		if err != nil {
			b.Fatal(err)
		}
		txs[i] = tx
	}
	return txs
}

// BenchmarkPendingPool_Cycle128 measures one executor-cycle's worth of pool ops:
// add 128 txs, drain them, remove them (the per-cycle churn on the hot path).
func BenchmarkPendingPool_Cycle128(b *testing.B) {
	txs := benchTxs(b, 128)
	hashes := make([]ethcommon.Hash, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewPendingPool()
		for _, tx := range txs {
			p.Add(tx)
		}
		_ = p.DrainUpTo(128)
		p.Remove(hashes)
	}
	b.StopTimer()
	b.ReportMetric(float64(128*b.N)/b.Elapsed().Seconds(), "tx/s")
}

// BenchmarkTxMarshal_128 measures encoding a 128-tx batch into the committer-tx
// Args (tx.MarshalBinary per sub-tx), done once per batch in EndorsementClient.ExecuteBatch.
func BenchmarkTxMarshal_128(b *testing.B) {
	txs := benchTxs(b, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tx := range txs {
			if _, err := tx.MarshalBinary(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(128*b.N)/b.Elapsed().Seconds(), "tx/s")
}
