package core

import (
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func txWithNonce(n uint64) *types.Transaction {
	return types.NewTx(&types.LegacyTx{Nonce: n, Gas: 21000})
}

func TestPendingPoolAddDrainRemove(t *testing.T) {
	p := NewPendingPool()
	a, b := txWithNonce(1), txWithNonce(2)
	p.Add(a)
	p.Add(b)
	p.Add(a) // dedup by hash
	if p.Len() != 2 {
		t.Fatalf("len = %d, want 2", p.Len())
	}
	drained := p.DrainAll()
	if len(drained) != 2 {
		t.Fatalf("drained %d, want 2", len(drained))
	}
	// DrainAll leaves entries pending until Remove.
	if p.Len() != 2 {
		t.Fatalf("after drain len = %d, want 2 (still pending)", p.Len())
	}
	p.Remove([]ethcommon.Hash{a.Hash()})
	if p.Len() != 1 || !p.Has(b.Hash()) || p.Has(a.Hash()) {
		t.Fatalf("after remove: len=%d hasB=%v hasA=%v", p.Len(), p.Has(b.Hash()), p.Has(a.Hash()))
	}
}
