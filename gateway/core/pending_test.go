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

func TestPendingPoolDrainUpTo(t *testing.T) {
	p := NewPendingPool()
	txs := make([]*types.Transaction, 5)
	for i := range txs {
		txs[i] = txWithNonce(uint64(i))
		p.Add(txs[i])
	}

	// Cap smaller than the pool: exactly max txs, in insertion (FIFO) order.
	got := p.DrainUpTo(3)
	if len(got) != 3 {
		t.Fatalf("DrainUpTo(3) returned %d, want 3", len(got))
	}
	for i, tx := range got {
		if tx.Hash() != txs[i].Hash() {
			t.Fatalf("DrainUpTo(3)[%d] = nonce %d, want insertion order nonce %d", i, tx.Nonce(), i)
		}
	}
	// Non-destructive: all 5 remain pending for the next cycle.
	if p.Len() != 5 {
		t.Fatalf("after DrainUpTo len = %d, want 5 (non-destructive)", p.Len())
	}

	// Cap >= pool size, and the <= 0 (unbounded) cases, all return everything.
	for _, max := range []int{5, 10, 0, -1} {
		if got := p.DrainUpTo(max); len(got) != 5 {
			t.Fatalf("DrainUpTo(%d) returned %d, want all 5", max, len(got))
		}
	}

	// After removing the first 3, DrainUpTo(3) yields the remaining 2 in order.
	p.Remove([]ethcommon.Hash{txs[0].Hash(), txs[1].Hash(), txs[2].Hash()})
	got = p.DrainUpTo(3)
	if len(got) != 2 || got[0].Hash() != txs[3].Hash() || got[1].Hash() != txs[4].Hash() {
		t.Fatalf("after remove, DrainUpTo(3) = %d txs, want the 2 survivors in order", len(got))
	}
}
