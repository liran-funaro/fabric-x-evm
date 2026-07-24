/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"sync"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// PendingPool holds accepted-but-not-committed transactions. It has no
// dependency graph and no ordering beyond insertion order: the drain-all
// executor takes the whole pool as one batch, and the two-phase authoritative
// pass + MVCC establish correctness. Entries stay pending until their batch
// commits (Remove) or, if excluded from a batch (nonce gap) or belonging to an
// aborted batch, until a later cycle picks them up again.
type PendingPool struct {
	mu    sync.Mutex
	order []ethcommon.Hash
	txs   map[ethcommon.Hash]*types.Transaction
}

func NewPendingPool() *PendingPool {
	return &PendingPool{txs: make(map[ethcommon.Hash]*types.Transaction)}
}

func (p *PendingPool) Add(tx *types.Transaction) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := tx.Hash()
	if _, ok := p.txs[h]; ok {
		return
	}
	p.txs[h] = tx
	p.order = append(p.order, h)
}

func (p *PendingPool) Has(hash ethcommon.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.txs[hash]
	return ok
}

func (p *PendingPool) DrainAll() []*types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.order) == 0 {
		return nil
	}
	out := make([]*types.Transaction, 0, len(p.order))
	for _, h := range p.order {
		out = append(out, p.txs[h])
	}
	return out
}

func (p *PendingPool) Remove(hashes []ethcommon.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range hashes {
		delete(p.txs, h)
	}
	kept := p.order[:0]
	for _, h := range p.order {
		if _, ok := p.txs[h]; ok {
			kept = append(kept, h)
		}
	}
	p.order = kept
}

func (p *PendingPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.txs)
}
