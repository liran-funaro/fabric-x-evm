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
	mu       sync.Mutex
	order    []ethcommon.Hash
	txs      map[ethcommon.Hash]*types.Transaction
	reserved map[ethcommon.Hash]struct{} // drained-but-not-yet-resolved (pipelined executor only)
}

func NewPendingPool() *PendingPool {
	return &PendingPool{
		txs:      make(map[ethcommon.Hash]*types.Transaction),
		reserved: make(map[ethcommon.Hash]struct{}),
	}
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

// Get returns the pending transaction for hash, if present. Used by
// TransactionByHash to serve a not-yet-committed tx.
func (p *PendingPool) Get(hash ethcommon.Hash) (*types.Transaction, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tx, ok := p.txs[hash]
	return tx, ok
}

// DrainAll returns every pending tx in insertion order (non-destructive: the
// caller Removes committed txs later). Equivalent to DrainUpTo(0).
func (p *PendingPool) DrainAll() []*types.Transaction {
	return p.DrainUpTo(0)
}

// DrainUpTo returns up to max pending txs in insertion (FIFO) order, or all of
// them when max <= 0. It is non-destructive -- entries stay in the pool until
// Remove -- so the executor can bound the size of a single merged batch (a
// Fabric committer tx has a hard max message size, and a burst that outpaces
// the drain cycle would otherwise be swallowed into one oversized batch)
// without losing the remainder: the leftover txs are simply picked up by the
// next drain cycle, which runs immediately after the current batch commits.
func (p *PendingPool) DrainUpTo(max int) []*types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.order)
	if n == 0 {
		return nil
	}
	if max > 0 && max < n {
		n = max
	}
	out := make([]*types.Transaction, 0, n)
	for _, h := range p.order[:n] {
		out = append(out, p.txs[h])
	}
	return out
}

// DrainUpToReserved returns up to max pending txs in insertion (FIFO) order,
// SKIPPING any already reserved, and marks the returned hashes reserved before
// returning them. It is the pipelined executor's drain (runExecutorPipelined):
// it lets the executor hold batch N reserved (drained-but-not-yet-resolved)
// while it drains and warms batch N+1, so the non-destructive pool never
// re-draws an in-flight batch. max <= 0 means "all unreserved". The serial path
// uses DrainUpTo, which does not reserve, so it is unaffected.
func (p *PendingPool) DrainUpToReserved(max int) []*types.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*types.Transaction, 0, len(p.order))
	for _, h := range p.order {
		if _, isReserved := p.reserved[h]; isReserved {
			continue
		}
		tx, ok := p.txs[h]
		if !ok {
			continue
		}
		out = append(out, tx)
		p.reserved[h] = struct{}{}
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

// Release clears the reservation for the given hashes that remain in pending
// (e.g. a batch whose results were handled: included/terminal txs are Remove()d,
// the rest are Released so a later drain can re-draw them). Releasing an unknown
// or already-removed hash is a no-op.
func (p *PendingPool) Release(hashes []ethcommon.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range hashes {
		delete(p.reserved, h)
	}
}

func (p *PendingPool) Remove(hashes []ethcommon.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range hashes {
		delete(p.txs, h)
		delete(p.reserved, h)
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
