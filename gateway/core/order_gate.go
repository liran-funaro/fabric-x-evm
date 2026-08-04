/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import "sync"

// OrderGate enforces depth-1 ordered submission: the submit worker Arms the
// gate with a committer TxID before broadcasting, then waits on the returned
// channel until that TxID is Observed in a delivered ordered block. Single-armed
// (the pipelined submitter is one serialized worker); Observe runs concurrently
// from the ordered-delivery drain goroutine.
type OrderGate struct {
	mu      sync.Mutex
	armedID string
	armedCh chan struct{}
}

// NewOrderGate returns an unarmed gate.
func NewOrderGate() *OrderGate { return &OrderGate{} }

// Arm sets the single armed slot to txID and returns a channel that is closed
// when txID is Observed. It overwrites any prior arm (the single serialized
// worker never arms twice concurrently, so this only matters after a Disarm).
func (g *OrderGate) Arm(txID string) <-chan struct{} {
	ch := make(chan struct{})
	g.mu.Lock()
	g.armedID = txID
	g.armedCh = ch
	g.mu.Unlock()
	return ch
}

// Disarm clears the armed slot without signaling. Used on the
// Submit-error/timeout/ctx path so a tx that will never be Observed does not
// leave a stale arm that a later Observe would spuriously close.
func (g *OrderGate) Disarm() {
	g.mu.Lock()
	g.armedID = ""
	g.armedCh = nil
	g.mu.Unlock()
}

// Observe closes the armed channel and clears the slot if any of txIDs equals
// the armed TxID. Non-matching IDs and observes-while-unarmed are no-ops; a
// matching TxID observed twice is idempotent (the slot is cleared on the first
// match, so the second finds nothing armed).
func (g *OrderGate) Observe(txIDs []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.armedCh == nil {
		return
	}
	for _, id := range txIDs {
		if id == g.armedID {
			close(g.armedCh)
			g.armedID = ""
			g.armedCh = nil
			return
		}
	}
}
