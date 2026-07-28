/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/notification"
)

// txNotifier resolves each submitted-but-unconfirmed committer batch by its real
// per-TxID commit outcome instead of a blind timeout rollback. It implements
// notification.TxStatusHandler, so it can be plugged into a
// notification.Processor fed by the Fabric-X sidecar's per-TxID notification
// stream (see Gateway.SetNotifier and the notification harness wiring).
//
// Lifecycle of one TxID:
//   - Watch(txID) arms a client-side backstop timer and pushes the TxID onto the
//     subscribe channel the notification Subscribe loop reads (register-then-submit:
//     the caller submits the committer tx only after Watch returns).
//   - Handle receives the sidecar's verdict: a COMMITTED / abort status resolves
//     directly; a STATUS_UNSPECIFIED status (the sidecar's own commit-timeout
//     signal) defers to the query-service fallback.
//   - onTimeout is the client-side backstop for a dead stream (no event ever
//     arrives): it also defers to the fallback.
//
// The mu-guarded check-and-delete of timers[txID] is the exactly-once gate:
// whichever of Handle / onTimeout removes the entry does the resolve; the other
// finds it gone and no-ops. (resolve is idempotent and fallback is a read-only
// query, so a rare double call would be harmless anyway -- the gate just avoids
// a wasted query.)
type txNotifier struct {
	subscribe chan<- []string
	timeout   time.Duration
	fallback  func(txID string) bool // returns committed?
	resolve   func(txID string, committed bool)

	mu     sync.Mutex
	timers map[string]*time.Timer
}

// newTxNotifier builds a txNotifier that registers TxIDs on subscribe, arms a
// client-side backstop of timeout per TxID, adjudicates sidecar-timeouts via
// fallback, and reports every outcome through resolve (the gateway wires
// resolve = resolveInflight and fallback = queryCommitStatus).
func newTxNotifier(subscribe chan<- []string, timeout time.Duration,
	fallback func(txID string) bool, resolve func(txID string, committed bool)) *txNotifier {
	return &txNotifier{
		subscribe: subscribe,
		timeout:   timeout,
		fallback:  fallback,
		resolve:   resolve,
		timers:    make(map[string]*time.Timer),
	}
}

// Watch registers txID for commit notification and arms its client-side backstop
// timer. Idempotent: a second Watch for a TxID already being tracked is a no-op.
// The timer is armed BEFORE the subscribe send so it exists before any event can
// be handled; the subscribe send then registers the TxID with the notification
// stream. The send is blocking by design (register-then-submit): the caller must
// provide a subscribe channel with enough buffering / a live drain that this send
// does not wedge the executor goroutine.
func (n *txNotifier) Watch(txID string) {
	n.mu.Lock()
	if _, ok := n.timers[txID]; ok {
		n.mu.Unlock()
		return // already watching -- idempotent
	}
	n.timers[txID] = time.AfterFunc(n.timeout, func() { n.onTimeout(txID) })
	n.mu.Unlock()

	n.subscribe <- []string{txID} // register (after arming)
}

// Handle implements notification.TxStatusHandler. For each event it claims the
// TxID's timer under the exactly-once gate (skipping already-resolved IDs), then
// resolves: a STATUS_UNSPECIFIED status is the sidecar's own commit-timeout
// signal and defers to the query-service fallback; any other status resolves
// directly (committed iff COMMITTED, via TxStatusEvent.Valid). Never returns an
// error (handler errors are non-fatal to the stream anyway).
func (n *txNotifier) Handle(_ context.Context, events []notification.TxStatusEvent) error {
	for _, ev := range events {
		n.mu.Lock()
		timer, ok := n.timers[ev.TxID]
		if !ok {
			n.mu.Unlock()
			continue // already resolved (timer/other event won) -- idempotent
		}
		timer.Stop()
		delete(n.timers, ev.TxID)
		n.mu.Unlock()

		if ev.Status == committerpb.Status_STATUS_UNSPECIFIED {
			n.resolve(ev.TxID, n.fallback(ev.TxID)) // sidecar timeout -> query fallback
		} else {
			n.resolve(ev.TxID, ev.Valid())
		}
	}
	return nil
}

// onTimeout is the client-side backstop for a dead stream: if no event for txID
// ever arrives within the timeout, adjudicate via the fallback. It shares the
// exactly-once gate with Handle: if Handle already claimed the TxID, this finds
// the timer gone and no-ops.
func (n *txNotifier) onTimeout(txID string) {
	n.mu.Lock()
	if _, ok := n.timers[txID]; !ok {
		n.mu.Unlock()
		return // Handle already won -- idempotent
	}
	delete(n.timers, txID)
	n.mu.Unlock()

	n.resolve(txID, n.fallback(txID))
}
