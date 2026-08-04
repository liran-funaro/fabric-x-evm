/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import "context"

// inflightLimiter bounds how many submitted-but-not-yet-committed EVM txs the
// replay feeder may have outstanding, turning the fire-everything feeder into a
// closed loop that self-paces to whatever the stack can actually sustain.
//
// The replay deliberately has no flow control (see the -outstanding removal note
// on the flags): for a fixed 50k window the pending pool is bounded by the window
// itself, so firing everything is safe and measures the true ceiling. Over hours,
// though, the feeder outruns the drain rate without bound and exhausts the host's
// memory -- so a multi-hour run needs a cap, and only a multi-hour run does.
//
// A nil *inflightLimiter is the disabled case. Every method tolerates a nil
// receiver, so the feeder needs no branch and the default (unbounded) path stays
// byte-for-byte what it was.
type inflightLimiter struct {
	// slots holds one token per in-flight tx; its capacity is the bound. Acquire
	// adds a token and Release removes one. A buffered channel rather than a
	// sync.Cond because Acquire must be cancellable -- Cond.Wait cannot select on
	// a context, and a feeder blocked forever on a stalled stack would deadlock
	// the harness past its own stall detection.
	slots chan struct{}
}

// newInflightLimiter returns a limiter bounding in-flight txs to maxInflight, or
// nil (disabled) when maxInflight is not positive.
func newInflightLimiter(maxInflight int) *inflightLimiter {
	if maxInflight <= 0 {
		return nil
	}
	return &inflightLimiter{slots: make(chan struct{}, maxInflight)}
}

// Acquire blocks until the in-flight count is below the bound, then reserves a
// slot. It returns ctx.Err() if ctx ends first, leaving no slot reserved.
func (l *inflightLimiter) Acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns n slots. Releasing more slots than are held is ignored rather
// than raising the bound: a rolled-back batch's EVM txs stay outstanding and are
// credited only when a later batch commits them, so double-crediting them would
// silently defeat the cap.
func (l *inflightLimiter) Release(n int) {
	if l == nil {
		return
	}
	for range n {
		select {
		case <-l.slots:
		default:
			return
		}
	}
}
