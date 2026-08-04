/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package pipediag

import "testing"

// reset clears the package-level diagnostic state so each subtest starts clean.
// White-box (same package) so it can touch the unexported maps directly.
func reset(enabled bool) {
	mu.Lock()
	commitLog = map[string]uint64{}
	coldKeys = map[string]struct{}{}
	qslagKeys = map[string]struct{}{}
	mu.Unlock()
	qslagTotal.Store(0)
	abortTotal.Store(0)
	Enabled = enabled
}

// TestClassifyAbort_BucketsByDirectionAndAttributesUnder proves the classifier
// buckets each read by the DIRECTION of its version mismatch (the whole point of
// the v2 diagnostic) and sub-attributes the Under bucket to the right hypothesis:
//
//	Under H1  a cold fetch returned a version BELOW an already-acked commit (QS
//	          lag): commit recorded first, then the cold fetch trails it.
//	Under H2  cold-fetched while current, committed higher only afterwards (stale
//	          clone): fetch first (no acked commit yet), then the commit.
//	Under H3  never cold-fetched -- only ever committed (write-cache/inheritance).
//	Over      recorded read ABOVE committed -- a speculative write-cache version.
//	Equal     recorded read EQUALS committed -- not the conflicting key.
//	NotInLog  key never recorded committed by this gateway.
func TestClassifyAbort_BucketsByDirectionAndAttributesUnder(t *testing.T) {
	reset(true)

	// Under/H1: acked commit at 5, then a cold fetch returns the stale 3.
	RecordCommit("h1", 5)
	RecordColdFetch("h1", 3)

	// Under/H2: cold fetch at 5 while current (no acked commit yet), commit to 6.
	RecordColdFetch("h2", 5)
	RecordCommit("h2", 6)

	// Under/H3: only ever committed, never cold-fetched.
	RecordCommit("h3", 6)

	// Over: committed 5, auth recorded a speculative future 7.
	RecordCommit("over", 5)

	// Equal: read equals committed.
	RecordCommit("eq", 6)

	// NotInLog "nl": never committed -- absent from commitLog.

	reads := map[string]uint64{
		"h1":   3, // under (committed 5) -> H1
		"h2":   5, // under (committed 6) -> H2
		"h3":   5, // under (committed 6) -> H3
		"over": 7, // over  (committed 5)
		"eq":   6, // equal (committed 6)
		"nl":   4, // not in commitLog
	}
	got := ClassifyAbort("tx1", reads)
	want := AbortClass{Reads: 6, NotInLog: 1, Under: 3, Equal: 1, Over: 1, H1: 1, H2: 1, H3: 1}
	if got != want {
		t.Fatalf("ClassifyAbort = %+v, want %+v", got, want)
	}
	if qslagTotal.Load() != 1 {
		t.Fatalf("qslagTotal = %d, want 1 (only h1's cold fetch trailed an acked commit)", qslagTotal.Load())
	}
}

// TestDisabledIsNoOp proves that with EVM_PIPE_DIAG off every entry point is
// inert: no state accumulates and ClassifyAbort returns a zero tally, so the
// serial path and clean pipelined runs are unperturbed.
func TestDisabledIsNoOp(t *testing.T) {
	reset(false)

	RecordCommit("k", 5)
	RecordColdFetch("k", 3)
	got := ClassifyAbort("tx", map[string]uint64{"k": 3})

	if got != (AbortClass{}) {
		t.Fatalf("disabled ClassifyAbort = %+v, want zero", got)
	}
	mu.RLock()
	defer mu.RUnlock()
	if len(commitLog) != 0 || len(coldKeys) != 0 || len(qslagKeys) != 0 {
		t.Fatalf("disabled recorded state: commitLog=%d coldKeys=%d qslagKeys=%d, want all 0",
			len(commitLog), len(coldKeys), len(qslagKeys))
	}
}
