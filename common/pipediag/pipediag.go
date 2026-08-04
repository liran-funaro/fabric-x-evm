/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

// Package pipediag is REMOVABLE diagnostic instrumentation (default OFF, gated by
// EVM_PIPE_DIAG) for the pipelined warm/auth executor's residual stale-read
// livelock. It answers exactly one question: when the pipelined authoritative
// pass records a read version the committer then rejects (an MVCC abort), WHERE
// did that stale version come from?
//
//	H1  query-service visibility lag: warm cold-fetched the key from the query
//	    service and got a version OLDER than a commit the gateway had ALREADY
//	    acknowledged. The QS trails the commit-notification stream. => a per-key
//	    visibility-gated eviction is the right fix.
//	H2  stale clone across a boundary: warm cold-fetched the key when its value
//	    was current, the reopened auth view's clone carried that value forward,
//	    and the key's committed version advanced in between. => the clone/inherit
//	    layer is the culprit, not the query service.
//	H3  write-cache / inheritance: the stale version never came from a QS fetch
//	    at all -- the key was served from the in-flight write cache or an inherited
//	    write-cache resolution. => a write-cache/eviction logic bug.
//
// It records three cheap facts (max acked commit version per key; whether a key
// was ever cold-fetched; whether a cold fetch ever trailed an acked commit) and
// classifies at abort time. When disabled every entry point is a single boolean
// load that returns immediately, so the serial path and clean pipelined runs are
// unperturbed and the livelock's timing (hence its incidence) is preserved. This
// is NOT production code: delete it and its call sites once the residual source
// is classified.
package pipediag

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// Enabled gates every entry point. Read ONCE from EVM_PIPE_DIAG at process start
// (the perf rig sets it in the child env before the in-process endorser/gateway
// run, so this package-init read observes it).
var Enabled = os.Getenv("EVM_PIPE_DIAG") != ""

var (
	mu        sync.RWMutex
	commitLog = map[string]uint64{}   // raw key -> max committed version the gateway has acked
	coldKeys  = map[string]struct{}{} // raw key -> was cold-fetched from the query service at least once
	qslagKeys = map[string]struct{}{} // raw key -> a cold fetch returned a version below an already-acked commit (H1)

	qslagTotal atomic.Int64 // real-time QS-lag detections (see RecordColdFetch)
	abortTotal atomic.Int64 // ClassifyAbort calls (aborted batches inspected)
)

// logCap bounds the per-event detail lines (PIPE-DIAG-QSLAG, PIPE-DIAG-CONFLICT)
// so a heavy-lag run cannot flood stderr enough to slow warm and perturb the
// livelock being measured. The decisive aggregates -- qslagTotal and the
// per-abort PIPE-DIAG-ABORT summary -- are always emitted regardless of the cap.
const logCap = 50

// RecordCommit notes that the gateway has acknowledged key as committed at
// version (the batch's spec version, which equals the committed version for a
// committed batch). Called on the commit path per written key; commitLog keeps
// the max, since per-key committed versions advance monotonically.
func RecordCommit(key string, version uint64) {
	if !Enabled {
		return
	}
	mu.Lock()
	if version >= commitLog[key] {
		commitLog[key] = version
	}
	mu.Unlock()
}

// RecordColdFetch notes that a query-service GetRows returned key at version
// fetched (a cold fetch: the read missed the per-view cache). It marks the key
// as QS-sourced and, if the gateway has ALREADY acknowledged a HIGHER committed
// version for this key, records a real-time query-service visibility-lag (H1)
// event -- the QS served a version older than a commit we had already seen.
// Call ONLY for a present key (a real version); absent keys (version 0) would
// spuriously trip the lag check.
//
// Caveat (documented, not corrected): the acked commit could have landed in the
// narrow window between GetRows returning and this lookup, in which case the QS
// was actually current at fetch time -- a false positive. The check is therefore
// conservative for CONFIRMATION (a nonzero, clustered count is strong H1
// evidence) and clean for REFUTATION (a zero count across captured livelocks
// rules H1 out, because the race window only ADDS false positives).
func RecordColdFetch(key string, fetched uint64) {
	if !Enabled {
		return
	}
	mu.Lock()
	coldKeys[key] = struct{}{}
	acked, ok := commitLog[key]
	lag := ok && acked > fetched
	if lag {
		qslagKeys[key] = struct{}{}
	}
	mu.Unlock()
	if lag {
		n := qslagTotal.Add(1)
		if n <= logCap {
			fmt.Fprintf(os.Stderr, "PIPE-DIAG-QSLAG key=%q qs_version=%d acked_committed=%d gap=%d\n",
				key, fetched, acked, acked-fetched)
		}
	}
}

// AbortClass tallies, for one aborted batch, how each of its recorded reads
// compares to the acked committed version for that key -- the DIRECTION of the
// mismatch, which discriminates the two surviving residual hypotheses:
//
//	Under  recorded read version BELOW the acked committed version -- a classic
//	       STALE read (auth read an old value; the committer advanced past it).
//	       This is the H2-stale-clone / H1-qs-lag failure mode. Sub-attributed
//	       into H1/H2/H3 by provenance.
//	Over   recorded read version ABOVE the acked committed version -- a
//	       SPECULATIVE over-read. The query view never returns a version above
//	       committed, so an over-read can only be a write-cache spec version
//	       (readBase+chain_position, a prediction of a future commit) that never
//	       committed at that number -- the chain-misprediction failure mode.
//	Equal  recorded read version EQUALS the acked committed version (auth read
//	       exactly what committed for that key -- not the conflicting key).
//	NotInLog key never recorded as committed by this gateway (no ground truth).
//
// Under is the only bucket whose reliability depends on commitLog being current
// at classify time (the winning commit is almost always notified before the
// losing abort -- lower TxNum / earlier block -- so RecordCommit runs first, but
// a residual lag can only UNDER-count Under, never inflate it). Over and NotInLog
// are ordering-race-immune. H1/H2/H3 sub-attribute Under only.
type AbortClass struct {
	Reads, NotInLog, Under, Equal, Over, H1, H2, H3 int
}

// ClassifyAbort inspects the read-set of an MVCC-aborted batch, buckets each read
// by the direction of its version mismatch against the acked committed version,
// logs per-key detail for the interesting (Under/Over) cases and a per-abort
// summary, and returns the tally. reads maps raw key -> recorded read version.
// Call on the abort path for the EARLIEST aborted batch only (the one the
// committer actually rejected; later cascade victims read against its
// speculative writes and would misattribute). Returns a zero AbortClass when
// disabled.
func ClassifyAbort(txID string, reads map[string]uint64) AbortClass {
	if !Enabled {
		return AbortClass{}
	}
	nth := abortTotal.Add(1)
	detail := nth <= logCap // per-key detail only for the first logCap aborts
	mu.RLock()
	defer mu.RUnlock()
	r := AbortClass{Reads: len(reads)}
	for k, vread := range reads {
		acked, ok := commitLog[k]
		switch {
		case !ok:
			r.NotInLog++
		case acked > vread: // stale under-read
			r.Under++
			var class string
			switch {
			case has(qslagKeys, k):
				r.H1++
				class = "H1-qs-lag"
			case has(coldKeys, k):
				r.H2++
				class = "H2-stale-clone"
			default:
				r.H3++
				class = "H3-write-cache"
			}
			if detail {
				fmt.Fprintf(os.Stderr, "PIPE-DIAG-CONFLICT tx=%s key=%q dir=under v_read=%d v_committed=%d class=%s\n",
					txID, k, vread, acked, class)
			}
		case acked == vread:
			r.Equal++
		default: // acked < vread: speculative over-read (write-cache future spec version)
			r.Over++
			if detail {
				fmt.Fprintf(os.Stderr, "PIPE-DIAG-CONFLICT tx=%s key=%q dir=over v_read=%d v_committed=%d\n",
					txID, k, vread, acked)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "PIPE-DIAG-ABORT tx=%s reads=%d notinlog=%d under=%d equal=%d over=%d H1=%d H2=%d H3=%d qslag_total=%d abort_total=%d\n",
		txID, r.Reads, r.NotInLog, r.Under, r.Equal, r.Over, r.H1, r.H2, r.H3, qslagTotal.Load(), abortTotal.Load())
	return r
}

func has(m map[string]struct{}, k string) bool {
	_, ok := m[k]
	return ok
}
