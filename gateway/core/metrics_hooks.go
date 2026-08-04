/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"time"

	"github.com/hyperledger/fabric-x-evm/common/pipediag"
)

// This file collects the optional, nil-guarded metric hooks for the two-phase
// batch executor. They follow the same pattern as SetBatchSubmitterQueueSizeMetric
// (batch_submitter.go): package-level func vars that default to nil, so core has
// ZERO metrics overhead and no Prometheus dependency in production. The perf test
// (integration/perf) assigns Prometheus recorders to them; everyone else leaves
// them nil and the guarded calls are skipped.

// RecordWarmPhaseDuration, when non-nil, is called with the wall-clock duration
// of each concurrent WARM pass. Only the pipelined executor has a separate warm
// pass; the serial executor never calls this. It may fire from a background warm
// goroutine, so the recorder must be safe for concurrent use (Prometheus
// histograms are).
var RecordWarmPhaseDuration func(d time.Duration)

// RecordAuthPhaseDuration, when non-nil, is called with the wall-clock duration
// of each AUTHORITATIVE pass: AuthBatch in the pipelined executor, and the
// combined ExecuteBatch in the serial executor (where the whole pass is
// authoritative -- there is no overlap to hide behind). This is the serial CPU
// floor both executors pay per batch.
var RecordAuthPhaseDuration func(d time.Duration)

// RecordCommitLatency, when non-nil, is called once per committed batch with the
// submit->commit-notification wall time (the same sample fed to CommitLatencyStats).
// Only the committed path fires; a rollback/timeout is not a commit latency. It is
// called from the commit-notification path, so the recorder must be safe for
// concurrent use.
var RecordCommitLatency func(d time.Duration)

// RecordSpecAbort, when non-nil, is called exactly once per MVCC rollback cascade
// with a short class label for the earliest aborted batch (the one the committer
// actually rejected). When pipediag is enabled the label is its dominant
// stale-read bucket (H1-qs-lag / H2-stale-clone / H3-write-cache / over-read);
// otherwise it is "unclassified" -- the cascade still counts, just without the
// diagnostic attribution.
var RecordSpecAbort func(class string)

// specAbortClass maps a pipediag.AbortClass tally to the single dominant
// stale-read label for the spec-abort counter. H1/H2/H3 sub-attribute the
// stale UNDER-reads (query-service lag, stale view clone, write cache); an
// over-read is a chain misprediction. A zero tally (pipediag disabled, or no
// classifiable read) is "unclassified".
func specAbortClass(ac pipediag.AbortClass) string {
	switch {
	case ac.H1 >= ac.H2 && ac.H1 >= ac.H3 && ac.H1 > 0:
		return "H1-qs-lag"
	case ac.H2 >= ac.H3 && ac.H2 > 0:
		return "H2-stale-clone"
	case ac.H3 > 0:
		return "H3-write-cache"
	case ac.Over > 0:
		return "over-read"
	default:
		return "unclassified"
	}
}
