/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// MergeResults folds an ordered batch of per-tx execution results into one
// read/write-set plus the per-tx event blobs (index = sub-index).
//
// Reads are unioned by key, keeping the first-seen version: because the whole
// batch executes under one query-service view (+ overlay in later stages), the
// first read of a key already carries the view-consistent committed version, so
// every tx that read it depends on the same version. Writes are applied in
// batch order with last-write-wins, matching the serial authoritative pass.
func MergeResults(results []endorsement.ExecutionResult) (blocks.ReadWriteSet, [][]byte) {
	var merged blocks.ReadWriteSet
	readIdx := make(map[string]int)  // key -> index in merged.Reads
	writeIdx := make(map[string]int) // key -> index in merged.Writes
	events := make([][]byte, len(results))

	for i, res := range results {
		events[i] = res.Event
		for _, rd := range res.RWS.Reads {
			if _, ok := readIdx[rd.Key]; ok {
				continue // union: keep first-seen version
			}
			readIdx[rd.Key] = len(merged.Reads)
			merged.Reads = append(merged.Reads, rd)
		}
		for _, w := range res.RWS.Writes {
			if j, ok := writeIdx[w.Key]; ok {
				merged.Writes[j] = w // last-write-wins
				continue
			}
			writeIdx[w.Key] = len(merged.Writes)
			merged.Writes = append(merged.Writes, w)
		}
	}
	return merged, events
}
