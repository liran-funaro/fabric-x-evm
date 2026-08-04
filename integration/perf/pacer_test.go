/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The pacer exists for the endurance demo run only. Disk, not wall-clock time,
// is what bounds a run (~6.3 KB/tx x 9 ledger copies), so a multi-hour run has
// to submit below the system's ceiling to fit the budget. Default off: an
// unpaced run still measures the true ceiling.
func TestPaceDeadlineIsLinearInIndex(t *testing.T) {
	start := time.Unix(1_000_000, 0)

	// At 100 tx/s the Nth tx is due N/100 seconds in.
	require.Equal(t, start, paceDeadline(start, 0, 100))
	require.Equal(t, start.Add(10*time.Millisecond), paceDeadline(start, 1, 100))
	require.Equal(t, start.Add(time.Second), paceDeadline(start, 100, 100))
	require.Equal(t, start.Add(time.Minute), paceDeadline(start, 6000, 100))
}

func TestPaceDeadlineDisabledWhenNonPositive(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	// Zero (or negative) means unpaced: everything is already due.
	require.Equal(t, start, paceDeadline(start, 5000, 0))
	require.Equal(t, start, paceDeadline(start, 5000, -1))
}

func TestPaceDeadlineFractionalRate(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	// 0.5 tx/s => the 3rd tx is due at t+6s.
	require.Equal(t, start.Add(6*time.Second), paceDeadline(start, 3, 0.5))
}

// TestPaceDeadlineHoldsOverLongRuns guards the property the endurance run needs:
// the schedule must not drift, because a 1% error over six hours is minutes of
// wall-clock and would blow the disk budget it was chosen to respect.
func TestPaceDeadlineHoldsOverLongRuns(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	const tps = 560.0
	const hours = 6

	n := int64(tps * 3600 * hours)
	got := paceDeadline(start, n, tps)
	want := start.Add(hours * time.Hour)
	drift := got.Sub(want)
	if drift < 0 {
		drift = -drift
	}
	require.Less(t, drift, time.Second,
		"pacing drifted %s over %dh; the disk budget assumes it does not", drift, hours)
}
