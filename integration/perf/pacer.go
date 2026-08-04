/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import "time"

// paceDeadline returns when the nth submitted tx (0-based) is due if submissions
// are spread evenly at targetTPS transactions per second. A non-positive
// targetTPS means unpaced, so everything is already due.
//
// This exists for the endurance demo run. What bounds a run on the experiment
// host is DISK, not wall-clock time: the stack writes ~6.3 KB per committed EVM
// tx across 9 replicated ledger copies, so the budget is a fixed ~12M
// transactions and duration therefore trades directly against rate. A run that
// must last hours has to submit below the system's ceiling to fit. Default off,
// so an ordinary run still measures the true ceiling and every number in
// findings.md stays comparable.
//
// Computed from the run's start plus an absolute offset rather than by
// accumulating per-tx sleeps, so the schedule cannot drift: over six hours a 1%
// drift is minutes of wall-clock, which would blow the disk budget the rate was
// chosen to respect.
func paceDeadline(start time.Time, n int64, targetTPS float64) time.Time {
	if targetTPS <= 0 {
		return start
	}
	offset := float64(n) / targetTPS * float64(time.Second)
	return start.Add(time.Duration(offset))
}
