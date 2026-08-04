/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEffectiveTotalUsesFedCountOnceFeederStops guards the failure mode that a
// duration-bounded run would otherwise hit: totalToSubmit is window x wrapCount
// (~1.5e10 for the demo config) and is never reached, so a completion condition
// comparing against it never fires and a *successful* overnight run hangs
// instead of printing its stats.
func TestEffectiveTotalUsesFedCountOnceFeederStops(t *testing.T) {
	// Feeder still running (-1): fall back to the configured target.
	require.Equal(t, int64(1_000), effectiveTotal(-1, 1_000))

	// Feeder stopped early on its deadline: the fed count is the real total.
	require.Equal(t, int64(42), effectiveTotal(42, 1_000))

	// Having fed nothing is still a *known* total, not "unknown" -- otherwise a
	// run that dies before feeding would wait for 1000 commits that never come.
	require.Equal(t, int64(0), effectiveTotal(0, 1_000))
}
