/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package app

import (
	"testing"

	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/stretchr/testify/require"
)

// TestOrderedOrdererSubmitterCount is the regression guard for the production
// clamp (see the doc comment on orderedOrdererSubmitterCount): the pipelined
// gateway path always serializes orderer submission to a single worker,
// regardless of the configured count, because the pipelined executor requires
// dependent committer txs to reach the orderer in submission order.
func TestOrderedOrdererSubmitterCount(t *testing.T) {
	for _, configured := range []int{1, 16, 0, -1} {
		got := orderedOrdererSubmitterCount(configured, sdk.NoOpLogger{})
		require.Equal(t, 1, got, "configured=%d must be clamped to 1", configured)
	}
}
