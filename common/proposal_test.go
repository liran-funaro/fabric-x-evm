/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package common_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-evm/common"
)

func TestProposalTypeEVMBatchDistinct(t *testing.T) {
	got := map[common.ProposalType]string{
		common.ProposalTypeEVMTx:    "evmtx",
		common.ProposalTypeCall:     "call",
		common.ProposalTypeState:    "state",
		common.ProposalTypeEVMBatch: "batch",
	}
	if len(got) != 4 {
		t.Fatalf("proposal types collide: %v", got)
	}
	if common.ProposalTypeEVMBatch == common.ProposalTypeEVMTx {
		t.Fatal("batch type must differ from evmtx")
	}
}
