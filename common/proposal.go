/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package common

type ProposalType byte

const (
	// ProposalTypeEVMTx must remain 0xfb: it is the wire-level envelope type byte
	// already relied on elsewhere (gateway/core, integration tests).
	ProposalTypeEVMTx ProposalType = 0xfb + iota
	ProposalTypeCall
	ProposalTypeState
	// ProposalTypeEVMBatch marks a Fabric tx that carries a merged batch of EVM
	// transactions (Args[0]=type, Args[1..N]=the EVM txs), committed atomically.
	ProposalTypeEVMBatch
)

const (
	StatusOK          int32 = 200
	StatusEVMRevert   int32 = 201
	StatusTxRejected  int32 = 400 // invalid tx, rejected before execution (nonce, funds, intrinsic gas, ...)
	StatusExecFailure int32 = 460 // valid tx whose EVM execution failed (out of gas, invalid opcode, ...); should be mined (not yet)
	StatusServerError int32 = 500
)
