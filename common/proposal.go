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
	StatusOK        int32 = 200
	StatusEVMRevert int32 = 201
	// StatusTxRejected marks a batch sub-tx rejected before execution for a
	// RETRYABLE reason (nonce too high, insufficient funds, ...): it may
	// still succeed once ledger state catches up, so the caller must leave
	// it pending rather than evict it.
	StatusTxRejected int32 = 400
	// StatusTxRejectedTerminal marks a batch sub-tx rejected before execution
	// for a reason that can never resolve as this exact tx stands (nonce too
	// low: an earlier tx with the same nonce, or this one itself, already
	// landed). The caller should evict it from the pending pool instead of
	// retrying it forever.
	StatusTxRejectedTerminal int32 = 410
	StatusExecFailure        int32 = 460 // valid tx whose EVM execution failed (out of gas, invalid opcode, ...); should be mined (not yet)
	StatusServerError        int32 = 500
)

// IsExcludedOutcome reports whether status marks a batch sub-tx that the
// endorser excluded from execution/commit entirely -- StatusTxRejected
// (retryable) or StatusTxRejectedTerminal (never retryable) -- as opposed to
// a committed-but-faulted outcome (revert, ExecFailure) or a genuine success.
// Callers that build per-sub-tx receipts (gateway/core/chain.go's
// ConvertToDomain) must skip both: neither was ever executed, so there is no
// RWS/event to build a receipt from.
func IsExcludedOutcome(status int32) bool {
	return status == StatusTxRejected || status == StatusTxRejectedTerminal
}
