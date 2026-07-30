/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// Endorser implements the ProcessProposal API to simulate the execution of ethereum transaction
type Endorser struct {
	Engine  EVMEngineInterface // Exported to allow injection of wrappers
	builder endorsement.Builder
}

// EVMEngineInterface defines the interface for EVM execution engines.
// This allows both *EVMEngine and *testimpl.EVMEngineWrapper to be used.
type EVMEngineInterface interface {
	Execute(ctx context.Context, tx *types.Transaction) (endorsement.ExecutionResult, error)
	ExecuteMergedBatch(ctx context.Context, txs []*types.Transaction) (endorsement.ExecutionResult, []execution.PerTxOutcome, error)
	// WarmBatch/AuthMergedBatch are the split form of ExecuteMergedBatch: the
	// concurrent warm pass (WarmBatch, returns an open handle) and the serial
	// authoritative pass (AuthMergedBatch, consumes and closes the handle).
	WarmBatch(ctx context.Context, txs []*types.Transaction) (*execution.WarmedBatch, error)
	AuthMergedBatch(ctx context.Context, wb *execution.WarmedBatch) (endorsement.ExecutionResult, []execution.PerTxOutcome, error)
	Call(msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	BalanceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (*big.Int, error)
	StorageAt(ctx context.Context, account ethcommon.Address, key ethcommon.Hash, blockNumber *big.Int) ([]byte, error)
	CodeAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) ([]byte, error)
	NonceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (uint64, error)
}

// New returns a new Endorser.
//
// Arguments:
//   - `engine`:  Manages EVM execution and state reads.
//   - `builder`: Creates the signed ProposalResponse.
func New(engine *execution.EVMEngine, builder endorsement.Builder) (*Endorser, error) {
	return &Endorser{
		Engine:  engine,
		builder: builder,
	}, nil
}

// Execute endorses an Ethereum transaction and returns a signed proposal response.
// Reverts are endorsed and submitted (so the receipt records status=0); client-caused failures
// (invalid tx or failed execution) surface as a non-2xx status that CreateSignedTx won't submit.
func (f *Endorser) Execute(ctx context.Context, inv endorsement.Invocation, ethTx *types.Transaction) (*peer.ProposalResponse, error) {
	// Signature and nonce are validated inside the engine during execution.
	res, err := f.Engine.Execute(ctx, ethTx)
	if err != nil {
		return response(nil, err), nil
	}

	// Build and sign the endorsement. A signing failure is a server fault, so it
	// rides in the response (500) like every other outcome, not as a Go error.
	resp, err := f.builder.Endorse(inv, res)
	if err != nil {
		return response(nil, fmt.Errorf("endorse: %w", err)), nil
	}
	return resp, nil
}

// ExecuteBatch endorses a merged batch of EVM transactions: it two-phase-executes
// them via the engine, folds the per-tx results into one read/write-set, and
// signs a single ProposalResponse over that merged set (CFT: one signature
// meets the policy). The per-tx outcomes (status + event, one per sub-tx) ride
// in the merged result's Event field (not the response Payload), because a
// later stage recovers per-tx receipts from the committed block's
// blocks.Transaction.Events, which is fed from the endorsement's
// ExecutionResult.Event — the response Payload does not survive to the
// committed block.
func (f *Endorser) ExecuteBatch(ctx context.Context, inv endorsement.Invocation, txs []*types.Transaction) (*peer.ProposalResponse, error) {
	res, outcomes, err := f.Engine.ExecuteMergedBatch(ctx, txs)
	if err != nil {
		return response(nil, err), nil
	}
	return f.endorseBatch(inv, res, outcomes)
}

// WarmBatch runs only the concurrent warm pass of a merged batch and returns an
// opaque handle (as api.WarmedBatch, so callers never depend on the execution
// package's concrete type). It produces no endorsement — signing stays in the
// authoritative pass, where it belongs. On error a nil interface is returned
// (not a typed nil), so `handle != nil` reliably distinguishes a live handle.
func (f *Endorser) WarmBatch(ctx context.Context, txs []*types.Transaction) (api.WarmedBatch, error) {
	wb, err := f.Engine.WarmBatch(ctx, txs)
	if err != nil {
		return nil, err
	}
	return wb, nil
}

// AuthBatch runs the serial authoritative pass over an already-warmed batch and
// signs the merged response, exactly as ExecuteBatch does — the two share
// endorseBatch so serial and pipelined execution fold identically. It closes the
// handle (via the engine's authoritative pass). A handle that did not originate
// from this endorser's WarmBatch is a programming error and yields a server
// error response.
func (f *Endorser) AuthBatch(ctx context.Context, inv endorsement.Invocation, warmed api.WarmedBatch) (*peer.ProposalResponse, error) {
	wb, ok := warmed.(*execution.WarmedBatch)
	if !ok {
		return response(nil, fmt.Errorf("auth batch: unexpected warmed-batch handle %T", warmed)), nil
	}
	res, outcomes, err := f.Engine.AuthMergedBatch(ctx, wb)
	if err != nil {
		return response(nil, err), nil
	}
	return f.endorseBatch(inv, res, outcomes)
}

// endorseBatch folds a batch's per-tx outcomes into the merged result's Event
// field and signs a single ProposalResponse over it. Shared by ExecuteBatch
// (serial) and AuthBatch (pipelined) so both fold and sign identically.
//
// Per-tx outcomes (status + event, nil event = no event) ride in the merged
// result's Event field — not the response Payload — because a later stage
// recovers per-tx receipts from the committed block's blocks.Transaction.Events,
// fed from the endorsement's ExecutionResult.Event; the Payload does not survive
// to the committed block.
func (f *Endorser) endorseBatch(inv endorsement.Invocation, res endorsement.ExecutionResult, outcomes []execution.PerTxOutcome) (*peer.ProposalResponse, error) {
	outcomesPayload, err := json.Marshal(outcomes)
	if err != nil {
		return response(nil, fmt.Errorf("marshal batch outcomes: %w", err)), nil
	}
	res.Event = outcomesPayload

	// Build and sign the endorsement. A signing failure is a server fault, so it
	// rides in the response (500) like every other outcome, not as a Go error.
	resp, err := f.builder.Endorse(inv, res)
	if err != nil {
		return response(nil, fmt.Errorf("endorse batch: %w", err)), nil
	}
	return resp, nil
}

// Call runs a read-only eth_call. A revert or failed execution comes back as a
// *common.CallError; on a revert the payload is returned alongside it.
func (f *Endorser) Call(ctx context.Context, msg *ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	res, err := f.Engine.Call(*msg, blockNumber)
	if err != nil {
		return res, &common.CallError{Status: classify(err), Message: err.Error(), Data: res}
	}
	return res, nil
}

func (f *Endorser) BalanceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (*big.Int, error) {
	return f.Engine.BalanceAt(ctx, account, blockNumber)
}

func (f *Endorser) StorageAt(ctx context.Context, account ethcommon.Address, key ethcommon.Hash, blockNumber *big.Int) ([]byte, error) {
	return f.Engine.StorageAt(ctx, account, key, blockNumber)
}

func (f *Endorser) CodeAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) ([]byte, error) {
	return f.Engine.CodeAt(ctx, account, blockNumber)
}

func (f *Endorser) NonceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (uint64, error) {
	return f.Engine.NonceAt(ctx, account, blockNumber)
}

// classify maps an engine error to a status code. A revert is a committed
// outcome; an *execution.ExecFailure is a valid tx whose EVM execution failed;
// an *execution.TxRejected is an invalid client tx; anything else is a
// server-side fault.
func classify(err error) int32 {
	if errors.Is(err, vm.ErrExecutionReverted) {
		return common.StatusEVMRevert
	}
	if _, ok := errors.AsType[*execution.ExecFailure](err); ok {
		return common.StatusExecFailure
	}
	if _, ok := errors.AsType[*execution.TxRejected](err); ok {
		return common.StatusTxRejected
	}
	return common.StatusServerError
}

func response(res []byte, err error) *peer.ProposalResponse {
	if err != nil {
		return &peer.ProposalResponse{
			Version: 1,
			Response: &peer.Response{
				Status:  classify(err),
				Message: err.Error(),
				Payload: res,
			},
		}
	}

	return &peer.ProposalResponse{
		Version: 1,
		Response: &peer.Response{
			Status:  common.StatusOK,
			Message: "OK",
			Payload: res,
		},
	}
}
