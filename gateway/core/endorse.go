/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"math/big"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/api"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// EndorsementClient forwards ethereum-style transactions and calls
// to the endorsers and returns their signed fabric-style responses.
type EndorsementClient struct {
	endorsers []api.Service
	signer    Signer
	channel   string
	namespace string
	nsVersion string
}

// NewEndorsementClient creates an EndorsementClient from api.Service instances.
// This allows using concrete endorsers, wrapped endorsers (e.g., from testimpl package), remote
// gRPC clients to other organizations' endorsers, or other implementations.
func NewEndorsementClient(endorsers []api.Service, signer Signer, channel, namespace, nsVersion string) (*EndorsementClient, error) {
	return &EndorsementClient{
		endorsers: endorsers,
		signer:    signer,
		channel:   channel,
		namespace: namespace,
		nsVersion: nsVersion,
	}, nil
}

func (e EndorsementClient) ExecuteTransaction(ctx context.Context, tx *types.Transaction) (sdk.Endorsement, error) {
	// Marshal the transaction for the invocation args
	ethTxBytes, err := tx.MarshalBinary()
	if err != nil {
		return sdk.Endorsement{}, err
	}

	// Create invocation
	inv, err := e.createInvocation([][]byte{{byte(common.ProposalTypeEVMTx)}, ethTxBytes})
	if err != nil {
		return sdk.Endorsement{}, err
	}

	// Derive a cancellable context so goroutines can stop early on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	res := make([]*peer.ProposalResponse, len(e.endorsers))
	errs := make([]error, len(e.endorsers)) // indexed — deterministic error order

	for i, end := range e.endorsers {
		processEndorsement := func(index int, endorser api.Service) {
			pResp, err := endorser.Execute(ctx, inv, tx)
			if err != nil {
				// A Go error is a transport/delivery failure (e.g. gRPC), not a tx outcome.
				errs[index] = fmt.Errorf("call endorser: %w", err)
				cancel() // signal other goroutines to stop early
				return
			}
			// Application outcomes ride in the status. A success and a revert are
			// committed; a valid tx whose execution failed is endorsable here but
			// not yet committed (a follow-up will mine it). An invalid (rejected) tx
			// or a server fault is an error the caller must see.
			switch pResp.Response.Status {
			case common.StatusOK, common.StatusEVMRevert, common.StatusExecFailure:
				res[index] = pResp
			default:
				errs[index] = fmt.Errorf("process EVM transaction: %s", pResp.Response.Message)
				cancel()
			}
		}

		if len(e.endorsers) > 1 {
			wg.Add(1)
			go func(index int, endorser api.Service) {
				defer wg.Done()
				processEndorsement(index, endorser)
			}(i, end)
		} else {
			processEndorsement(i, end)
		}
	}

	wg.Wait()

	// Return first error in slice order — stable and deterministic
	for _, err := range errs {
		if err != nil {
			return sdk.Endorsement{}, err
		}
	}

	return sdk.Endorsement{
		Proposal:  inv.Proposal,
		Responses: res,
	}, nil
}

// ExecuteBatch endorses a batch of EVM transactions as one merged Fabric tx.
// The invocation carries the batch: Args[0]=ProposalTypeEVMBatch, Args[1..N]=the
// marshaled EVM txs, in batch order.
//
// The endorser may EXCLUDE some of txs from the batch (a client-rejected tx,
// e.g. a nonce gap — see endorser/execution.EVMEngine.ExecuteBatch's
// authoritative pass) while still endorsing the rest: the committer tx's Args
// still carry all of txs (so the FabricTxID/invocation stays the same), but
// only the included ones actually commit anything. ExecuteBatch decodes each
// tx's outcome from the signed response and returns two subsets alongside the
// endorsement:
//   - included: actually executed (success, revert, or ExecFailure — every
//     committed outcome); the caller removes these from the pending pool
//     once the batch's committer tx commits.
//   - terminal: excluded for a reason that can never resolve as this exact
//     tx stands (nonce too low); the caller should evict these from the
//     pending pool unconditionally, regardless of this batch's own outcome.
//
// A tx excluded for a RETRYABLE reason (nonce too high, insufficient funds,
// ...) appears in neither slice: the caller leaves it pending for a future
// cycle instead of removing or evicting it.
func (e *EndorsementClient) ExecuteBatch(ctx context.Context, txs []*types.Transaction) (sdk.Endorsement, []*types.Transaction, []*types.Transaction, error) {
	args := make([][]byte, 0, len(txs)+1)
	args = append(args, []byte{byte(common.ProposalTypeEVMBatch)})
	for _, tx := range txs {
		b, err := tx.MarshalBinary()
		if err != nil {
			return sdk.Endorsement{}, nil, nil, err
		}
		args = append(args, b)
	}
	inv, err := e.createInvocation(args)
	if err != nil {
		return sdk.Endorsement{}, nil, nil, err
	}

	// Derive a cancellable context so goroutines can stop early on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := make([]*peer.ProposalResponse, len(e.endorsers))
	errs := make([]error, len(e.endorsers)) // indexed — deterministic error order
	var wg sync.WaitGroup
	for i, end := range e.endorsers {
		run := func(index int, endorser api.Service) {
			pResp, err := endorser.ExecuteBatch(ctx, inv, txs)
			if err != nil {
				errs[index] = fmt.Errorf("call endorser: %w", err)
				cancel()
				return
			}
			if pResp.Response.Status != common.StatusOK {
				errs[index] = fmt.Errorf("process EVM batch: %s", pResp.Response.Message)
				cancel()
				return
			}
			res[index] = pResp
		}
		if len(e.endorsers) > 1 {
			wg.Add(1)
			go func(index int, endorser api.Service) {
				defer wg.Done()
				run(index, endorser)
			}(i, end)
		} else {
			run(i, end)
		}
	}
	wg.Wait()

	// Return first error in slice order — stable and deterministic
	for _, err := range errs {
		if err != nil {
			return sdk.Endorsement{}, nil, nil, err
		}
	}

	// Every responding endorser executed the same deterministic batch, so any
	// one signed response decodes the same per-sub-tx outcomes; use the first
	// non-nil one.
	var signed *peer.ProposalResponse
	for _, r := range res {
		if r != nil {
			signed = r
			break
		}
	}
	included, terminal, err := classifyBatchOutcomes(signed, txs)
	if err != nil {
		return sdk.Endorsement{}, nil, nil, fmt.Errorf("decode included txs: %w", err)
	}

	return sdk.Endorsement{
		Proposal:  inv.Proposal,
		Responses: res,
	}, included, terminal, nil
}

// classifyBatchOutcomes decodes txs' per-sub-tx outcomes from resp (see
// decodeProposalResponseOutcomes) and splits txs into:
//   - included: actually executed (success, revert, or ExecFailure) — i.e.
//     not excluded at all. Safe to remove from the pending pool once the
//     batch's committer tx commits.
//   - terminal: excluded with common.StatusTxRejectedTerminal (nonce too
//     low): can never succeed as this exact tx stands. Safe to evict from
//     the pending pool unconditionally — the exclusion reflects ledger state
//     from BEFORE this cycle's batch ran, so it holds regardless of whether
//     the rest of this batch goes on to commit, abort, or time out.
//
// A tx excluded with common.StatusTxRejected (retryable: nonce too high,
// insufficient funds, ...) appears in neither slice: the caller leaves it
// pending for a future cycle.
//
// When outcomes can't be recovered at all (e.g. a bare/test response with no
// payload), every tx is treated as included and terminal is empty: the safe
// default when there is nothing to exclude on, and what preserves
// pre-Task-8 "whole batch commits" behavior.
func classifyBatchOutcomes(resp *peer.ProposalResponse, txs []*types.Transaction) (included, terminal []*types.Transaction, err error) {
	outcomes, err := decodeProposalResponseOutcomes(resp)
	if err != nil {
		return nil, nil, err
	}
	if outcomes == nil {
		return txs, nil, nil
	}

	included = make([]*types.Transaction, 0, len(txs))
	for i, tx := range txs {
		var status int32
		if i < len(outcomes) {
			status = outcomes[i].Status
		}
		switch status {
		case common.StatusTxRejectedTerminal:
			terminal = append(terminal, tx) // excluded, can never succeed as submitted
		case common.StatusTxRejected:
			// excluded, retryable: never committed, caller leaves it pending
		default:
			included = append(included, tx)
		}
	}
	return included, terminal, nil
}

// decodeProposalResponseOutcomes recovers the per-sub-tx outcomes
// (execution.PerTxOutcome, one per sub-index) from a signed batch
// ProposalResponse's top-level Payload — the applicationpb.Tx envelope built
// by endorsement/fabricx.Builder.Endorse (Metadata[0]=input args,
// Metadata[1]=the merged ExecutionResult.Event wrapped in one ChaincodeEvent).
// This is the same envelope blocks/fabricx.BlockParser.ParseTx later decodes
// into blocks.Transaction.Events for the committed block — see
// decodeBatchOutcomes in chain.go, which this reuses for the inner
// ChaincodeEvent/JSON decode once the outer applicationpb.Tx layer is
// stripped. Returns (nil, nil) — "nothing to decode", not an error — for a
// nil/payload-less response, so callers can fall back to treating every tx as
// included.
func decodeProposalResponseOutcomes(resp *peer.ProposalResponse) ([]execution.PerTxOutcome, error) {
	if resp == nil || len(resp.Payload) == 0 {
		return nil, nil
	}
	var ptx applicationpb.Tx
	if err := proto.Unmarshal(resp.Payload, &ptx); err != nil {
		return nil, fmt.Errorf("unmarshal proposal response payload: %w", err)
	}
	if len(ptx.Metadata) < 2 || len(ptx.Metadata[1]) == 0 {
		return nil, nil
	}
	return decodeBatchOutcomes(ptx.Metadata[1])
}

// CallContract queries a smart contract and returns the value.
// An EVM revert from the endorser is surfaced as *domain.RevertError so the API
// layer can map it to JSON-RPC -32000.
func (e *EndorsementClient) CallContract(ctx context.Context, args ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	payload, err := e.endorsers[0].Call(ctx, &args, blockNumber)
	if err == nil {
		return payload, nil
	}

	callErr, ok := errors.AsType[*common.CallError](err)
	if !ok {
		// Not an application outcome: a transport/delivery failure.
		return nil, fmt.Errorf("process call: %w", err)
	}
	if callErr.Reverted() {
		return nil, &domain.RevertError{Reason: callErr.Message, Data: callErr.Data}
	}
	// For a call, both a failed execution and a rejected tx are surfaced as an
	// execution error (-32000); only the reverted case carries data.
	if callErr.Status == common.StatusExecFailure || callErr.Status == common.StatusTxRejected {
		return nil, &domain.ExecutionError{Message: callErr.Message}
	}
	return nil, fmt.Errorf("query response was not successful, error code %d, msg %s", callErr.Status, callErr.Message)
}

// BalanceAt returns an account's balance at the given block.
func (e *EndorsementClient) BalanceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (*big.Int, error) {
	return e.endorsers[0].BalanceAt(ctx, account, blockNumber)
}

// StorageAt returns the storage word at key for an account.
func (e *EndorsementClient) StorageAt(ctx context.Context, account ethcommon.Address, key ethcommon.Hash, blockNumber *big.Int) ([]byte, error) {
	return e.endorsers[0].StorageAt(ctx, account, key, blockNumber)
}

// CodeAt returns an account's contract code.
func (e *EndorsementClient) CodeAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) ([]byte, error) {
	return e.endorsers[0].CodeAt(ctx, account, blockNumber)
}

// NonceAt returns an account's nonce.
func (e *EndorsementClient) NonceAt(ctx context.Context, account ethcommon.Address, blockNumber *big.Int) (uint64, error) {
	return e.endorsers[0].NonceAt(ctx, account, blockNumber)
}

// createInvocation creates an endorsement.Invocation from the given parameters
func (e *EndorsementClient) createInvocation(args [][]byte) (endorsement.Invocation, error) {
	return endorsement.NewInvocation(e.signer, e.channel, e.namespace, e.nsVersion, args)
}
