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
	"github.com/hyperledger/fabric-x-sdk/blocks"
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
//
// rws is the batch's merged read-write set for e.namespace, decoded from the
// same signed response (see decodeMergedRWS) -- the pipelined executor
// applies it to the cross-batch VersionedCache (ApplyWrites) BEFORE
// submitting, so the NEXT batch's endorsers can read this batch's in-flight
// writes without waiting for its committer tx to commit.
func (e *EndorsementClient) ExecuteBatch(ctx context.Context, txs []*types.Transaction) (sdk.Endorsement, []*types.Transaction, []*types.Transaction, blocks.ReadWriteSet, error) {
	args := make([][]byte, 0, len(txs)+1)
	args = append(args, []byte{byte(common.ProposalTypeEVMBatch)})
	for _, tx := range txs {
		b, err := tx.MarshalBinary()
		if err != nil {
			return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, err
		}
		args = append(args, b)
	}
	inv, err := e.createInvocation(args)
	if err != nil {
		return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, err
	}

	res, errs := e.fanOut(ctx, func(ctx context.Context, _ int, endorser api.Service) (*peer.ProposalResponse, error) {
		return statusOnly(endorser.ExecuteBatch(ctx, inv, txs))
	})
	return e.finishBatch(inv, txs, res, errs)
}

// WarmBatch runs only the warm pass of a batch across all endorsers and bundles
// the per-endorser warmed handles into one gateway-level WarmedBatch (handling
// N >= 1 endorsers; today N = 1). It builds the invocation up front so AuthBatch
// need not — this lets the pipelined executor overlap invocation-building and
// warming of batch N+1 with the authoritative pass of batch N. The caller MUST
// later pass the handle to AuthBatch or Close it; on any per-endorser warm
// failure, WarmBatch closes the handles that did open (no snapshot leaks) and
// returns the error.
func (e *EndorsementClient) WarmBatch(ctx context.Context, txs []*types.Transaction) (*WarmedBatch, error) {
	args := make([][]byte, 0, len(txs)+1)
	args = append(args, []byte{byte(common.ProposalTypeEVMBatch)})
	for _, tx := range txs {
		b, err := tx.MarshalBinary()
		if err != nil {
			return nil, err
		}
		args = append(args, b)
	}
	inv, err := e.createInvocation(args)
	if err != nil {
		return nil, err
	}

	// Derive a cancellable context so goroutines can stop early on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	per := make([]api.WarmedBatch, len(e.endorsers))
	errs := make([]error, len(e.endorsers)) // indexed — deterministic error order
	var wg sync.WaitGroup
	for i, end := range e.endorsers {
		run := func(index int, endorser api.Service) {
			wb, err := endorser.WarmBatch(ctx, txs)
			if err != nil {
				errs[index] = fmt.Errorf("call endorser warm: %w", err)
				cancel()
				return
			}
			per[index] = wb
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

	// Return first error in slice order — stable and deterministic. Close any
	// handles that DID open so a partially-warmed batch never leaks a snapshot.
	for _, err := range errs {
		if err != nil {
			closeWarmHandles(per)
			return nil, err
		}
	}

	return &WarmedBatch{inv: inv, txs: txs, per: per}, nil
}

// AuthBatch runs the authoritative pass over an already-warmed batch across all
// endorsers and returns the same (endorsement, included, terminal, mergedRWS)
// tuple as ExecuteBatch — the two share finishBatch so serial and pipelined
// execution decode identically. Each endorser's AuthBatch closes its own warmed
// handle; the executor may still Close the bundle defensively (idempotent).
func (e *EndorsementClient) AuthBatch(ctx context.Context, warmed *WarmedBatch) (sdk.Endorsement, []*types.Transaction, []*types.Transaction, blocks.ReadWriteSet, error) {
	if warmed == nil {
		return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, fmt.Errorf("auth batch: nil warmed handle")
	}
	res, errs := e.fanOut(ctx, func(ctx context.Context, index int, endorser api.Service) (*peer.ProposalResponse, error) {
		return statusOnly(endorser.AuthBatch(ctx, warmed.inv, warmed.per[index]))
	})
	return e.finishBatch(warmed.inv, warmed.txs, res, errs)
}

// WarmedBatch bundles the per-endorser warmed handles for one batch (index-aligned
// with EndorsementClient.endorsers) plus the invocation and txs the authoritative
// pass needs. It is produced by WarmBatch and consumed by AuthBatch.
type WarmedBatch struct {
	inv endorsement.Invocation
	txs []*types.Transaction
	per []api.WarmedBatch
}

// Txs returns the batch's transactions (in batch order) so the pipelined
// executor can Release their pending-pool reservation at the boundary.
func (w *WarmedBatch) Txs() []*types.Transaction { return w.txs }

// warmWriteSetter is the optional capability a per-endorser warmed handle
// exposes to accept the frozen warm-pass write snapshot (see
// execution.WarmedBatch.SetWarmWrites). The embedded endorser's handle is an
// *execution.WarmedBatch and implements it; a remote endorser's handle does not
// (its read cache lives out of process), so the fan-out simply skips it.
type warmWriteSetter interface {
	SetWarmWrites(map[string]*blocks.WriteRecord)
}

// SetWarmWrites hands the frozen warm-pass write snapshot to every per-endorser
// handle that can accept it. The pipelined executor calls this right after
// WarmBatch so the batch's authoritative pass can replay the in-flight keys the
// warm pass read from the write cache even after they commit and evict (see
// execution.WarmedBatch.SetWarmWrites and cachedView.warmWrites). A nil/empty
// snapshot, or a handle that cannot accept it, is a no-op.
func (w *WarmedBatch) SetWarmWrites(snap map[string]*blocks.WriteRecord) {
	if w == nil || len(snap) == 0 {
		return
	}
	for _, h := range w.per {
		if s, ok := h.(warmWriteSetter); ok {
			s.SetWarmWrites(snap)
		}
	}
}

// Close releases every per-endorser warmed handle. It is safe to call more than
// once and after AuthBatch (the endorser handles' Close is idempotent), so the
// executor can Close defensively on any abandon/error/shutdown path.
func (w *WarmedBatch) Close() error {
	if w == nil {
		return nil
	}
	closeWarmHandles(w.per)
	return nil
}

// closeWarmHandles closes each non-nil handle, ignoring per-handle Close errors
// (a snapshot-close failure is not actionable at this layer).
func closeWarmHandles(per []api.WarmedBatch) {
	for _, h := range per {
		if h != nil {
			_ = h.Close()
		}
	}
}

// statusOnly maps a per-endorser batch response to the fan-out contract: a
// non-nil error is a transport/delivery failure; a non-OK status is an
// application failure of the merged batch. Both are returned as errors so the
// fan-out cancels its siblings; only a StatusOK response passes through.
func statusOnly(pResp *peer.ProposalResponse, err error) (*peer.ProposalResponse, error) {
	if err != nil {
		return nil, fmt.Errorf("call endorser: %w", err)
	}
	if pResp.Response.Status != common.StatusOK {
		return nil, fmt.Errorf("process EVM batch: %s", pResp.Response.Message)
	}
	return pResp, nil
}

// fanOut runs call for every endorser — concurrently when there is more than
// one, inline for the common single-endorser case — and returns the
// per-endorser responses and errors, index-aligned and in deterministic slice
// order. The first per-endorser error cancels the derived context so siblings
// can stop early. call must depend only on its index; it must not write shared
// state for another endorser.
func (e *EndorsementClient) fanOut(ctx context.Context, call func(ctx context.Context, index int, endorser api.Service) (*peer.ProposalResponse, error)) ([]*peer.ProposalResponse, []error) {
	// Derive a cancellable context so goroutines can stop early on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := make([]*peer.ProposalResponse, len(e.endorsers))
	errs := make([]error, len(e.endorsers)) // indexed — deterministic error order
	var wg sync.WaitGroup
	for i, end := range e.endorsers {
		run := func(index int, endorser api.Service) {
			pResp, err := call(ctx, index, endorser)
			if err != nil {
				errs[index] = err
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
	return res, errs
}

// finishBatch turns the fan-out result into the batch endorsement: it returns
// the first per-endorser error (stable slice order), otherwise decodes the
// included/terminal tx subsets and the merged read-write set from any one signed
// response (every responding endorser executed the same deterministic batch, so
// they decode identically). Shared by ExecuteBatch (serial) and AuthBatch
// (pipelined).
func (e *EndorsementClient) finishBatch(inv endorsement.Invocation, txs []*types.Transaction, res []*peer.ProposalResponse, errs []error) (sdk.Endorsement, []*types.Transaction, []*types.Transaction, blocks.ReadWriteSet, error) {
	for _, err := range errs {
		if err != nil {
			return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, err
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
		return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, fmt.Errorf("decode included txs: %w", err)
	}

	mergedRWS, err := decodeMergedRWS(signed, e.namespace)
	if err != nil {
		return sdk.Endorsement{}, nil, nil, blocks.ReadWriteSet{}, fmt.Errorf("decode merged read-write set: %w", err)
	}

	return sdk.Endorsement{
		Proposal:  inv.Proposal,
		Responses: res,
	}, included, terminal, mergedRWS, nil
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

// decodeMergedRWS recovers namespace's merged read-write set from a signed
// batch ProposalResponse's top-level Payload -- the same applicationpb.Tx
// envelope decodeProposalResponseOutcomes reads (Metadata carries the
// outcomes; the Namespaces carry the actual reads/writes the endorsers
// agreed on). This is the read-write set the pipelined executor applies to
// the cross-batch VersionedCache (see executeCycle / VersionedCache.ApplyWrites)
// so the next batch can read this one's in-flight writes before it commits.
//
// Returns a zero-value ReadWriteSet — not an error — for a nil/payload-less
// response, or when the payload has no namespace matching e.namespace, since
// both are legitimate ("nothing to apply") rather than malformed input; only
// an undecodable payload is an error.
func decodeMergedRWS(resp *peer.ProposalResponse, namespace string) (blocks.ReadWriteSet, error) {
	if resp == nil || len(resp.Payload) == 0 {
		return blocks.ReadWriteSet{}, nil
	}
	var ptx applicationpb.Tx
	if err := proto.Unmarshal(resp.Payload, &ptx); err != nil {
		return blocks.ReadWriteSet{}, fmt.Errorf("unmarshal proposal response payload: %w", err)
	}
	for _, ns := range ptx.Namespaces {
		if ns.NsId != namespace {
			continue
		}
		return decodeNsRWS(ns), nil
	}
	return blocks.ReadWriteSet{}, nil
}

// decodeNsRWS decodes one applicationpb.TxNamespace into a blocks.ReadWriteSet,
// mirroring blocks/fabricx.BlockParser.ParseTx's namespace decode loop exactly
// (same field mapping, including its same non-recovery of KVWrite.IsDelete --
// the wire format has no delete flag; see endorsement/fabricx.marshalRWSet,
// which encodes a delete as a Write with a nil Value, indistinguishable on
// the wire from a blind write of an empty value).
func decodeNsRWS(ns *applicationpb.TxNamespace) blocks.ReadWriteSet {
	rws := blocks.ReadWriteSet{
		Reads:  make([]blocks.KVRead, 0, len(ns.ReadWrites)),
		Writes: make([]blocks.KVWrite, 0, len(ns.BlindWrites)+len(ns.ReadWrites)),
	}
	for _, bw := range ns.BlindWrites {
		rws.Writes = append(rws.Writes, blocks.KVWrite{Key: string(bw.Key), Value: bw.Value})
	}
	for _, rw := range ns.ReadWrites {
		read := blocks.KVRead{Key: string(rw.Key)}
		if rw.Version != nil {
			read.Version = &blocks.Version{BlockNum: *rw.Version}
		}
		rws.Reads = append(rws.Reads, read)
		rws.Writes = append(rws.Writes, blocks.KVWrite{Key: string(rw.Key), Value: rw.Value})
	}
	return rws
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
