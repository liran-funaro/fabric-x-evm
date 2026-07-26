/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/execution"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// newExecutorTestGateway builds a Gateway with just what executeCycle needs:
// a real *EndorsementClient over a stub low-level endorser (so ExecuteBatch
// builds a genuine signed invocation/proposal, including a real TxID), an
// empty pending pool for the caller to seed, and a buffered endorsementChan
// so SubmitFabricTx never blocks on a live BatchSubmitter.
func newExecutorTestGateway(stub *stubEndorser) *Gateway {
	return &Gateway{
		endorsers:       signingClient(stub),
		pending:         NewPendingPool(),
		arrivals:        make(chan struct{}, 1),
		endorsementChan: make(chan sdk.Endorsement, 1),
		commitWaiters:   make(map[string]chan committerpb.Status),
		commitTimeout:   commitTimeoutDefault,
	}
}

func okBatchResponse() *peer.ProposalResponse {
	return &peer.ProposalResponse{Response: &peer.Response{Status: common.StatusOK}}
}

// batchResponseWithStatuses builds a signed batch ProposalResponse whose
// top-level Payload decodes (via includedTxs/decodeProposalResponseOutcomes)
// to one execution.PerTxOutcome per given status, mirroring the real
// endorsement/fabricx.Builder.Endorse shape: Payload is a marshaled
// applicationpb.Tx whose Metadata[1] is a marshaled peer.ChaincodeEvent
// carrying the JSON-encoded outcomes. Used to make a stub endorser report
// some sub-txs excluded (common.StatusTxRejected) without a real EVM engine.
func batchResponseWithStatuses(t *testing.T, statuses ...int32) *peer.ProposalResponse {
	t.Helper()
	outcomes := make([]execution.PerTxOutcome, len(statuses))
	for i, s := range statuses {
		outcomes[i] = execution.PerTxOutcome{Status: s}
	}
	outcomesPayload, err := json.Marshal(outcomes)
	require.NoError(t, err)

	eventBytes, err := proto.Marshal(&peer.ChaincodeEvent{Payload: outcomesPayload, EventName: "log"})
	require.NoError(t, err)

	txPayload, err := proto.Marshal(&applicationpb.Tx{Metadata: [][]byte{nil, eventBytes}})
	require.NoError(t, err)

	return &peer.ProposalResponse{
		Response: &peer.Response{Status: common.StatusOK},
		Payload:  txPayload,
	}
}

// addThreeTxs seeds the pool with 3 distinct pending txs and returns them.
func addThreeTxs(g *Gateway) [3]*types.Transaction {
	tx1, tx2, tx3 := txWithNonce(1), txWithNonce(2), txWithNonce(3)
	g.pending.Add(tx1)
	g.pending.Add(tx2)
	g.pending.Add(tx3)
	return [3]*types.Transaction{tx1, tx2, tx3}
}

// runCycleAndCapture runs one executeCycle in the background and returns the
// submitted endorsement, read off endorsementChan. It fails the test if
// nothing is submitted within the timeout.
func runCycleAndCapture(t *testing.T, g *Gateway) (sdk.Endorsement, <-chan struct{}) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.executeCycle(context.Background())
		close(done)
	}()

	select {
	case end := <-g.endorsementChan:
		return end, done
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the batch to be submitted")
		return sdk.Endorsement{}, done
	}
}

// TestExecutorDrainCycle: 3 pending txs, one executeCycle, and a simulated
// VALID commit notification for the committer tx (delivered the same way the
// AllTxBatchDispatcher would, via HandleTx) -> ExecuteBatch is called once
// with all 3 txs merged into one invocation, the merged endorsement is
// submitted once, and after the commit notification the 3 txs are removed
// from the pending pool.
func TestExecutorDrainCycle(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)

	// ExecuteBatch was called exactly once, with all 3 txs merged into one
	// invocation (Args[0]=type byte, Args[1..3]=the 3 tx bytes).
	require.Len(t, stub.gotInv.Args, 4)

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NotEmpty(t, fabricTxID)

	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after the commit notification")
	}

	require.Equal(t, 0, g.pending.Len())
	for _, tx := range txs {
		require.False(t, g.pending.Has(tx.Hash()))
	}
}

// TestExecutorRollbackOnInvalid: same setup, but the commit notification
// reports an MVCC abort (invalid) -> the 3 txs remain pending so the next
// cycle re-drains and retries them (rollback).
func TestExecutorRollbackOnInvalid(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)

	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after the abort notification")
	}

	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()))
	}
}

// TestExecutorAwaitCommitTimesOut: if a commit/abort notification for a
// registered FabricTxID is never delivered (e.g. lost upstream before the
// gateway's HandleTx runs), awaitCommit must not block the executor forever.
// A tiny commitTimeout override forces the stall backstop to fire quickly;
// executeCycle then leaves the batch pending, exactly as it would for any
// other await-commit failure (rollback).
func TestExecutorAwaitCommitTimesOut(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.commitTimeout = 20 * time.Millisecond // force a fast timeout for this test
	txs := addThreeTxs(g)

	_, done := runCycleAndCapture(t, g) // submitted, but never HandleTx'd

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after the commit-wait timeout")
	}

	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()))
	}
}

// TestExecutorNonceGap: 3 pending txs, but the endorser's signed response
// reports the MIDDLE one excluded (common.StatusTxRejected -- e.g. a
// nonce-too-high gap tx; see EVMEngine.ExecuteBatch's authoritative pass,
// Task 8) while the other two are included. After a VALID commit
// notification, executeCycle must remove only the two INCLUDED txs from the
// pending pool; the excluded one stays pending so a future cycle retries it
// once its predecessor fills the gap.
func TestExecutorNonceGap(t *testing.T) {
	stub := &stubEndorser{execResp: batchResponseWithStatuses(t, common.StatusOK, common.StatusTxRejected, common.StatusOK)}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)

	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after the commit notification")
	}

	require.Equal(t, 1, g.pending.Len())
	require.False(t, g.pending.Has(txs[0].Hash()), "included tx[0] must be removed")
	require.True(t, g.pending.Has(txs[1].Hash()), "excluded tx[1] must stay pending")
	require.False(t, g.pending.Has(txs[2].Hash()), "included tx[2] must be removed")
}

// TestGatewayHandleCommitsOnValidBlockTx: same drain-cycle setup as
// TestExecutorDrainCycle, but the commit outcome is delivered via Handle
// (the block-sync topology's blocks.BlockHandler path) instead of HandleTx
// (the notification topology). A block containing one Valid transaction
// whose ID matches the submitted committer TxID must signal the waiter with
// Status_COMMITTED, exactly as the notification path would, so the 3 merged
// txs are removed from the pending pool.
func TestGatewayHandleCommitsOnValidBlockTx(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NotEmpty(t, fabricTxID)

	require.NoError(t, g.Handle(context.Background(), blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{
			{ID: fabricTxID, Valid: true},
		},
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after the block-sync commit signal")
	}

	require.Equal(t, 0, g.pending.Len())
	for _, tx := range txs {
		require.False(t, g.pending.Has(tx.Hash()))
	}
}

// TestGatewayHandleRollsBackOnInvalidBlockTx: mirrors
// TestExecutorRollbackOnInvalid via the block-sync Handle path -- a block
// reporting the committer tx as Invalid (tx.Valid == false) must signal an
// abort, leaving the 3 merged txs pending for the next cycle to retry.
func TestGatewayHandleRollsBackOnInvalidBlockTx(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)

	require.NoError(t, g.Handle(context.Background(), blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{
			{ID: fabricTxID, Valid: false},
		},
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after the block-sync abort signal")
	}

	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()))
	}
}

// TestGatewayHandleIgnoresUnrelatedTx: a block containing a transaction whose
// ID doesn't match any registered waiter (e.g. another gateway's batch, or a
// block delivered before/after the one carrying this batch's committer tx)
// must be silently ignored -- Handle must not panic or otherwise disturb an
// in-flight cycle it doesn't recognize.
func TestGatewayHandleIgnoresUnrelatedTx(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	// Short commitTimeout: the unrelated notification below must NOT be the
	// thing that unblocks awaitCommit, so give the real (never-delivered)
	// signal a fast backstop instead of relying on commitTimeoutDefault (60s)
	// and leaking a long-lived goroutine past the end of this test.
	g.commitTimeout = 50 * time.Millisecond
	addThreeTxs(g)

	_, done := runCycleAndCapture(t, g)

	require.NoError(t, g.Handle(context.Background(), blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{
			{ID: "some-other-tx-id", Valid: true},
		},
	}))

	select {
	case <-done:
		t.Fatal("executeCycle returned immediately after an unrelated block tx; it should still be awaiting its own commit")
	case <-time.After(10 * time.Millisecond):
	}

	// The commit-wait backstop now fires (never having been signaled by the
	// unrelated tx above), rolling the batch back.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after the commit-wait timeout")
	}

	require.Equal(t, 3, g.pending.Len())
}
