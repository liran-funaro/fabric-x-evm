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
// top-level Payload decodes (via classifyBatchOutcomes/decodeProposalResponseOutcomes)
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

// TestExecutorDrainCapBoundsBatch: with maxBatchSize set below the pending
// count, one cycle folds only the first maxBatchSize txs into the merged
// committer tx; the remainder stays pending for the next cycle. This is the
// bound that keeps a submission burst from being swallowed into one oversized
// Fabric tx (see PendingPool.DrainUpTo / Gateway.SetMaxBatchSize).
func TestExecutorDrainCapBoundsBatch(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxBatchSize(2)
	txs := addThreeTxs(g) // nonces 1,2,3 in insertion order

	end, done := runCycleAndCapture(t, g)

	// Only the first 2 txs were merged: Args[0]=type byte, Args[1..2]=2 txs.
	require.Len(t, stub.gotInv.Args, 3)

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

	// The two committed txs are gone; the third (beyond the cap) is still
	// pending and will be drained next cycle.
	require.Equal(t, 1, g.pending.Len())
	require.False(t, g.pending.Has(txs[0].Hash()))
	require.False(t, g.pending.Has(txs[1].Hash()))
	require.True(t, g.pending.Has(txs[2].Hash()))
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

// TestExecutorAllExcludedDoesNotSubmitOrSpin: all 3 pending txs are reported
// excluded (common.StatusTxRejected) by the endorser -- e.g. every drained tx
// happens to be a nonce-gap tx with no filler present yet (FIX I1(a)).
// executeCycle must NOT submit an empty committer tx for a batch with nothing
// included, and must NOT return immediately -- returning immediately would
// make runExecutor's loop busy-spin, re-draining and re-endorsing the exact
// same excluded txs cycle after cycle with no backoff. Instead it blocks in
// waitForWork exactly as the empty-pool case does, until either ctx is done
// or new work arrives. None of the 3 txs are removed from the pending pool.
func TestExecutorAllExcludedDoesNotSubmitOrSpin(t *testing.T) {
	stub := &stubEndorser{execResp: batchResponseWithStatuses(t, common.StatusTxRejected, common.StatusTxRejected, common.StatusTxRejected)}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		g.executeCycle(ctx)
		close(done)
	}()

	select {
	case <-g.endorsementChan:
		t.Fatal("executeCycle submitted a committer tx for a batch with nothing included")
	case <-done:
		t.Fatal("executeCycle returned immediately instead of waiting for new work (would busy-spin runExecutor's loop)")
	case <-time.After(200 * time.Millisecond):
		// still blocked in waitForWork, as expected
	}

	cancel() // simulate shutdown to unblock waitForWork, like a real ctx cancellation would
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeCycle did not return after ctx cancellation")
	}

	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()), "an all-excluded batch must leave every tx pending")
	}
}

// TestExecutorTerminalExclusionEvictedRetryableStays: 3 pending txs; the
// endorser's signed response reports tx[0] included (common.StatusOK), tx[1]
// excluded TERMINALLY (common.StatusTxRejectedTerminal -- e.g. nonce too low:
// this exact tx can never succeed as submitted), and tx[2] excluded
// RETRYABLY (common.StatusTxRejected -- e.g. nonce too high). FIX I1(b):
//   - The terminally-excluded tx is evicted from the pending pool
//     unconditionally, as soon as ExecuteBatch returns -- even before the
//     batch's committer tx has committed (the exclusion reflects pre-cycle
//     ledger state, so it holds no matter what the rest of the batch does).
//   - The retryable-excluded tx stays pending throughout: it may still
//     succeed once ledger state catches up (mirrors TestExecutorNonceGap).
//   - The included tx is removed only after a VALID commit notification.
func TestExecutorTerminalExclusionEvictedRetryableStays(t *testing.T) {
	stub := &stubEndorser{execResp: batchResponseWithStatuses(t, common.StatusOK, common.StatusTxRejectedTerminal, common.StatusTxRejected)}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)

	// Terminal eviction is unconditional: it must already have happened by the
	// time the committer tx is submitted, well before any commit notification.
	require.Equal(t, 2, g.pending.Len(), "terminal tx must be evicted as soon as ExecuteBatch returns")
	require.True(t, g.pending.Has(txs[0].Hash()), "included tx[0] stays pending until commit")
	require.False(t, g.pending.Has(txs[1].Hash()), "terminal tx[1] must be evicted immediately")
	require.True(t, g.pending.Has(txs[2].Hash()), "retryable tx[2] stays pending")

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
	require.False(t, g.pending.Has(txs[0].Hash()), "included tx[0] must be removed after commit")
	require.False(t, g.pending.Has(txs[1].Hash()), "terminal tx[1] must stay evicted")
	require.True(t, g.pending.Has(txs[2].Hash()), "retryable tx[2] must stay pending")
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
