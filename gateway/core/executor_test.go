/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-evm/common"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/stretchr/testify/require"
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
