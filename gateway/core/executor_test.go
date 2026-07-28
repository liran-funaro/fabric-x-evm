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

// newExecutorTestGateway builds a Gateway with just what the pipelined
// executeCycle needs: a real *EndorsementClient over a stub low-level endorser
// (so ExecuteBatch builds a genuine signed invocation/proposal, including a
// real TxID), an empty pending pool for the caller to seed, a fresh
// cross-batch cache, a full-size in-flight window, and a buffered
// endorsementChan so SubmitFabricTx never blocks on a live BatchSubmitter.
func newExecutorTestGateway(stub *stubEndorser) *Gateway {
	return &Gateway{
		endorsers:       signingClient(stub),
		pending:         NewPendingPool(),
		arrivals:        make(chan struct{}, 1),
		endorsementChan: make(chan sdk.Endorsement, 1),
		cache:           NewVersionedCache(),
		inflightSlots:   make(chan struct{}, defaultMaxInflight),
		maxInflight:     defaultMaxInflight,
		commitTimeout:   commitTimeoutDefault,
	}
}

// inflightCount returns how many submitted-but-unresolved batches the pipeline
// is currently tracking. Test-only helper (same package) for asserting the
// in-flight registry state without reaching into its lock at every call site.
func (g *Gateway) inflightCount() int {
	g.inflightMu.Lock()
	defer g.inflightMu.Unlock()
	return len(g.inflight)
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

// batchResponseWithWrites builds a signed batch ProposalResponse whose
// top-level Payload carries a namespace read-write set (one blind write per
// key) for namespace "ns" -- the namespace signingClient uses. It sets no
// Metadata outcomes, so classifyBatchOutcomes falls back to "all included":
// every drained tx commits AND the batch reports the given writes, which is
// what the pipelined executor applies to the cross-batch cache (ApplyWrites)
// before submitting. Used to exercise the cache write-through without a real
// EVM engine.
func batchResponseWithWrites(t *testing.T, writes map[string][]byte) *peer.ProposalResponse {
	t.Helper()
	ns := &applicationpb.TxNamespace{NsId: "ns"}
	for k, v := range writes {
		ns.BlindWrites = append(ns.BlindWrites, &applicationpb.Write{Key: []byte(k), Value: v})
	}
	txPayload, err := proto.Marshal(&applicationpb.Tx{Namespaces: []*applicationpb.TxNamespace{ns}})
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

// runCycleAndCapture runs one pipelined executeCycle in the background and
// returns the submitted endorsement, read off endorsementChan. Because the
// pipelined cycle does NOT wait for the commit, it returns right after submit;
// the returned done channel closes then, and the batch is left IN FLIGHT for
// the caller to resolve via HandleTx/Handle (or the timeout backstop). Fails
// the test if nothing is submitted within the timeout.
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

// TestExecutorDrainCycle: 3 pending txs, one pipelined executeCycle -> all 3
// are merged into one invocation and submitted once, and (pipelined) the
// included txs are removed from the pending pool at SUBMIT time and the batch
// is left in flight. A subsequent VALID commit notification (delivered the way
// the AllTxBatchDispatcher would, via HandleTx) resolves the in-flight batch
// and frees its slot.
func TestExecutorDrainCycle(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done // pipelined: the cycle returns right after submit, without awaiting commit

	// ExecuteBatch was called exactly once, with all 3 txs merged into one
	// invocation (Args[0]=type byte, Args[1..3]=the 3 tx bytes).
	require.Len(t, stub.gotInv.Args, 4)

	// Included txs are removed from pending at submit time, and the batch is now
	// in flight awaiting its commit outcome.
	require.Equal(t, 0, g.pending.Len())
	require.Equal(t, 1, g.inflightCount())

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NotEmpty(t, fabricTxID)

	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	// The commit resolved the batch (slot freed); the txs stay gone.
	require.Equal(t, 0, g.inflightCount())
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
	<-done

	// Only the first 2 txs were merged: Args[0]=type byte, Args[1..2]=2 txs.
	require.Len(t, stub.gotInv.Args, 3)

	// The two included txs are removed at submit; the third (beyond the cap) is
	// still pending and will be drained next cycle.
	require.Equal(t, 1, g.pending.Len())
	require.False(t, g.pending.Has(txs[0].Hash()))
	require.False(t, g.pending.Has(txs[1].Hash()))
	require.True(t, g.pending.Has(txs[2].Hash()))

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	// Commit resolves the batch; the third tx is untouched.
	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 1, g.pending.Len())
	require.True(t, g.pending.Has(txs[2].Hash()))
}

// TestExecutorRollbackOnInvalid: same setup, but the commit notification
// reports an MVCC abort (invalid) -> resolveInflight rolls the batch back,
// returning its 3 txs to the pending pool so the next cycle re-drains and
// retries them.
func TestExecutorRollbackOnInvalid(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done

	// Removed from pending at submit, in flight until the abort arrives.
	require.Equal(t, 0, g.pending.Len())
	require.Equal(t, 1, g.inflightCount())

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))

	// Rollback re-added all 3 txs to pending and freed the slot.
	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()))
	}
}

// TestExecutorBackstopRollsBackOnLostNotification: if a commit/abort
// notification for an in-flight batch is never delivered (e.g. lost upstream
// before HandleTx runs), the per-batch timeout backstop armed by trackInflight
// must eventually roll the batch back so its in-flight slot is not held
// forever. A tiny commitTimeout override forces the backstop to fire quickly;
// the batch's txs then return to pending, exactly as an MVCC abort would leave
// them (rollback).
func TestExecutorBackstopRollsBackOnLostNotification(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.commitTimeout = 20 * time.Millisecond // force a fast backstop for this test
	txs := addThreeTxs(g)

	_, done := runCycleAndCapture(t, g) // submitted, but never HandleTx'd
	<-done

	// The backstop fires (never having been signaled), rolling the batch back:
	// its txs return to pending and the in-flight slot is freed.
	require.Eventually(t, func() bool {
		return g.inflightCount() == 0 && g.pending.Len() == 3
	}, 2*time.Second, 5*time.Millisecond)
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()))
	}
}

// TestExecutorNonceGap: 3 pending txs, but the endorser's signed response
// reports the MIDDLE one excluded (common.StatusTxRejected -- e.g. a
// nonce-too-high gap tx; see EVMEngine.ExecuteBatch's authoritative pass) while
// the other two are included. executeCycle must remove only the two INCLUDED
// txs from the pending pool (at submit); the excluded one stays pending so a
// future cycle retries it once its predecessor fills the gap.
func TestExecutorNonceGap(t *testing.T) {
	stub := &stubEndorser{execResp: batchResponseWithStatuses(t, common.StatusOK, common.StatusTxRejected, common.StatusOK)}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done

	require.Equal(t, 1, g.pending.Len())
	require.False(t, g.pending.Has(txs[0].Hash()), "included tx[0] must be removed at submit")
	require.True(t, g.pending.Has(txs[1].Hash()), "excluded tx[1] must stay pending")
	require.False(t, g.pending.Has(txs[2].Hash()), "included tx[2] must be removed at submit")

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	// Commit resolves the batch; the excluded tx is untouched.
	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 1, g.pending.Len())
	require.True(t, g.pending.Has(txs[1].Hash()))
}

// TestExecutorAllExcludedDoesNotSubmitOrSpin: all 3 pending txs are reported
// excluded (common.StatusTxRejected) by the endorser -- e.g. every drained tx
// happens to be a nonce-gap tx with no filler present yet (FIX I1(a)).
// executeCycle must NOT submit an empty committer tx for a batch with nothing
// included, and must NOT return immediately -- returning immediately would
// make runExecutor's loop busy-spin, re-draining and re-endorsing the exact
// same excluded txs cycle after cycle with no backoff. Instead it blocks in
// waitForWork exactly as the empty-pool case does, until either ctx is done
// or new work arrives. None of the 3 txs are removed from the pending pool,
// and nothing is left in flight.
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

	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()), "an all-excluded batch must leave every tx pending")
	}
}

// TestExecutorTerminalExclusionEvictedRetryableStays: 3 pending txs; the
// endorser's signed response reports tx[0] included (common.StatusOK), tx[1]
// excluded TERMINALLY (common.StatusTxRejectedTerminal -- e.g. nonce too low:
// this exact tx can never succeed as submitted), and tx[2] excluded RETRYABLY
// (common.StatusTxRejected -- e.g. nonce too high). FIX I1(b):
//   - The included tx AND the terminally-excluded tx are both removed from the
//     pending pool as soon as the batch is submitted; the terminal one because
//     its exclusion reflects pre-cycle ledger state that holds regardless of
//     this batch's outcome, the included one because it is now in flight.
//   - The retryable-excluded tx stays pending throughout: it may still succeed
//     once ledger state catches up (mirrors TestExecutorNonceGap).
//   - A later ROLLBACK re-adds only the included tx, never the terminal one
//     (see TestExecutorRollbackKeepsTerminalEvicted).
func TestExecutorTerminalExclusionEvictedRetryableStays(t *testing.T) {
	stub := &stubEndorser{execResp: batchResponseWithStatuses(t, common.StatusOK, common.StatusTxRejectedTerminal, common.StatusTxRejected)}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done

	// Included tx[0] and terminal tx[1] are both gone at submit; retryable tx[2]
	// stays pending.
	require.Equal(t, 1, g.pending.Len())
	require.False(t, g.pending.Has(txs[0].Hash()), "included tx[0] removed at submit")
	require.False(t, g.pending.Has(txs[1].Hash()), "terminal tx[1] evicted at submit")
	require.True(t, g.pending.Has(txs[2].Hash()), "retryable tx[2] stays pending")

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 1, g.pending.Len())
	require.True(t, g.pending.Has(txs[2].Hash()), "retryable tx[2] must stay pending")
}

// TestExecutorRollbackKeepsTerminalEvicted: on a rollback, resolveInflight
// re-adds the batch's INCLUDED txs (they never committed, retry them) but must
// NOT resurrect a terminally-excluded tx -- it was evicted at submit because it
// can never succeed as submitted, and that verdict is independent of this
// batch's commit outcome.
func TestExecutorRollbackKeepsTerminalEvicted(t *testing.T) {
	stub := &stubEndorser{execResp: batchResponseWithStatuses(t, common.StatusOK, common.StatusTxRejectedTerminal, common.StatusOK)}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))

	// The two included txs come back; the terminal one stays evicted.
	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 2, g.pending.Len())
	require.True(t, g.pending.Has(txs[0].Hash()), "included tx[0] re-added on rollback")
	require.False(t, g.pending.Has(txs[1].Hash()), "terminal tx[1] must stay evicted")
	require.True(t, g.pending.Has(txs[2].Hash()), "included tx[2] re-added on rollback")
}

// TestExecutorAppliesWritesToCacheBeforeCommit: the pipelined cycle applies the
// batch's merged writes to the cross-batch cache at SUBMIT time (so the next
// batch can read them before this one commits), and those entries survive
// until a commit notification's queued eviction is applied at the next batch
// boundary (DrainEvictions).
func TestExecutorAppliesWritesToCacheBeforeCommit(t *testing.T) {
	writes := map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")}
	stub := &stubEndorser{execResp: batchResponseWithWrites(t, writes)}
	g := newExecutorTestGateway(stub)
	addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done

	// Writes are in the cache before the batch commits.
	require.Equal(t, 2, g.cache.Len())
	rec, ok := g.cache.Read("k1")
	require.True(t, ok)
	require.Equal(t, []byte("v1"), rec.Value)

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: fabricTxID, Status: committerpb.Status_COMMITTED},
	}))

	// Commit only QUEUES the eviction; the entries are still present until the
	// next batch boundary applies it.
	require.Equal(t, 2, g.cache.Len())
	g.cache.DrainEvictions() // simulate the next cycle's batch boundary
	require.Equal(t, 0, g.cache.Len())
}

// TestExecutorPipelinesWithoutAwaitingCommit: the defining property of the
// pipelined executor -- a second cycle runs and submits while the first batch
// is still in flight (unresolved). Two successive cycles must leave TWO batches
// in flight simultaneously, neither having been committed.
func TestExecutorPipelinesWithoutAwaitingCommit(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)

	// Cycle 1.
	g.pending.Add(txWithNonce(1))
	end1, done1 := runCycleAndCapture(t, g)
	<-done1
	require.Equal(t, 1, g.inflightCount())

	// Cycle 2 runs without cycle 1 having been resolved.
	g.pending.Add(txWithNonce(2))
	end2, done2 := runCycleAndCapture(t, g)
	<-done2
	require.Equal(t, 2, g.inflightCount(), "second batch must be in flight alongside the first")

	// Distinct committer txs.
	id1, err := committerTxID(end1.Proposal)
	require.NoError(t, err)
	id2, err := committerTxID(end2.Proposal)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	// Resolve both.
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: id1, Status: committerpb.Status_COMMITTED},
		{FabricTxID: id2, Status: committerpb.Status_COMMITTED},
	}))
	require.Equal(t, 0, g.inflightCount())
}

// TestExecutorInflightWindowBlocks: the in-flight window is the pipeline's
// backpressure. With maxInflight=1, once one batch is in flight the next cycle
// must BLOCK before submitting until a slot frees; resolving the first batch
// releases the slot and lets the second submit.
func TestExecutorInflightWindowBlocks(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxInflight(1)

	// Cycle 1 takes the only slot.
	g.pending.Add(txWithNonce(1))
	end1, done1 := runCycleAndCapture(t, g)
	<-done1
	require.Equal(t, 1, g.inflightCount())

	// Cycle 2 starts but must block acquiring the slot: no submit yet.
	g.pending.Add(txWithNonce(2))
	done2 := make(chan struct{})
	go func() {
		g.executeCycle(context.Background())
		close(done2)
	}()

	select {
	case <-g.endorsementChan:
		t.Fatal("second cycle submitted while the in-flight window was full")
	case <-done2:
		t.Fatal("second cycle returned before a slot was available")
	case <-time.After(150 * time.Millisecond):
		// blocked acquiring the slot, as expected
	}

	// Free the slot by committing the first batch.
	id1, err := committerTxID(end1.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: id1, Status: committerpb.Status_COMMITTED},
	}))

	// The second cycle now acquires the freed slot and submits.
	select {
	case <-g.endorsementChan:
	case <-time.After(2 * time.Second):
		t.Fatal("second cycle did not submit after a slot was freed")
	}
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("second cycle did not return after submitting")
	}
	require.Equal(t, 1, g.inflightCount())
}

// TestGatewayHandleCommitsOnValidBlockTx: same drain-cycle setup as
// TestExecutorDrainCycle, but the commit outcome is delivered via Handle (the
// block-sync topology's blocks.BlockHandler path) instead of HandleTx (the
// notification topology). A block containing one Valid transaction whose ID
// matches the submitted committer TxID must resolve the in-flight batch as
// committed, exactly as the notification path would.
func TestGatewayHandleCommitsOnValidBlockTx(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done
	require.Equal(t, 1, g.inflightCount())

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NotEmpty(t, fabricTxID)

	require.NoError(t, g.Handle(context.Background(), blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{
			{ID: fabricTxID, Valid: true},
		},
	}))

	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 0, g.pending.Len())
	for _, tx := range txs {
		require.False(t, g.pending.Has(tx.Hash()))
	}
}

// TestGatewayHandleRollsBackOnInvalidBlockTx: mirrors
// TestExecutorRollbackOnInvalid via the block-sync Handle path -- a block
// reporting the committer tx as Invalid (tx.Valid == false) must roll the
// batch back, returning its 3 merged txs to pending for the next cycle.
func TestGatewayHandleRollsBackOnInvalidBlockTx(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	txs := addThreeTxs(g)

	end, done := runCycleAndCapture(t, g)
	<-done

	fabricTxID, err := committerTxID(end.Proposal)
	require.NoError(t, err)

	require.NoError(t, g.Handle(context.Background(), blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{
			{ID: fabricTxID, Valid: false},
		},
	}))

	require.Equal(t, 0, g.inflightCount())
	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()))
	}
}

// TestGatewayHandleIgnoresUnrelatedTx: a block containing a transaction whose
// ID doesn't match any in-flight batch (e.g. another gateway's batch, or a
// block delivered before/after the one carrying this batch's committer tx)
// must be silently ignored -- resolveInflight is a no-op for an unknown ID and
// must not disturb the genuinely in-flight batch. That batch is only resolved
// later, by its own timeout backstop.
func TestGatewayHandleIgnoresUnrelatedTx(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	// Short commitTimeout so the real (never-delivered) signal's backstop fires
	// quickly instead of leaking a 60s goroutine past the end of this test.
	g.commitTimeout = 50 * time.Millisecond
	txs := addThreeTxs(g)

	_, done := runCycleAndCapture(t, g)
	<-done
	require.Equal(t, 1, g.inflightCount())

	// Unrelated tx: ignored, the in-flight batch is untouched.
	require.NoError(t, g.Handle(context.Background(), blocks.Block{
		Number: 1,
		Transactions: []blocks.Transaction{
			{ID: "some-other-tx-id", Valid: true},
		},
	}))
	require.Equal(t, 1, g.inflightCount(), "unrelated tx must not resolve the in-flight batch")
	require.Equal(t, 0, g.pending.Len())

	// The backstop eventually rolls the batch back (never having been signaled).
	require.Eventually(t, func() bool {
		return g.inflightCount() == 0 && g.pending.Len() == 3
	}, 2*time.Second, 5*time.Millisecond)
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()))
	}
}
