/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"encoding/json"
	"errors"
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

// reservedLen returns how many pending txs are currently reserved
// (drained-but-not-yet-resolved by the pipelined executor). Test-only helper
// (same package) for asserting the reservation lifecycle: a batch is reserved
// from its DrainUpToReserved until the boundary Releases it (or Remove clears it
// at submit). A reservation that outlives a batch's handling is a leak that
// would make DrainUpToReserved skip those txs forever.
func (p *PendingPool) reservedLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.reserved)
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

// runOneBatchInFlight drives a single cycle for one seeded tx and returns the
// committer TxID of the batch it left in flight. Helper for the cascade tests,
// which need several distinct batches in flight with a known TxID each.
func runOneBatchInFlight(t *testing.T, g *Gateway) string {
	t.Helper()
	end, done := runCycleAndCapture(t, g)
	<-done
	id, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	return id
}

// containsAll reports whether got contains every element of want.
func containsAll(got, want []string) bool {
	set := make(map[string]struct{}, len(got))
	for _, g := range got {
		set[g] = struct{}{}
	}
	for _, w := range want {
		if _, ok := set[w]; !ok {
			return false
		}
	}
	return true
}

// TestExecutorCascadeInvalidatesSuffix (brief 1b): three batches in flight;
// invalidating the FIRST cascades all three -- every batch's included txs go
// back to pending, all three are NoteInvalidated (observed via the queued
// eviction set), and all three in-flight slots are released. This is the suffix
// cascade: a later batch may have read the invalidated batch's speculative
// writes, so the whole suffix must re-execute.
func TestExecutorCascadeInvalidatesSuffix(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxBatchSize(1) // one tx per batch, so three cycles => three batches
	txs := addThreeTxs(g)

	idA := runOneBatchInFlight(t, g)
	idB := runOneBatchInFlight(t, g)
	idC := runOneBatchInFlight(t, g)
	require.Equal(t, 3, g.inflightCount())
	require.Equal(t, 0, g.pending.Len())

	// Invalidate the FIRST (idA) -- must cascade the whole suffix {A,B,C}.
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: idA, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))

	// All three slots released, all three batches gone from the registry.
	require.Equal(t, 0, g.inflightCount())
	// All three batches' txs are back in pending.
	require.Equal(t, 3, g.pending.Len())
	for _, tx := range txs {
		require.True(t, g.pending.Has(tx.Hash()), "cascaded tx must return to pending")
	}
	// All three batches were NoteInvalidated (observe the queued eviction set at
	// a batch boundary).
	_, invalidated := g.cache.DrainEvictions()
	require.True(t, containsAll(invalidated, []string{idA, idB, idC}),
		"all three cascaded batches must be NoteInvalidated, got %v", invalidated)
}

// TestExecutorCascadeRebuildRepairsOverwrite is THE ⚠️ regression test. Two
// batches are in flight, BOTH writing key K (A writes va, B overwrites with vb),
// so the cache ends with K=vb attributed to B. Invalidating ONLY the later batch
// B drops B's cache entry -- which, under a naive suffix-drop, also erases K
// entirely even though the SURVIVING earlier batch A still holds a valid
// speculative write of K. The batch-boundary rebuild must restore K=va,
// re-attributed to A, at A's original spec version. Fails under a naive
// suffix-drop (K absent); passes once the boundary rebuilds from the survivors.
func TestExecutorCascadeRebuildRepairsOverwrite(t *testing.T) {
	stub := &stubEndorser{}
	g := newExecutorTestGateway(stub)

	// Batch A writes K=va.
	stub.execResp = batchResponseWithWrites(t, map[string][]byte{"K": []byte("va")})
	g.pending.Add(txWithNonce(1))
	idA := runOneBatchInFlight(t, g)

	// Batch B overwrites K=vb -> cache holds K=vb, writerTx=B, spec 1.
	stub.execResp = batchResponseWithWrites(t, map[string][]byte{"K": []byte("vb")})
	g.pending.Add(txWithNonce(2))
	idB := runOneBatchInFlight(t, g)

	require.Equal(t, 2, g.inflightCount())
	rec, ok := g.cache.Read("K")
	require.True(t, ok)
	require.Equal(t, []byte("vb"), rec.Value, "precondition: B's write is the latest")

	// Invalidate ONLY the later batch B (suffix = {B}); A survives.
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: idB, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))
	require.Equal(t, 1, g.inflightCount(), "A must survive; only B cascaded")

	// Hit the batch boundary: drain the queued invalidation, then rebuild the
	// cache from the surviving in-flight batches -- exactly what executeCycle
	// does at a cycle boundary when the drain reported an invalidation.
	_, invalidated := g.cache.DrainEvictions()
	require.Contains(t, invalidated, idB)
	g.rebuildCacheFromInflight()

	// A's speculative write of K must be RESTORED (not absent, not vb), attributed
	// back to A at A's original spec version (0: a first blind write of an absent
	// key, re-applied from empty).
	rec, ok = g.cache.Read("K")
	require.True(t, ok, "K erased by naive suffix-drop -- surviving batch A's write was lost")
	require.Equal(t, []byte("va"), rec.Value, "K must be A's va, not B's vb")
	require.Equal(t, uint64(0), rec.Version, "A's spec version must be reproduced exactly by the rebuild")
	e, ok := g.cache.readEntry("K")
	require.True(t, ok)
	require.Equal(t, idA, e.writerTx, "K must be re-attributed to surviving batch A")
	require.Equal(t, 1, g.cache.Len(), "only A's single key remains")
	require.NotEqual(t, idA, idB)
}

// TestExecutorCascadeRebuildClearsWhenAllWritersDepart is the mirror of the
// repair test: when the EARLIER batch A is invalidated the cascade suffix is
// {A,B} -- both writers of K depart -- so after the boundary rebuild from an
// empty survivor set K must be absent and the cache empty.
func TestExecutorCascadeRebuildClearsWhenAllWritersDepart(t *testing.T) {
	stub := &stubEndorser{}
	g := newExecutorTestGateway(stub)

	stub.execResp = batchResponseWithWrites(t, map[string][]byte{"K": []byte("va")})
	g.pending.Add(txWithNonce(1))
	idA := runOneBatchInFlight(t, g)

	stub.execResp = batchResponseWithWrites(t, map[string][]byte{"K": []byte("vb")})
	g.pending.Add(txWithNonce(2))
	_ = runOneBatchInFlight(t, g)
	require.Equal(t, 2, g.inflightCount())

	// Invalidate the EARLIER batch A -> cascade {A,B}: both writers of K gone.
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: idA, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))
	require.Equal(t, 0, g.inflightCount())

	_, invalidated := g.cache.DrainEvictions()
	require.NotEmpty(t, invalidated)
	g.rebuildCacheFromInflight()

	_, ok := g.cache.Read("K")
	require.False(t, ok, "K must be absent: both of its writers were invalidated")
	require.Equal(t, 0, g.cache.Len())
}

// TestExecutorCascadeRebuildRunsViaExecuteCycle proves the rebuild is actually
// wired into the executeCycle batch boundary (not only reachable by calling the
// helper directly). After invalidating the later of two K-writers, the next full
// executeCycle -- whose re-executed tx writes nothing that could clobber K --
// must, at its boundary, drain the invalidation and rebuild K=va from the
// surviving batch A.
func TestExecutorCascadeRebuildRunsViaExecuteCycle(t *testing.T) {
	stub := &stubEndorser{}
	g := newExecutorTestGateway(stub)

	stub.execResp = batchResponseWithWrites(t, map[string][]byte{"K": []byte("va")})
	g.pending.Add(txWithNonce(1))
	idA := runOneBatchInFlight(t, g)

	stub.execResp = batchResponseWithWrites(t, map[string][]byte{"K": []byte("vb")})
	g.pending.Add(txWithNonce(2))
	idB := runOneBatchInFlight(t, g)
	require.Equal(t, 2, g.inflightCount())

	// Invalidate only B; A survives, tx2 returns to pending, invalidation queued.
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: idB, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
	}))
	require.Equal(t, 1, g.inflightCount())

	// The next cycle re-executes the returned tx2 writing NOTHING (so it cannot
	// clobber K); its boundary must drain B's invalidation and rebuild K=va.
	stub.execResp = okBatchResponse()
	idC := runOneBatchInFlight(t, g)

	rec, ok := g.cache.Read("K")
	require.True(t, ok, "executeCycle's boundary did not rebuild the cache from survivors")
	require.Equal(t, []byte("va"), rec.Value)
	e, _ := g.cache.readEntry("K")
	require.Equal(t, idA, e.writerTx)

	// Stop the surviving batches' backstop timers by committing them (keeps the
	// test from leaking AfterFunc goroutines to the default 60s timeout).
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: idA, Status: committerpb.Status_COMMITTED},
		{FabricTxID: idC, Status: committerpb.Status_COMMITTED},
	}))
}

// TestExecutorConcurrentCascadeIsIdempotent (-race): two batches in flight; an
// MVCC-abort for batch A and both batches' short-timeout backstops race to
// cascade the same suffix. The registry-removal-under-lock is the idempotency
// gate: whichever caller removes a batch releases its slot exactly once, and any
// later cascade for an already-removed TxID is a no-op. Asserts no panic, the
// registry drains to empty, both txs are back in pending exactly once, and --
// the key check that no slot was double-released -- a subsequent cycle can still
// acquire a slot and submit.
func TestExecutorConcurrentCascadeIsIdempotent(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxBatchSize(1)
	g.SetMaxInflight(2)
	g.commitTimeout = 15 * time.Millisecond // arm short backstops that race the abort

	tx1, tx2 := txWithNonce(1), txWithNonce(2)
	g.pending.Add(tx1)
	g.pending.Add(tx2)
	idA := runOneBatchInFlight(t, g)
	_ = runOneBatchInFlight(t, g)
	require.Equal(t, 2, g.inflightCount())

	// Race an explicit MVCC-abort of A against the two backstops firing.
	go func() {
		_ = g.HandleTx(context.Background(), []common.TxNotification{
			{FabricTxID: idA, Status: committerpb.Status_ABORTED_MVCC_CONFLICT},
		})
	}()

	// The union of the racing cascades must settle to: nothing in flight, both
	// txs back in pending -- exactly once each.
	require.Eventually(t, func() bool {
		return g.inflightCount() == 0 && g.pending.Len() == 2
	}, 2*time.Second, 5*time.Millisecond)
	require.True(t, g.pending.Has(tx1.Hash()))
	require.True(t, g.pending.Has(tx2.Hash()))

	// No slot was double-released: a subsequent cycle can still acquire a slot and
	// submit. (A double `<-inflightSlots` would have left a goroutine blocked and
	// the semaphore corrupt.) Use a long timeout so this batch's own backstop
	// does not race the assertion.
	g.commitTimeout = commitTimeoutDefault
	end, done := runCycleAndCapture(t, g)
	<-done
	require.Equal(t, 1, g.inflightCount())
	// Drain the backstop for the batch we just submitted.
	id, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(context.Background(), []common.TxNotification{
		{FabricTxID: id, Status: committerpb.Status_COMMITTED},
	}))
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

// --- Pipelined executor (warm(N+1) || auth(N)) single-cycle tests ---------
//
// These drive drainAndWarm / pipelineIteration directly (deterministic, no real
// network) to prove the b-c-d-e-f-g ordering and, above all, the reservation
// lifecycle: every batch a pipeline iteration touches must end either submitted
// (its txs Removed) or released (its reservation cleared) -- never stuck
// reserved, which would make DrainUpToReserved skip those txs forever.

// TestPipelineHappyPathSubmitsAndPrefetchesNext: the defining overlap. With one
// tx per batch and two txs, drainAndWarm establishes the invariant (batch 1
// warmed + reserved); one pipelineIteration then auths+submits batch 1 WHILE
// warming batch 2, and carries batch 2 (warmed + still reserved) forward. A
// final iteration over batch 2 finds nothing left to prefetch, submits batch 2,
// and returns nil so the caller re-establishes the invariant.
func TestPipelineHappyPathSubmitsAndPrefetchesNext(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxBatchSize(1) // one tx per batch -> two batches from two txs
	tx1, tx2 := txWithNonce(1), txWithNonce(2)
	g.pending.Add(tx1)
	g.pending.Add(tx2)
	ctx := context.Background()

	// Establish the invariant: drain + warm batch 1 (tx1); tx1 is now reserved.
	warmed := g.drainAndWarm(ctx)
	require.NotNil(t, warmed)
	require.Equal(t, []*types.Transaction{tx1}, warmed.Txs())
	require.Equal(t, 1, g.pending.reservedLen(), "batch 1 reserved after drainAndWarm")

	// One overlapped iteration: auth+submit batch 1, warm batch 2, carry it.
	next := g.pipelineIteration(ctx, warmed)
	end1 := <-g.endorsementChan // batch 1 was submitted during the iteration

	require.NotNil(t, next)
	require.Equal(t, []*types.Transaction{tx2}, next.Txs(), "batch 2 (tx2) prefetched + carried")
	require.Equal(t, 1, g.inflightCount(), "batch 1 in flight")
	require.False(t, g.pending.Has(tx1.Hash()), "batch 1's tx removed at submit")
	require.True(t, g.pending.Has(tx2.Hash()), "batch 2's tx still pending (reserved)")
	require.Equal(t, 1, g.pending.reservedLen(), "only batch 2 reserved now (batch 1 removed)")

	// Commit batch 1.
	id1, err := committerTxID(end1.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(ctx, []common.TxNotification{
		{FabricTxID: id1, Status: committerpb.Status_COMMITTED},
	}))
	require.Equal(t, 0, g.inflightCount())

	// Final iteration over batch 2: the next drain is empty (tx2 is the only tx and
	// is reserved), so it submits batch 2 and returns nil.
	last := g.pipelineIteration(ctx, next)
	end2 := <-g.endorsementChan
	require.Nil(t, last, "no next batch to carry -> nil (caller re-establishes the invariant)")
	require.Equal(t, 1, g.inflightCount())
	require.False(t, g.pending.Has(tx2.Hash()), "batch 2's tx removed at submit")
	require.Equal(t, 0, g.pending.reservedLen(), "no reservation survives once batch 2 is submitted")

	id2, err := committerTxID(end2.Proposal)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2, "each batch is a distinct committer tx")
	require.NoError(t, g.HandleTx(ctx, []common.TxNotification{
		{FabricTxID: id2, Status: committerpb.Status_COMMITTED},
	}))
	require.Equal(t, 0, g.inflightCount())
}

// TestPipelineEmptyNextReturnsNil: with a single tx there is nothing to
// prefetch, so a pipelineIteration must NOT launch a warm pass, must still
// submit the batch it holds, and must return nil (no next batch). No
// reservation may survive.
func TestPipelineEmptyNextReturnsNil(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	tx1 := txWithNonce(1)
	g.pending.Add(tx1)
	ctx := context.Background()

	warmed := g.drainAndWarm(ctx)
	require.NotNil(t, warmed)
	require.Equal(t, 1, g.pending.reservedLen())

	next := g.pipelineIteration(ctx, warmed)
	end := <-g.endorsementChan
	require.Nil(t, next, "empty next drain -> nil")
	require.Equal(t, 1, g.inflightCount(), "the held batch is still submitted")
	require.False(t, g.pending.Has(tx1.Hash()), "batch removed at submit")
	require.Equal(t, 0, g.pending.reservedLen(), "batch's reservation cleared at submit")

	id, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(ctx, []common.TxNotification{
		{FabricTxID: id, Status: committerpb.Status_COMMITTED},
	}))
	require.Equal(t, 0, g.inflightCount())
}

// TestPipelineAuthErrorReleasesBatchAndClosesNext: when the authoritative pass
// of batch N fails (a non-OK batch response), the iteration must submit NOTHING,
// release BOTH batch N (so it re-drains) AND the batch N+1 it had already warmed
// (else its reservation leaks and its snapshot leaks), close the N+1 snapshot,
// and return nil. Both batches must be re-drawable afterwards.
func TestPipelineAuthErrorReleasesBatchAndClosesNext(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxBatchSize(1)
	tx1, tx2 := txWithNonce(1), txWithNonce(2)
	g.pending.Add(tx1)
	g.pending.Add(tx2)
	ctx := context.Background()

	// Warm batch 1 while the response is still OK (WarmBatch never reads execResp).
	warmed := g.drainAndWarm(ctx)
	require.NotNil(t, warmed)

	// Now make the authoritative pass fail: a non-OK status is an auth error
	// (mirrors TestAuthBatchNonOKStatusErrors). Set before pipelineIteration so
	// the concurrent warm(N+1) goroutine never races this write.
	stub.execResp = &peer.ProposalResponse{
		Response: &peer.Response{Status: common.StatusServerError, Message: "auth pass: backend gone"},
	}

	next := g.pipelineIteration(ctx, warmed)

	require.Nil(t, next, "auth error -> no next batch carried forward")
	select {
	case <-g.endorsementChan:
		t.Fatal("auth error must not submit a committer tx")
	default:
	}
	require.Equal(t, 0, g.inflightCount(), "nothing in flight after an auth error")
	require.True(t, g.pending.Has(tx1.Hash()), "batch N stays pending")
	require.True(t, g.pending.Has(tx2.Hash()), "prefetched batch N+1 stays pending")
	require.Equal(t, 0, g.pending.reservedLen(), "auth error released BOTH batch N and prefetched N+1")
	require.Equal(t, 2, stub.warmClosed,
		"batch N closed by AuthBatch (1) + prefetched N+1 closed explicitly (1)")
	require.Len(t, g.pending.DrainUpToReserved(0), 2, "both batches are re-drawable")
}

// TestPipelineWarmErrorStillSubmitsAndReleasesNext: when warming batch N+1 fails
// but batch N's authoritative pass succeeds, batch N must still be submitted
// (the warm failure only concerns the prefetch), the failed N+1 reservation must
// be released so its txs re-draw, and the iteration returns nil (no next batch).
func TestPipelineWarmErrorStillSubmitsAndReleasesNext(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxBatchSize(1)
	tx1, tx2 := txWithNonce(1), txWithNonce(2)
	g.pending.Add(tx1)
	g.pending.Add(tx2)
	ctx := context.Background()

	warmed := g.drainAndWarm(ctx) // batch 1 warmed OK
	require.NotNil(t, warmed)

	// Make the NEXT warm (batch 2) fail; batch 1's auth still succeeds (execResp OK).
	stub.warmErr = errors.New("open snapshot: db unavailable")

	next := g.pipelineIteration(ctx, warmed)
	end := <-g.endorsementChan // batch 1 still submitted

	require.Nil(t, next, "warm(N+1) error -> no next batch carried forward")
	require.Equal(t, 1, g.inflightCount(), "batch 1 submitted despite warm(N+1) failing")
	require.False(t, g.pending.Has(tx1.Hash()), "batch 1 removed at submit")
	require.True(t, g.pending.Has(tx2.Hash()), "batch 2 stays pending")
	require.Equal(t, 0, g.pending.reservedLen(), "batch 2's failed reservation released; batch 1 removed")
	require.Len(t, g.pending.DrainUpToReserved(0), 1, "only batch 2 remains, re-drawable")

	id, err := committerTxID(end.Proposal)
	require.NoError(t, err)
	require.NoError(t, g.HandleTx(ctx, []common.TxNotification{
		{FabricTxID: id, Status: committerpb.Status_COMMITTED},
	}))
	require.Equal(t, 0, g.inflightCount())
}

// TestPipelineDrainAndWarmEmptyWaitsForWork: on an empty pool, drainAndWarm must
// BLOCK in waitForWork (leaving the invariant unestablished) rather than
// busy-return nil in a tight loop; a ctx cancel unblocks it and it returns nil.
func TestPipelineDrainAndWarmEmptyWaitsForWork(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan *WarmedBatch, 1)
	go func() { done <- g.drainAndWarm(ctx) }()

	select {
	case <-done:
		t.Fatal("drainAndWarm returned on an empty pool instead of waiting for work")
	case <-time.After(150 * time.Millisecond):
		// still blocked in waitForWork, as expected
	}

	cancel() // shutdown unblocks waitForWork
	select {
	case w := <-done:
		require.Nil(t, w, "empty drain -> nil warmed batch")
	case <-time.After(2 * time.Second):
		t.Fatal("drainAndWarm did not return after ctx cancel")
	}
}

// TestRunExecutorPipelinedEndToEnd drives the whole pipelined loop with a
// background auto-committer, proving the loop drains a burst of txs to
// completion and shuts down cleanly on ctx cancel with NOTHING left reserved --
// the end-to-end reservation-lifecycle guarantee (every tx either committed or
// re-drawable, none stuck).
func TestRunExecutorPipelinedEndToEnd(t *testing.T) {
	stub := &stubEndorser{execResp: okBatchResponse()}
	g := newExecutorTestGateway(stub)
	g.SetMaxBatchSize(1) // one tx per batch -> many small batches through the loop
	ctx, cancel := context.WithCancel(context.Background())

	// Auto-commit every submitted batch so the in-flight window never wedges and
	// the pipeline keeps flowing. The reader touches only the endorsement it reads
	// off the channel (never the stub), so it never races the pipeline's goroutines.
	go func() {
		for {
			select {
			case end := <-g.endorsementChan:
				id, err := committerTxID(end.Proposal)
				if err != nil {
					continue
				}
				_ = g.HandleTx(context.Background(), []common.TxNotification{
					{FabricTxID: id, Status: committerpb.Status_COMMITTED},
				})
			case <-ctx.Done():
				return
			}
		}
	}()

	const n = 5
	txs := make([]*types.Transaction, n)
	for i := range n {
		txs[i] = txWithNonce(uint64(i + 1))
		g.pending.Add(txs[i])
	}

	loopDone := make(chan struct{})
	go func() { g.runExecutorPipelined(ctx); close(loopDone) }()

	// The whole burst drains and every batch commits: the system goes quiescent
	// (nothing pending, nothing in flight). Waiting for BOTH ensures no batch's
	// 60s backstop timer is still armed when we cancel.
	require.Eventually(t, func() bool {
		return g.pending.Len() == 0 && g.inflightCount() == 0
	}, 3*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runExecutorPipelined did not return after ctx cancel")
	}

	// Clean shutdown: no reservation may survive, and the pool is fully drained.
	require.Equal(t, 0, g.pending.Len(), "every tx committed")
	require.Equal(t, 0, g.pending.reservedLen(), "no reservation may survive shutdown")
	for _, tx := range txs {
		require.False(t, g.pending.Has(tx.Hash()))
	}
}
