/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"fmt"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	cmn "github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
)

// commitTimeoutDefault is the stall backstop applied by awaitCommit. It is
// deliberately far above any normal BFT commit latency: it is not a tuning
// parameter, it exists only so the single executor goroutine can never block
// forever if a commit/abort notification for a submitted committer tx is
// ever lost upstream (e.g. an earlier TxHandler panics in HandleBatch before
// the gateway's own HandleTx runs). On timeout, awaitCommit returns an error
// exactly like an MVCC abort would: the batch's txs are left pending and
// re-drained next cycle. If the batch had in fact committed, its EVM txs are
// now nonce-too-low on re-submission and get excluded during re-execution --
// self-correcting.
const commitTimeoutDefault = 60 * time.Second

// failureBackoff is the fixed delay executeCycle waits after any error
// (endorse, TxID extraction, submit, or await-commit failure) before
// returning, so a persistent endorser/orderer outage -- or a string of
// commit-wait timeouts -- doesn't spin a tight, log-flooding retry loop.
// Never applied on the happy path.
const failureBackoff = 50 * time.Millisecond

// runExecutor is the drain-all loop: one batch in flight at a time. Each cycle
// drains the whole pending pool, two-phase-executes + merges it into one
// Fabric tx, submits it, waits for it to commit, and removes the committed
// txs on success. No worker pool, no dependency ordering; the query service
// paces reads, so there is no in-flight cap here either. (Stage 2.1:
// wait-for-commit; the overlay + wait-for-ordered pipeline is stage 2.3.)
func (g *Gateway) runExecutor(ctx context.Context) {
	defer g.wg.Done()
	for ctx.Err() == nil {
		g.executeCycle(ctx)
	}
}

// executeCycle runs exactly one drain -> endorse -> submit -> await-commit
// cycle. If the pending pool is empty it blocks until either a new tx arrives
// (see SendTransaction's non-blocking signal on g.arrivals) or ctx is done,
// then returns having done nothing else. Factored out of runExecutor so a
// test can drive a single cycle deterministically without a real network.
func (g *Gateway) executeCycle(ctx context.Context) {
	// Drain up to maxBatchSize txs (all of them when unbounded). Any remainder
	// stays pending and is picked up by the next cycle, which runs immediately
	// after this batch commits -- so a submission burst is pipelined across
	// several right-sized batches instead of one oversized Fabric tx.
	batch := g.pending.DrainUpTo(int(g.maxBatchSize.Load()))
	if len(batch) == 0 {
		g.waitForWork(ctx)
		return
	}

	end, included, terminal, err := g.endorsers.ExecuteBatch(ctx, batch)
	if err != nil {
		logger.Errorf("batch endorse failed (%d txs): %v", len(batch), err)
		g.backoff(ctx) // txs stay pending; re-drained next cycle
		return
	}

	// Evict terminally-excluded txs (nonce too low, ...) unconditionally: the
	// exclusion reflects ledger state from BEFORE this cycle's batch ran, so
	// it holds no matter what happens to the rest of this batch below (commit,
	// abort, or timeout). Left in the pool, a nonce-too-low tx could never
	// succeed as submitted and would leak forever (see
	// EndorsementClient.ExecuteBatch / classifyBatchOutcomes).
	if len(terminal) > 0 {
		g.pending.Remove(hashesOf(terminal))
	}

	if len(included) == 0 {
		// Every drained tx was excluded (e.g. a lone nonce-gap tx with no
		// filler yet present). There is nothing to submit this cycle:
		// submitting an empty committer tx would waste a Fabric round-trip,
		// and looping back to DrainAll immediately (with no backoff) would
		// busy-spin since the same excluded tx(s) would just be drained again.
		// Wait for genuinely new work instead, exactly as the empty-pool case
		// above does.
		g.waitForWork(ctx)
		return
	}

	fabricTxID, err := committerTxID(end.Proposal)
	if err != nil {
		logger.Errorf("extract committer tx id (%d txs): %v", len(batch), err)
		g.backoff(ctx) // txs stay pending; re-drained next cycle
		return
	}

	// Register the waiter *before* submitting: otherwise a fast commit
	// notification could arrive before we start listening for it.
	waitCh := g.registerCommitWaiter(fabricTxID)

	if err := g.SubmitFabricTx(ctx, end); err != nil {
		g.forgetCommitWaiter(fabricTxID)
		logger.Errorf("batch submit failed (tx %s, %d txs): %v", fabricTxID, len(batch), err)
		g.backoff(ctx) // txs stay pending; re-drained next cycle
		return
	}

	if err := g.awaitCommit(ctx, fabricTxID, waitCh); err != nil {
		logger.Errorf("batch %s did not commit (%d txs): %v", fabricTxID, len(batch), err)
		g.backoff(ctx) // rollback: txs stay pending; re-drained next cycle
		return
	}

	// Remove only the INCLUDED txs: an excluded one (nonce gap, rejected, ...)
	// was never committed and stays pending, so a future cycle re-drains and
	// retries it once its predecessor fills the gap.
	g.pending.Remove(hashesOf(included))
}

// committerTxID recovers the Fabric TxID of the committer transaction that
// will carry this batch, so the executor can later correlate the commit/abort
// notification (delivered by FabricTxID; see HandleTx) back to this
// submission. sdk.Endorsement only carries the raw *peer.Proposal — the
// endorsement.Invocation that createInvocation built (and which had a TxID
// field ready-made) is not itself returned — so we recover the TxID the same
// way the committer/notification path does: from the proposal's channel
// header (see endorsement.NewInvocation, which sets the channel header's TxId
// to the same value it returns as Invocation.TxID).
func committerTxID(prop *peer.Proposal) (string, error) {
	hdr, err := protoutil.UnmarshalHeader(prop.Header)
	if err != nil {
		return "", fmt.Errorf("unmarshal proposal header: %w", err)
	}
	chdr, err := protoutil.UnmarshalChannelHeader(hdr.ChannelHeader)
	if err != nil {
		return "", fmt.Errorf("unmarshal channel header: %w", err)
	}
	return chdr.TxId, nil
}

// registerCommitWaiter creates and stores a one-shot channel for fabricTxID's
// eventual commit/abort outcome. Must be called before the corresponding
// SubmitFabricTx (see executeCycle) so a fast notification can never race
// ahead of our subscription.
func (g *Gateway) registerCommitWaiter(fabricTxID string) chan committerpb.Status {
	ch := make(chan committerpb.Status, 1)
	g.commitMu.Lock()
	g.commitWaiters[fabricTxID] = ch
	g.commitMu.Unlock()
	return ch
}

// forgetCommitWaiter removes a registered waiter without waiting on it. Used
// when submission itself fails, so nothing will ever signal the waiter.
func (g *Gateway) forgetCommitWaiter(fabricTxID string) {
	g.commitMu.Lock()
	delete(g.commitWaiters, fabricTxID)
	g.commitMu.Unlock()
}

// awaitCommit blocks until fabricTxID's outcome is delivered via HandleTx,
// ctx is done, or the per-batch commit timeout (see commitTimeoutDefault)
// elapses, then removes the waiter. It returns nil only when the committer
// tx committed valid; any other outcome (MVCC abort, any other
// non-committed status, ctx cancellation, or a timed-out wait) is an error,
// and the caller must leave the batch's txs pending so the next cycle
// re-drains and retries them (rollback).
func (g *Gateway) awaitCommit(ctx context.Context, fabricTxID string, ch chan committerpb.Status) error {
	defer g.forgetCommitWaiter(fabricTxID)

	timeout := g.commitTimeout
	if timeout <= 0 {
		timeout = commitTimeoutDefault
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case status := <-ch:
		if status != committerpb.Status_COMMITTED {
			return fmt.Errorf("committer tx %s not committed: status=%s", fabricTxID, status)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("await commit of tx %s: %w", fabricTxID, ctx.Err())
	}
}

// waitForWork blocks until either ctx is done or new work arrives (see
// AddPending's non-blocking signal on g.arrivals). Used both when the pending
// pool is empty and when a drained batch's every tx was excluded: in both
// cases there is nothing usable to submit this cycle, and looping back to
// DrainAll immediately would busy-spin/log-flood for no reason.
func (g *Gateway) waitForWork(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-g.arrivals:
	}
}

// backoff pauses briefly after an executeCycle error (endorse, TxID
// extraction, submit, or await-commit failure) so a persistent
// endorser/orderer outage -- or repeated commit-wait timeouts -- doesn't spin
// a tight, log-flooding retry loop. It never delays the happy path, and
// returns early if ctx is done.
func (g *Gateway) backoff(ctx context.Context) {
	select {
	case <-time.After(failureBackoff):
	case <-ctx.Done():
	}
}

// HandleTx implements common.TxHandler. It is the gateway's half of the
// wait-for-commit signal in the notification-based topology: the
// AllTxBatchDispatcher delivers every committed transaction here, and for
// each notification whose FabricTxID matches a waiter currently registered
// by executeCycle/awaitCommit, it hands off the outcome status. Notifications
// for unrecognized IDs (already handled, abandoned, or simply not a batch
// this executor submitted) are ignored.
func (g *Gateway) HandleTx(_ context.Context, notifs []cmn.TxNotification) error {
	for _, n := range notifs {
		g.signalCommitOutcome(n.FabricTxID, n.Status)
	}
	return nil
}

// Handle implements blocks.BlockHandler. It is the gateway's half of the
// wait-for-commit signal in the block-sync topology (no notification
// stream): the synchronizer delivers every committed block here, and for
// each transaction in it whose Fabric TxID (b.Transactions[i].ID) matches a
// waiter currently registered by executeCycle/awaitCommit, it hands off the
// commit/abort outcome via the same signalCommitOutcome path HandleTx uses.
//
// This looks at the block's committer-level transactions directly rather
// than decoding embedded EVM sub-txs (contrast with ConvertToDomain): the
// executor's waiter is keyed by the Fabric TxID of the committer transaction
// that carried a whole merged batch, not by any individual EVM tx hash, so
// b.Transactions[i].ID is exactly the correlation key it needs regardless of
// how many EVM sub-txs that committer tx carried.
//
// A deployment wires at most one of Handle/HandleTx per topology (see
// gateway/app.buildApp for block-sync, integration/test_helpers.go for
// notification-based harnesses), but registering both is harmless: each
// waiter is one-shot and removed after its first signal.
func (g *Gateway) Handle(_ context.Context, b blocks.Block) error {
	for _, tx := range b.Transactions {
		status := committerpb.Status_ABORTED_MVCC_CONFLICT
		if tx.Valid {
			status = committerpb.Status_COMMITTED
		}
		g.signalCommitOutcome(tx.ID, status)
	}
	return nil
}

// signalCommitOutcome delivers status to the waiter registered for
// fabricTxID, if any is currently registered. Shared by HandleTx
// (notification-based topology) and Handle (block-sync topology).
func (g *Gateway) signalCommitOutcome(fabricTxID string, status committerpb.Status) {
	g.commitMu.Lock()
	defer g.commitMu.Unlock()
	ch, ok := g.commitWaiters[fabricTxID]
	if !ok {
		return
	}
	select {
	case ch <- status:
	default:
		// Buffered size 1; a second send would mean the waiter already
		// has a status pending. Shouldn't happen for a one-shot commit
		// ID, but never block the notification dispatcher/synchronizer on it.
	}
}

// hashesOf returns the hashes of a batch of transactions, in order.
func hashesOf(txs []*types.Transaction) []ethcommon.Hash {
	out := make([]ethcommon.Hash, len(txs))
	for i, tx := range txs {
		out[i] = tx.Hash()
	}
	return out
}
