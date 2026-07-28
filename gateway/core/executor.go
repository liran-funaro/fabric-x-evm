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

// commitTimeoutDefault is the per-batch stall backstop armed by trackInflight.
// It is deliberately far above any normal BFT commit latency: it is not a
// tuning parameter, it exists only so an in-flight slot can never be held
// forever if a commit/abort notification for a submitted committer tx is ever
// lost upstream (e.g. an earlier TxHandler panics in HandleBatch before the
// gateway's own HandleTx runs). On timeout, resolveInflight rolls the batch
// back exactly as an MVCC abort would: its txs return to pending and are
// re-drained next cycle. If the batch had in fact committed, its EVM txs are
// now nonce-too-low on re-submission and get excluded during re-execution --
// self-correcting. Task 6 replaces the blind rollback with a query-service
// status check.
const commitTimeoutDefault = 60 * time.Second

// failureBackoff is the fixed delay executeCycle waits after any error
// (endorse, TxID extraction, or submit failure) before returning, so a
// persistent endorser/orderer outage doesn't spin a tight, log-flooding retry
// loop. Never applied on the happy path.
const failureBackoff = 50 * time.Millisecond

// defaultMaxInflight bounds submitted-but-unconfirmed committer txs when the
// caller does not configure MaxInflight (see Gateway.SetMaxInflight).
const defaultMaxInflight = 16

// inflightBatch is one submitted-but-unconfirmed committer tx tracked in the
// pipeline. included are the EVM txs that committed in it -- removed from
// pending at submit time (so the next cycle does not re-drain them) and
// re-added on rollback; rws is the merged read-write set applied to the cache
// (retained so a cascade can rebuild the cache from surviving batches -- Task
// 5); timer is the per-batch backstop that rolls the batch back if no
// commit/abort notification arrives in time.
type inflightBatch struct {
	txID     string
	included []*types.Transaction
	rws      blocks.ReadWriteSet
	timer    *time.Timer
}

// runExecutor is the pipelined drain loop. Each cycle drains up to
// maxBatchSize txs, two-phase-executes + merges them into one committer tx,
// applies that tx's writes to the cross-batch cache, records it in-flight,
// submits it, and returns IMMEDIATELY -- it does NOT wait for the commit. The
// next cycle runs at once, executing against the cache (which now carries the
// prior in-flight batch's writes). Backpressure comes from the in-flight
// window (inflightSlots): once maxInflight batches are outstanding, the next
// acquire blocks until one is confirmed. Commit/abort outcomes are resolved
// asynchronously by resolveInflight (driven by HandleTx/Handle).
func (g *Gateway) runExecutor(ctx context.Context) {
	defer g.wg.Done()
	for ctx.Err() == nil {
		g.executeCycle(ctx)
	}
}

// executeCycle runs exactly one drain -> endorse -> apply-to-cache -> submit
// cycle and returns immediately WITHOUT waiting for the commit (the commit is
// resolved asynchronously by resolveInflight). If the pending pool is empty it
// blocks until either a new tx arrives (see SendTransaction's non-blocking
// signal on g.arrivals) or ctx is done, then returns having done nothing else.
// Factored out of runExecutor so a test can drive a single cycle
// deterministically without a real network.
func (g *Gateway) executeCycle(ctx context.Context) {
	// Batch boundary: apply queued commit/abort evictions to the cache before
	// executing, so the cache reflects confirmed outcomes exactly once per
	// cycle and never mutates mid-execution.
	g.cache.DrainEvictions()

	// DrainUpTo is non-destructive: included txs are removed below at submit
	// time (so the next cycle can't re-drain and double-submit them) while
	// retryable-excluded (nonce-too-high) txs stay pending to retry later.
	batch := g.pending.DrainUpTo(int(g.maxBatchSize.Load()))
	if len(batch) == 0 {
		g.waitForWork(ctx)
		return
	}

	end, included, terminal, rws, err := g.endorsers.ExecuteBatch(ctx, batch)
	if err != nil {
		logger.Errorf("batch endorse failed (%d txs): %v", len(batch), err)
		g.backoff(ctx) // txs stay pending; re-drained next cycle
		return
	}

	// Evict terminally-excluded txs (nonce too low, ...) unconditionally: the
	// exclusion reflects ledger state from before this cycle, so it holds
	// regardless of this batch's outcome; left pending they would leak.
	if len(terminal) > 0 {
		g.pending.Remove(hashesOf(terminal))
	}

	if len(included) == 0 {
		// Every drained tx was excluded (e.g. a lone nonce-gap tx with no
		// filler yet). Nothing to submit; wait for new work. A predecessor's
		// commit also signals arrivals (see resolveInflight), so a gap-filling
		// commit re-wakes us to retry the excluded txs.
		g.waitForWork(ctx)
		return
	}

	fabricTxID, err := committerTxID(end.Proposal)
	if err != nil {
		logger.Errorf("extract committer tx id (%d txs): %v", len(batch), err)
		g.backoff(ctx) // txs stay pending; re-drained next cycle
		return
	}

	// Apply this batch's writes to the cross-batch cache BEFORE submitting, so
	// the next cycle's endorsement reads them without waiting for the commit.
	g.cache.ApplyWrites(fabricTxID, rws)

	// Backpressure: block until an in-flight slot is free (resolveInflight
	// frees one on each commit/abort/timeout). On shutdown just return; the
	// cache is discarded with the gateway.
	select {
	case g.inflightSlots <- struct{}{}:
	case <-ctx.Done():
		return
	}

	// Record in-flight (with the timeout backstop) and remove the included txs
	// from pending BEFORE submit, so the next cycle cannot re-drain them and a
	// commit notification cannot race ahead of the registry entry.
	g.trackInflight(fabricTxID, included, rws)
	g.pending.Remove(hashesOf(included))

	if err := g.SubmitFabricTx(ctx, end); err != nil {
		logger.Errorf("batch submit failed (tx %s, %d txs): %v", fabricTxID, len(batch), err)
		g.resolveInflight(fabricTxID, false) // roll back: re-add included, evict, free the slot
		g.backoff(ctx)
		return
	}
	// No await-commit: return so the next cycle runs immediately. resolveInflight
	// (driven by HandleTx/Handle or the timeout) finalizes or rolls back later.
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

// trackInflight records a submitted batch in the in-flight registry (oldest
// first) and arms its timeout backstop. Called from executeCycle after a slot
// is acquired and before SubmitFabricTx, so a commit notification can never
// arrive before the entry exists.
func (g *Gateway) trackInflight(txID string, included []*types.Transaction, rws blocks.ReadWriteSet) {
	timeout := g.commitTimeout
	if timeout <= 0 {
		timeout = commitTimeoutDefault
	}
	b := &inflightBatch{txID: txID, included: included, rws: rws}
	// No commit/abort heard in time -> roll back, so a lost notification cannot
	// wedge the in-flight window. (Task 6 replaces this blind rollback with a
	// query-service status check.)
	b.timer = time.AfterFunc(timeout, func() { g.resolveInflight(txID, false) })
	g.inflightMu.Lock()
	g.inflight = append(g.inflight, b)
	g.inflightMu.Unlock()
}

// resolveInflight finalizes (committed) or rolls back (aborted / timed out) the
// in-flight batch for txID, then frees its in-flight slot. It is idempotent:
// the timeout timer and the commit/abort notification race to call it, and only
// the first -- the one that removes the registry entry -- does the work.
//
// On commit the batch's writes are confirmed (NoteCommitted queues their cache
// eviction for the next batch boundary) and its included txs, already removed
// from pending at submit, stay gone. On rollback the writes are invalidated
// (NoteInvalidated) and the included txs return to pending to retry.
//
// TODO(Task 5): a rollback must also CASCADE -- re-execute every LATER
// in-flight batch, since it may have read this batch's now-invalidated writes.
func (g *Gateway) resolveInflight(txID string, committed bool) {
	g.inflightMu.Lock()
	idx := -1
	for i, b := range g.inflight {
		if b.txID == txID {
			idx = i
			break
		}
	}
	if idx == -1 {
		g.inflightMu.Unlock()
		return // already resolved (timer/notification race) -- idempotent
	}
	b := g.inflight[idx]
	b.timer.Stop()
	g.inflight = append(g.inflight[:idx], g.inflight[idx+1:]...)
	g.inflightMu.Unlock()

	<-g.inflightSlots // free the in-flight slot

	if committed {
		g.cache.NoteCommitted(txID)
	} else {
		g.cache.NoteInvalidated(txID)
		for _, tx := range b.included {
			g.pending.Add(tx) // return to pending to retry
		}
	}

	// Wake the executor if idle: a commit may unblock retryable-excluded txs
	// whose predecessor just committed, and a rollback just re-queued work.
	// Non-blocking, mirroring AddPending.
	select {
	case g.arrivals <- struct{}{}:
	default:
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

// HandleTx implements common.TxHandler. It is the gateway's commit-outcome
// input in the notification-based topology: the AllTxBatchDispatcher delivers
// every committed transaction here, and for each notification whose FabricTxID
// matches an in-flight batch this executor submitted, resolveInflight
// finalizes (commit) or rolls back (any non-committed status) that batch.
// Notifications for unrecognized IDs (already resolved, or not a batch this
// executor submitted) are ignored by resolveInflight.
func (g *Gateway) HandleTx(_ context.Context, notifs []cmn.TxNotification) error {
	for _, n := range notifs {
		g.resolveInflight(n.FabricTxID, n.Status == committerpb.Status_COMMITTED)
	}
	return nil
}

// Handle implements blocks.BlockHandler. It is the gateway's commit-outcome
// input in the block-sync topology (no notification stream): the synchronizer
// delivers every committed block here, and each transaction whose Fabric TxID
// (b.Transactions[i].ID) matches an in-flight batch is finalized or rolled
// back via the same resolveInflight path HandleTx uses.
//
// This looks at the block's committer-level transactions directly rather
// than decoding embedded EVM sub-txs (contrast with ConvertToDomain): the
// in-flight registry is keyed by the Fabric TxID of the committer transaction
// that carried a whole merged batch, not by any individual EVM tx hash, so
// b.Transactions[i].ID is exactly the correlation key it needs regardless of
// how many EVM sub-txs that committer tx carried.
//
// A deployment wires at most one of Handle/HandleTx per topology (see
// gateway/app.buildApp for block-sync, integration/test_helpers.go for
// notification-based harnesses), but registering both is harmless:
// resolveInflight is idempotent and a no-op for an already-resolved ID.
func (g *Gateway) Handle(_ context.Context, b blocks.Block) error {
	for _, tx := range b.Transactions {
		g.resolveInflight(tx.ID, tx.Valid)
	}
	return nil
}

// hashesOf returns the hashes of a batch of transactions, in order.
func hashesOf(txs []*types.Transaction) []ethcommon.Hash {
	out := make([]ethcommon.Hash, len(txs))
	for i, tx := range txs {
		out[i] = tx.Hash()
	}
	return out
}
