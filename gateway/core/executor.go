/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"fmt"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	cmn "github.com/hyperledger/fabric-x-evm/common"
)

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
	batch := g.pending.DrainAll()
	if len(batch) == 0 {
		select {
		case <-ctx.Done():
		case <-g.arrivals: // signaled by SendTransaction when the pool was empty
		}
		return
	}

	end, err := g.endorsers.ExecuteBatch(ctx, batch)
	if err != nil {
		logger.Errorf("batch endorse failed (%d txs): %v", len(batch), err)
		return // txs stay pending; re-drained next cycle
	}

	fabricTxID, err := committerTxID(end.Proposal)
	if err != nil {
		logger.Errorf("extract committer tx id (%d txs): %v", len(batch), err)
		return // txs stay pending; re-drained next cycle
	}

	// Register the waiter *before* submitting: otherwise a fast commit
	// notification could arrive before we start listening for it.
	waitCh := g.registerCommitWaiter(fabricTxID)

	if err := g.SubmitFabricTx(ctx, end); err != nil {
		g.forgetCommitWaiter(fabricTxID)
		logger.Errorf("batch submit failed (tx %s, %d txs): %v", fabricTxID, len(batch), err)
		return // txs stay pending; re-drained next cycle
	}

	if err := g.awaitCommit(ctx, fabricTxID, waitCh); err != nil {
		logger.Errorf("batch %s did not commit (%d txs): %v", fabricTxID, len(batch), err)
		return // rollback: txs stay pending; re-drained next cycle
	}

	g.pending.Remove(hashesOf(batch)) // refine: remove only INCLUDED txs (Task 8)
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

// awaitCommit blocks until fabricTxID's outcome is delivered via HandleTx, or
// ctx is done, then removes the waiter. It returns nil only when the
// committer tx committed valid; any other outcome (MVCC abort or any other
// non-committed status, or ctx cancellation) is an error, and the caller must
// leave the batch's txs pending so the next cycle re-drains and retries them
// (rollback).
func (g *Gateway) awaitCommit(ctx context.Context, fabricTxID string, ch chan committerpb.Status) error {
	defer g.forgetCommitWaiter(fabricTxID)
	select {
	case status := <-ch:
		if status != committerpb.Status_COMMITTED {
			return fmt.Errorf("committer tx %s not committed: status=%s", fabricTxID, status)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HandleTx implements common.TxHandler. It is the gateway's half of the
// wait-for-commit signal: the AllTxBatchDispatcher delivers every committed
// transaction here, and for each notification whose FabricTxID matches a
// waiter currently registered by executeCycle/awaitCommit, it hands off the
// outcome status. Notifications for unrecognized IDs (already handled,
// abandoned, or simply not a batch this executor submitted) are ignored.
func (g *Gateway) HandleTx(_ context.Context, notifs []cmn.TxNotification) error {
	g.commitMu.Lock()
	defer g.commitMu.Unlock()
	for _, n := range notifs {
		ch, ok := g.commitWaiters[n.FabricTxID]
		if !ok {
			continue
		}
		select {
		case ch <- n.Status:
		default:
			// Buffered size 1; a second send would mean the waiter already
			// has a status pending. Shouldn't happen for a one-shot commit
			// ID, but never block the notification dispatcher on it.
		}
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
