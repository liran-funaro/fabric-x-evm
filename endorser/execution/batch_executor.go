/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// ExecuteBatch runs an ordered batch of transactions under a single view.
//
//   - len(txs)==1: one pass, sharing runOn/newState with Execute so the two are
//     identical.
//   - len(txs)>1: a warm pass runs each tx concurrently against the shared
//     snapshot to fill any caching reader's read cache (results discarded), then
//     an authoritative pass re-runs them in order against an overlay of that
//     snapshot, applying each tx's write-set before running the next so a later
//     tx observes earlier writes.
//
// In the len(txs)>1 authoritative pass, a tx that runOn rejects before
// execution (nonce gap, bad signature, insufficient funds, ...) is EXCLUDED
// rather than aborting the batch: it gets a sentinel result (Status
// common.StatusTxRejected, empty RWS) in its slot and the rest of the batch
// still runs. This always returns exactly len(txs) results (one slot per
// input tx, index = sub-index) unless a genuine server-side fault occurs, in
// which case it still returns an error as before.
func (e *EVMEngine) ExecuteBatch(ctx context.Context, txs []*types.Transaction) ([]endorsement.ExecutionResult, error) {
	if len(txs) == 0 {
		return nil, nil
	}

	// One snapshot (one view) for the whole batch: every tx, in both passes,
	// simulates against the same consistent point-in-time state.
	reader, err := e.kvs.NewSnapshot(0)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	if len(txs) == 1 {
		var res endorsement.ExecutionResult
		var err error
		if e.stateDecorator == nil && !e.evmConfig.DebugLogs {
			res, err = e.executeReusing(reader, txs[0])
		} else {
			var state ExtendedStateDB
			if state, err = e.newState(reader); err == nil {
				res, err = e.runOn(state, txs[0])
			}
		}
		if err != nil {
			if rej, ok := errors.AsType[*TxRejected](err); ok {
				// Excluded, not aborted: align with the len(txs)>1 path below
				// so a single pending tx (e.g. a lone nonce-gap tx with no
				// batchmate yet) doesn't hard-fail the whole cycle just
				// because it happens to be alone.
				return []endorsement.ExecutionResult{excludedResult(rej)}, nil
			}
			return nil, err
		}
		return []endorsement.ExecutionResult{res}, nil
	}

	// The fast path (production: no per-tx decorator, no debug logging) reuses
	// one StateDB+Executor per warm-pass worker and one for the whole
	// authoritative pass, resetting the StateDB in place between txs, so the
	// per-tx machinery (StateDB maps, access list, block context, signer, EVM)
	// is built once instead of per tx. The slow path (a decorator or debug
	// logging wraps each StateDB) keeps building fresh per-tx state via newState.
	fast := e.stateDecorator == nil && !e.evmConfig.DebugLogs

	// Warm pass: run every tx against the shared snapshot to warm any caching
	// reader (e.g. the query-service view backing `reader`) before the sequential
	// pass below, which is what surfaces real results and errors. A tx that would
	// fail here (e.g. against pre-batch state) is not a bug.
	//
	// Concurrency = batch size (design RQ1: "one goroutine per transaction in the
	// batch"). The warm pass exists to fill the query-service view's read cache, and
	// each warm read BLOCKS on a gRPC round-trip to the query service -- it is
	// I/O-bound, not CPU-bound, so the worker count is NOT tied to GOMAXPROCS. The
	// query service coalesces concurrent single-key reads into one DB call
	// (min-batch-keys / max-batch-wait); firing all of a batch's reads at once fills
	// that window, instead of leaving the server's max-batch-wait exposed on every
	// small wave. Blocked workers are parked on I/O (not busy-waiting), so a large
	// count is cheap -- the Go scheduler handles it fine.
	//
	// A work-stealing atomic-index pool -- not a raw goroutine-per-tx spawn -- lets a
	// free worker pick up a slow worker's remaining txs; with the default count ==
	// len(txs) it is effectively one worker per tx. Each worker owns its per-tx state
	// so concurrent execution never shares a journal. An explicit WarmWorkers override
	// caps concurrency for a fast, non-blocking backend (e.g. an in-memory KVS) where
	// unbounded warm goroutines would add scheduler churn with no I/O to overlap.
	warmWorkers := e.evmConfig.WarmWorkers
	if warmWorkers <= 0 || warmWorkers > len(txs) {
		warmWorkers = len(txs)
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range warmWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Fast path: one reused StateDB+Executor for this worker, reset per
			// tx. Each worker owns its own, so concurrent warming never shares
			// mutable state (verified under -race).
			var sdb *StateDB
			var ex *Executor
			if fast {
				var err error
				if sdb, ex, err = e.newReusableExecutor(reader); err != nil {
					return
				}
			}
			for {
				i := int(next.Add(1)) - 1
				if i >= len(txs) {
					return
				}
				// StateDB accessors record a read error rather than panic now,
				// but recover defensively: a warm-pass failure must never crash
				// the endorser -- the authoritative pass re-runs and surfaces it.
				func(tx *types.Transaction) {
					defer func() { _ = recover() }()
					if fast {
						sdb.reset(reader)
						_, _ = e.classify(ex, sdb, tx) // warm only; ignore result/error.
						return
					}
					if s, err := e.newState(reader); err == nil {
						_, _ = e.runOn(s, tx) // warm only; ignore result/error.
					}
				}(txs[i])
			}
		}()
	}
	wg.Wait()

	// Authoritative pass: sequential, each tx against the snapshot plus an
	// overlay carrying every earlier tx's writes from this batch.
	overlay := &overlayReader{under: reader, writes: map[string]*blocks.WriteRecord{}}
	out := make([]endorsement.ExecutionResult, 0, len(txs))
	// Fast path: one reused StateDB+Executor for the whole (serial) pass, reset
	// against the overlay before each tx.
	var authSdb *StateDB
	var authEx *Executor
	if fast {
		var err error
		if authSdb, authEx, err = e.newReusableExecutor(overlay); err != nil {
			return nil, err
		}
	}
	for _, tx := range txs {
		var res endorsement.ExecutionResult
		var err error
		if fast {
			authSdb.reset(overlay)
			res, err = e.classify(authEx, authSdb, tx)
		} else {
			var state ExtendedStateDB
			if state, err = e.newState(overlay); err == nil {
				res, err = e.runOn(state, tx)
			}
		}
		if err != nil {
			if rej, ok := errors.AsType[*TxRejected](err); ok {
				// Excluded, not aborted: a client-rejected tx (nonce gap, bad
				// signature, insufficient funds, ...) can never be included as
				// it stands, but the rest of the batch must still make
				// progress. Record a sentinel outcome with an empty RWS --
				// MergeResults folds it in as a no-op -- and continue without
				// applying anything to the overlay. The caller (chain.go's
				// block parser) recognizes this status and skips it entirely:
				// no domain tx, no committed write. A RETRYABLE exclusion
				// (nonce too high, insufficient funds, ...) stays pending and
				// is retried once its gap is filled; a TERMINAL exclusion
				// (nonce too low) can never resolve as this exact tx and the
				// caller should evict it instead (see excludedResult).
				out = append(out, excludedResult(rej))
				continue
			}
			// A genuine server-side fault (not a client rejection): still
			// abort the whole batch, as before.
			return nil, err
		}
		overlay.apply(res.RWS)
		out = append(out, res)
	}
	return out, nil
}

// excludedResult builds the sentinel outcome for a tx that runOn rejected
// before execution (see the len(txs)==1 and len(txs)>1 cases above): never
// executed, so its RWS/Event stay empty. rej's underlying error further
// classifies the exclusion as:
//   - TERMINAL (common.StatusTxRejectedTerminal): nonce too low. This exact
//     tx can never succeed as submitted -- either an earlier tx with the
//     same nonce already landed, or this one did -- so the caller should
//     evict it from the pending pool rather than retry it forever.
//   - RETRYABLE (common.StatusTxRejected), the default: nonce too high,
//     insufficient funds, bad signature, .... It may still succeed once
//     ledger state catches up (e.g. its predecessor commits), so the caller
//     must leave it pending.
func excludedResult(rej *TxRejected) endorsement.ExecutionResult {
	status := common.StatusTxRejected
	if errors.Is(rej, core.ErrNonceTooLow) {
		status = common.StatusTxRejectedTerminal
	}
	return endorsement.ExecutionResult{
		Status:  status,
		Message: rej.Error(),
	}
}

// newReusableExecutor builds a bare *StateDB and an *Executor (with a primed,
// reusable EVM) bound to it, for the batch fast path. The pair is reused across
// many txs: reset the StateDB in place (which the Executor and EVM both point
// at) between txs. Only valid when no decorator/debug wrapping is in play (the
// caller gates on `fast`), so the concrete *StateDB -- required by reset -- is
// exactly what the Executor holds.
func (e *EVMEngine) newReusableExecutor(store ReadStore) (*StateDB, *Executor, error) {
	sdb, err := NewStateDB(context.TODO(), store, e.namespace, 0, e.monotonicVersions)
	if err != nil {
		return nil, nil, err
	}
	ex, err := NewExecutor(sdb, noopCloser{}, nil, e.evmConfig)
	if err != nil {
		return nil, nil, err
	}
	ex.primeEVM()
	return sdb, ex, nil
}

// getExec returns a reusableExec ready to run against store: a recycled one from
// the pool (reset in place) or a freshly built one. Fast path only.
func (e *EVMEngine) getExec(store ReadStore) (*reusableExec, error) {
	if v := e.execPool.Get(); v != nil {
		re := v.(*reusableExec)
		re.sdb.reset(store)
		return re, nil
	}
	sdb, ex, err := e.newReusableExecutor(store)
	if err != nil {
		return nil, err
	}
	return &reusableExec{sdb: sdb, ex: ex}, nil
}

// putExec returns re to the pool for reuse by a later single-tx execution.
func (e *EVMEngine) putExec(re *reusableExec) { e.execPool.Put(re) }

// executeReusing runs one tx on a pooled, reused StateDB+Executor and returns
// the raw classification, exactly as runOn would over a fresh newState(reader)
// -- minus the production-nil decorator. Used by the single-tx fast path
// (Execute and ExecuteBatch's N==1 case).
func (e *EVMEngine) executeReusing(reader ReadStore, tx *types.Transaction) (endorsement.ExecutionResult, error) {
	re, err := e.getExec(reader)
	if err != nil {
		return endorsement.ExecutionResult{}, err
	}
	defer e.putExec(re)
	return e.classify(re.ex, re.sdb, tx)
}

// newState builds an ExtendedStateDB over reader, wrapping it with StateDBLogger
// when debug logging is enabled. It mirrors the StateDB construction inside
// newExecutor minus the snapshot lifecycle: the caller owns reader (it may be a
// fresh per-tx snapshot, as in newExecutor/Execute, or a shared batch reader).
func (e *EVMEngine) newState(reader ReadStore) (ExtendedStateDB, error) {
	stateDB, err := NewStateDB(context.TODO(), reader, e.namespace, 0, e.monotonicVersions)
	if err != nil {
		return nil, err
	}
	if e.evmConfig.DebugLogs {
		return NewStateDBLogger(stateDB), nil
	}
	return stateDB, nil
}

// overlayReader layers an in-memory, batch-local write set over an underlying
// snapshot ReadStore, so the authoritative pass sees earlier transactions'
// writes in this batch as if they were already committed.
type overlayReader struct {
	under  ReadStore
	mu     sync.Mutex
	writes map[string]*blocks.WriteRecord
}

// Get returns the value visible to the authoritative pass for (ns, key): the
// latest in-batch write if one exists, else the underlying snapshot's value.
//
// The record's version metadata always comes from the underlying snapshot,
// never fabricated from the in-batch write (a blocks.KVWrite carries no version
// at all). The whole batch is validated for MVCC at commit time against the
// ledger state this snapshot was taken from, so a later tx's read dependency on
// an in-batch write must still be checked against that same pre-batch version —
// not a synthetic intra-batch one — or MVCC read-versions would stop being
// view-consistent.
//
// A write to a key that is absent from the underlying snapshot (under == nil)
// is reported back with IsDelete forced true, so the read is recorded with a
// nil MVCC version (i.e. "absent"), while Value still carries the in-batch
// write. This reuses the existing convention StateDB relies on for
// snapshot-revert: see getStateFromStore's "IsDelete: true with non-nil Value"
// case in statedb.go.
func (o *overlayReader) Get(ns, key string) (*blocks.WriteRecord, error) {
	under, err := o.under.Get(ns, key)
	if err != nil {
		return nil, err
	}

	o.mu.Lock()
	rec, overlaid := o.writes[key]
	o.mu.Unlock()
	if !overlaid {
		return under, nil
	}

	result := &blocks.WriteRecord{Value: rec.Value, IsDelete: rec.IsDelete || under == nil}
	if under != nil {
		result.BlockNum = under.BlockNum
		result.TxNum = under.TxNum
		result.Version = under.Version
	}
	return result, nil
}

// Close is a no-op: the underlying snapshot's lifecycle is owned by the caller
// of ExecuteBatch (via the `reader` it wraps), not by the overlay itself.
func (o *overlayReader) Close() error { return nil }

// apply records tx's write-set so later Get calls in this batch observe it.
func (o *overlayReader) apply(rws blocks.ReadWriteSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, w := range rws.Writes {
		o.writes[w.Key] = &blocks.WriteRecord{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete}
	}
}

var _ ReadStore = (*overlayReader)(nil)
