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
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"go.uber.org/zap/zapcore"
)

// batchLogger carries the per-batch endorse-phase timing (ENDORSE-TIMING lines).
// It is diagnostic instrumentation for the two-phase execution bottleneck hunt:
// which phase (concurrent warm vs serial authoritative) dominates, and whether
// that phase's cost is backend read I/O or CPU. The timing is emitted at DEBUG
// and gated on IsEnabledFor(debug): at the default info level ExecuteBatch does
// not wrap the view at all, so production pays nothing (no per-read atomics, no
// formatting). Enable with `logging.logSpec: evm.batch=debug`.
var batchLogger = flogging.MustGetLogger("evm.batch")

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

	if len(txs) == 1 {
		// One snapshot (one view) for the single tx. This path shares
		// runOn/newState with Execute so the two are identical. (The len>1 path
		// opens its snapshot inside WarmBatch, so it can be carried across the
		// warm/auth split for the pipelined loop.)
		reader, err := e.kvs.NewSnapshot(0)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		var res endorsement.ExecutionResult
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
				// Excluded, not aborted: align with the len(txs)>1 path so a
				// single pending tx (e.g. a lone nonce-gap tx with no batchmate
				// yet) doesn't hard-fail the whole cycle just because it is alone.
				return []endorsement.ExecutionResult{excludedResult(rej)}, nil
			}
			return nil, err
		}
		return []endorsement.ExecutionResult{res}, nil
	}

	// len(txs)>1: the batch runs as a concurrent warm pass then a serial
	// authoritative pass. ExecuteBatch runs them back-to-back (the serial path);
	// the pipelined gateway loop instead overlaps WarmBatch(N+1) with
	// AuthMergedBatch(N). Composing here keeps both callers on one code path so
	// their behavior can never drift.
	wb, err := e.WarmBatch(ctx, txs)
	if err != nil {
		return nil, err
	}
	return e.authBatch(wb)
}

// WarmedBatch bundles an open query-service snapshot that WarmBatch has already
// warmed for txs (the concurrent warm pass ran against it, priming the snapshot
// view's read cache) and left OPEN. The caller MUST later consume it via
// AuthMergedBatch / authBatch (which close the snapshot) or call Close directly,
// or the snapshot -- a query-service view -- leaks. It also carries the
// warm-phase timing so the authoritative pass can emit the combined
// ENDORSE-TIMING / OVERLAP-SIM lines (debug only).
type WarmedBatch struct {
	reader  ReadStore // the raw open snapshot; closed by Close
	readSrc ReadStore // reader, or a countingReader wrapping it (debug timing)
	txs     []*types.Transaction

	fast    bool // no decorator / no debug logging: reuse one executor per pass
	timing  bool // evm.batch at debug level: measure read load per phase
	counted *countingReader

	// Pipelined authoritative pass only (set by AuthMergedBatch when the reader
	// is reopenable). warm's snapshot is one batch boundary stale by auth time,
	// so auth reads a FRESH view (authReader) reflecting current committed state
	// -- reusing warm's cold reads via the shared read cache (see
	// ReopenableReadStore). Nil for the serial path, which reuses readSrc.
	authReader  ReadStore       // fresh reopened snapshot; closed by Close
	authSrc     ReadStore       // authReader, or a countingReader wrapping it (debug timing)
	authCounted *countingReader // counts the auth pass's reads on authReader (debug timing)

	snapDur       time.Duration
	warmDur       time.Duration
	warmReads     int64
	warmReadNanos int64
	warmDoneNanos []int64 // per-tx warm-completion offset (ns from warm start)
	authCostNanos []int64 // per-tx authoritative cost (ns), filled by authBatch

	closeOnce sync.Once
}

// Close releases the warmed snapshot. Idempotent: authBatch/AuthMergedBatch
// close it via defer, while error and shutdown paths may close it directly --
// only the first call closes the underlying snapshot.
func (wb *WarmedBatch) Close() error {
	wb.closeOnce.Do(func() {
		if wb.reader != nil {
			_ = wb.reader.Close()
		}
		// The pipelined auth pass reads a fresh reopened view (a distinct
		// query-service view) that must be ended too, or it leaks.
		if wb.authReader != nil {
			_ = wb.authReader.Close()
		}
	})
	return nil
}

// WarmBatch opens one query-service snapshot for the batch and runs the
// concurrent warm pass against it (results discarded), priming the snapshot
// view's read cache before the authoritative pass. It returns the STILL-OPEN
// snapshot bundled in a WarmedBatch; the caller MUST later call AuthMergedBatch
// / authBatch (which close it) or Close() directly. This is the first half of
// ExecuteBatch's len>1 path, split out so the pipelined gateway loop can overlap
// WarmBatch(N+1) with the authoritative pass of batch N. It only READS the
// shared cross-batch caches, exactly as the serial path does.
//
// Precondition: len(txs) > 1 (the caller handles the 0/1 cases).
func (e *EVMEngine) WarmBatch(ctx context.Context, txs []*types.Transaction) (*WarmedBatch, error) {
	// One snapshot (one view) for the whole batch: every tx, in both passes,
	// simulates against the same consistent point-in-time state.
	snapStart := time.Now()
	reader, err := e.kvs.NewSnapshot(0)
	if err != nil {
		return nil, err
	}
	snapDur := time.Since(snapStart)

	// When evm.batch is at debug level, wrap the batch view to measure the read
	// load each phase places on the backend: how many under.Get calls it makes
	// and their cumulative wall-time. Instrumentation only -- Get is a
	// pass-through. Shared by both passes; its atomic counters are snapshotted at
	// the warm/auth boundary to attribute reads (and read I/O time) to each phase
	// (see the ENDORSE-TIMING log in authBatch). At the default info level the
	// view is used unwrapped, so no per-read atomics run on the hot path.
	timing := batchLogger.IsEnabledFor(zapcore.DebugLevel)
	readSrc := reader
	var counted *countingReader
	if timing {
		counted = &countingReader{under: reader}
		readSrc = counted
	}

	// Trace-driven overlap simulation (debug only): per-tx warm-completion offset
	// (ns from warm start) and per-tx authoritative cost (ns). authBatch uses
	// these post-batch to compute what an in-order warm||auth overlap WOULD
	// achieve on THIS batch's real (work-stealing) warm-completion order. Each
	// warm worker writes its own distinct index (no false-sharing race; wg.Wait
	// provides the read barrier); the auth pass is serial. See OVERLAP-SIM in
	// authBatch.
	var warmDoneNanos, authCostNanos []int64
	if timing {
		warmDoneNanos = make([]int64, len(txs))
		authCostNanos = make([]int64, len(txs))
	}

	// The fast path (production: no per-tx decorator, no debug logging) reuses
	// one StateDB+Executor per warm-pass worker, resetting the StateDB in place
	// between txs, so the per-tx machinery (StateDB maps, access list, block
	// context, signer, EVM) is built once instead of per tx. The slow path (a
	// decorator or debug logging wraps each StateDB) keeps building fresh per-tx
	// state via newState.
	fast := e.stateDecorator == nil && !e.evmConfig.DebugLogs

	// Warm pass: run every tx against the shared snapshot to warm any caching
	// reader (e.g. the query-service view backing `reader`) before the sequential
	// authoritative pass, which is what surfaces real results and errors. A tx
	// that would fail here (e.g. against pre-batch state) is not a bug.
	//
	// Concurrency = batch size (design RQ1: "one goroutine per transaction in the
	// batch"). Each warm read BLOCKS on a gRPC round-trip to the query service --
	// it is I/O-bound, not CPU-bound, so the worker count is NOT tied to
	// GOMAXPROCS. The query service coalesces concurrent single-key reads into one
	// DB call (min-batch-keys / max-batch-wait); firing all of a batch's reads at
	// once fills that window instead of leaving the server's max-batch-wait
	// exposed on every small wave. Blocked workers are parked on I/O (not
	// busy-waiting), so a large count is cheap.
	//
	// A work-stealing atomic-index pool -- not a raw goroutine-per-tx spawn -- lets
	// a free worker pick up a slow worker's remaining txs; with the default count
	// == len(txs) it is effectively one worker per tx. Each worker owns its per-tx
	// state so concurrent execution never shares a journal. An explicit
	// WarmWorkers override caps concurrency for a fast, non-blocking backend (e.g.
	// an in-memory KVS) where unbounded warm goroutines would add scheduler churn
	// with no I/O to overlap.
	warmWorkers := e.evmConfig.WarmWorkers
	if warmWorkers <= 0 || warmWorkers > len(txs) {
		warmWorkers = len(txs)
	}
	warmStart := time.Now()
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
				if sdb, ex, err = e.newReusableExecutor(readSrc); err != nil {
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
						sdb.reset(readSrc)
						_, _ = e.classify(ex, sdb, tx) // warm only; ignore result/error.
						return
					}
					if s, err := e.newState(readSrc); err == nil {
						_, _ = e.runOn(s, tx) // warm only; ignore result/error.
					}
				}(txs[i])
				if timing {
					warmDoneNanos[i] = int64(time.Since(warmStart))
				}
			}
		}()
	}
	wg.Wait()
	warmDur := time.Since(warmStart)
	var warmReads, warmReadNanos int64
	if timing {
		warmReads, warmReadNanos = counted.n.Load(), counted.nanos.Load()
	}

	return &WarmedBatch{
		reader:        reader,
		readSrc:       readSrc,
		txs:           txs,
		fast:          fast,
		timing:        timing,
		counted:       counted,
		snapDur:       snapDur,
		warmDur:       warmDur,
		warmReads:     warmReads,
		warmReadNanos: warmReadNanos,
		warmDoneNanos: warmDoneNanos,
		authCostNanos: authCostNanos,
	}, nil
}

// authBatch runs the serial authoritative pass over wb's already-warmed snapshot
// and closes the snapshot when done (defer wb.Close()). It returns the same
// []endorsement.ExecutionResult ExecuteBatch does -- one slot per input tx, index
// = sub-index -- and is the second half of ExecuteBatch's len>1 path.
func (e *EVMEngine) authBatch(wb *WarmedBatch) ([]endorsement.ExecutionResult, error) {
	defer wb.Close()

	txs := wb.txs
	// The pipelined path reopens warm's stale snapshot onto a fresh view and
	// sets authSrc (see AuthMergedBatch); the serial path leaves it nil and
	// reuses warm's still-fresh view.
	readSrc := wb.readSrc
	if wb.authSrc != nil {
		readSrc = wb.authSrc
	}

	// Authoritative pass: sequential, each tx against the snapshot plus an
	// overlay carrying every earlier tx's writes from this batch.
	authStart := time.Now()
	overlay := &overlayReader{under: readSrc, writes: map[string]*blocks.WriteRecord{}}
	out := make([]endorsement.ExecutionResult, 0, len(txs))
	// Fast path: one reused StateDB+Executor for the whole (serial) pass, reset
	// against the overlay before each tx.
	var authSdb *StateDB
	var authEx *Executor
	if wb.fast {
		var err error
		if authSdb, authEx, err = e.newReusableExecutor(overlay); err != nil {
			return nil, err
		}
	}
	for idx, tx := range txs {
		var authTxStart time.Time
		if wb.timing {
			authTxStart = time.Now()
		}
		var res endorsement.ExecutionResult
		var err error
		if wb.fast {
			authSdb.reset(overlay)
			res, err = e.classify(authEx, authSdb, tx)
		} else {
			var state ExtendedStateDB
			if state, err = e.newState(overlay); err == nil {
				res, err = e.runOn(state, tx)
			}
		}
		if wb.timing {
			// Per-tx auth EVM cost. warm has already run to completion, so these
			// reads are cache hits (auth readtime ~5ms/batch) -- i.e. essentially
			// pure EVM CPU, exactly the cost auth[i] would incur in an overlap
			// once warm[i] has prefetched its keys.
			wb.authCostNanos[idx] = int64(time.Since(authTxStart))
		}
		if err != nil {
			if rej, ok := errors.AsType[*TxRejected](err); ok {
				// Excluded, not aborted: a client-rejected tx (nonce gap, bad
				// signature, insufficient funds, ...) can never be included as it
				// stands, but the rest of the batch must still make progress.
				// Record a sentinel outcome with an empty RWS -- MergeResults
				// folds it in as a no-op -- and continue without applying anything
				// to the overlay. The caller (chain.go's block parser) recognizes
				// this status and skips it entirely: no domain tx, no committed
				// write. A RETRYABLE exclusion (nonce too high, insufficient funds,
				// ...) stays pending and is retried once its gap is filled; a
				// TERMINAL exclusion (nonce too low) can never resolve as this
				// exact tx and the caller should evict it (see excludedResult).
				out = append(out, excludedResult(rej))
				continue
			}
			// A genuine server-side fault (not a client rejection): still abort
			// the whole batch, as before.
			return nil, err
		}
		overlay.apply(res.RWS)
		out = append(out, res)
	}
	authDur := time.Since(authStart)

	// warm reads run concurrently (readtime is the SUM across workers, so it can
	// exceed warm wall-time); auth reads are serial (readtime <= auth wall-time,
	// and auth-wall minus auth-readtime is the serial CPU/EVM cost). This is the
	// single line that says which phase dominates and why. Guarded by wb.timing:
	// only reached when evm.batch is at debug level (counted is non-nil).
	if wb.timing {
		// Serial reuses warm's counted reader, so auth reads are the delta since
		// the warm/auth boundary. The pipelined path reads a separate reopened
		// reader (authCounted), whose own totals ARE the auth reads.
		authReads := wb.counted.n.Load() - wb.warmReads
		authReadNanos := wb.counted.nanos.Load() - wb.warmReadNanos
		if wb.authCounted != nil {
			authReads = wb.authCounted.n.Load()
			authReadNanos = wb.authCounted.nanos.Load()
		}
		batchLogger.Debugf("ENDORSE-TIMING n=%d snapshot=%s warm=%s{reads=%d readtime=%s} auth=%s{reads=%d readtime=%s cpu=%s}",
			len(txs),
			wb.snapDur.Round(time.Microsecond),
			wb.warmDur.Round(time.Microsecond), wb.warmReads, time.Duration(wb.warmReadNanos).Round(time.Microsecond),
			authDur.Round(time.Microsecond), authReads, time.Duration(authReadNanos).Round(time.Microsecond),
			(authDur - time.Duration(authReadNanos)).Round(time.Microsecond),
		)

		// Trace-driven overlap simulation. Given THIS batch's real per-tx warm-
		// completion offsets and per-tx auth costs, compute the wall an in-order
		// warm||auth overlap would achieve: auth[i] cannot start until warm[i] has
		// finished AND auth[i-1] has finished (auth stays serial; MVCC needs
		// tx-order). Both clocks share the warm-start origin. serial = today's
		// warm+auth; ideal = max(warm,auth) (a perfectly in-order warm); stall =
		// total time auth would sit idle waiting for its next-in-order tx.
		var authClock, authSum, stall int64
		for i := range txs {
			if authClock < wb.warmDoneNanos[i] {
				stall += wb.warmDoneNanos[i] - authClock
				authClock = wb.warmDoneNanos[i]
			}
			authClock += wb.authCostNanos[i]
			authSum += wb.authCostNanos[i]
		}
		serial := wb.warmDur + authDur
		overlap := time.Duration(authClock)
		ideal := wb.warmDur
		if authDur > ideal {
			ideal = authDur
		}
		batchLogger.Debugf("OVERLAP-SIM n=%d serial=%s overlap=%s ideal=%s stall=%s authwork=%s speedup=%.2fx (ceiling=%.2fx)",
			len(txs),
			serial.Round(time.Microsecond), overlap.Round(time.Microsecond), ideal.Round(time.Microsecond),
			time.Duration(stall).Round(time.Microsecond), time.Duration(authSum).Round(time.Microsecond),
			float64(serial)/float64(overlap), float64(serial)/float64(ideal),
		)
	}
	return out, nil
}

// AuthMergedBatch is authBatch folded into a single merged endorsement -- the
// pipelined counterpart of ExecuteMergedBatch. It runs the serial authoritative
// pass over wb's already-warmed snapshot (closing the snapshot when done) and
// folds the per-tx results into one merged ExecutionResult (status 200) plus one
// PerTxOutcome per sub-tx. For the same txs it is byte-identical to
// ExecuteMergedBatch(txs), only with the warm pass already run separately.
func (e *EVMEngine) AuthMergedBatch(ctx context.Context, wb *WarmedBatch) (endorsement.ExecutionResult, []PerTxOutcome, error) {
	// Pipelined authoritative pass: warm's snapshot was opened one batch boundary
	// ago (WarmBatch(N) overlaps auth(N-1)), so by now commits have advanced the
	// ledger and it is stale. Reopen it onto a FRESH view reflecting current
	// committed state, so auth reads exactly what a serial cycle's post-boundary
	// view would -- eliminating the stale-read MVCC aborts that livelock the
	// pipeline on conflict-heavy traffic -- while reusing warm's already-fetched
	// cold reads via the shared read cache (see ReopenableReadStore). This makes
	// pipelined auth read-identical to serial auth. Stores that cannot reopen
	// (e.g. the in-memory test KVS) fall back to reusing warm's view.
	if r, ok := wb.reader.(ReopenableReadStore); ok {
		fresh, err := r.Reopen()
		if err != nil {
			return endorsement.ExecutionResult{}, nil, err
		}
		wb.authReader = fresh // closed by wb.Close()
		wb.authSrc = fresh
		if wb.timing {
			wb.authCounted = &countingReader{under: fresh}
			wb.authSrc = wb.authCounted
		}
	}

	results, err := e.authBatch(wb)
	if err != nil {
		return endorsement.ExecutionResult{}, nil, err
	}
	res, outcomes := mergeOutcomes(results)
	return res, outcomes, nil
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
	sdb.codeHashCache = e.codeHashCache
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
	stateDB.codeHashCache = e.codeHashCache
	if e.evmConfig.DebugLogs {
		return NewStateDBLogger(stateDB), nil
	}
	return stateDB, nil
}

// overlayReader layers an in-memory, batch-local write set over an underlying
// snapshot ReadStore, so the authoritative pass sees earlier transactions'
// writes in this batch as if they were already committed.
//
// It is created per-batch and used ONLY by the serial authoritative pass on the
// single executor goroutine (Get during each tx, apply after each tx, strictly
// in sequence); the concurrent warm pass never touches it. writes therefore
// needs no lock.
type overlayReader struct {
	under  ReadStore
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

	rec, overlaid := o.writes[key]
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
// Called serially after each tx in the authoritative pass (no lock needed; see
// overlayReader).
func (o *overlayReader) apply(rws blocks.ReadWriteSet) {
	for _, w := range rws.Writes {
		o.writes[w.Key] = &blocks.WriteRecord{Key: w.Key, Value: w.Value, IsDelete: w.IsDelete}
	}
}

var _ ReadStore = (*overlayReader)(nil)

// countingReader is a pass-through ReadStore that counts Get calls and the
// cumulative wall-time spent inside the wrapped store's Get. It exists to
// attribute ExecuteBatch's backend read load to the warm vs authoritative
// phase (see the ENDORSE-TIMING log): counters are atomic so the concurrent
// warm pass and the serial authoritative pass can share one instance and be
// read at the phase boundary. Get adds only two atomic increments and a
// time.Now/Since pair per call -- negligible against a gRPC read -- and it is
// only wired in when evm.batch is at debug level, so production never allocates
// or touches it.
type countingReader struct {
	under ReadStore
	n     atomic.Int64 // number of Get calls
	nanos atomic.Int64 // cumulative wall-time spent inside under.Get, in ns
}

func (c *countingReader) Get(ns, key string) (*blocks.WriteRecord, error) {
	start := time.Now()
	rec, err := c.under.Get(ns, key)
	c.nanos.Add(int64(time.Since(start)))
	c.n.Add(1)
	return rec, err
}

// Close is a no-op: the wrapped snapshot's lifecycle is owned by ExecuteBatch
// (which closes `reader` directly), not by this instrumentation wrapper.
func (c *countingReader) Close() error { return nil }

var _ ReadStore = (*countingReader)(nil)
