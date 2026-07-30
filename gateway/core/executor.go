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
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"go.uber.org/zap/zapcore"
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
// 5); specVers maps each written key to the spec version the cache assigned it
// at submit time (captured immediately after ApplyWrites, when this batch is the
// latest writer of all its keys), used by queryCommitStatus to adjudicate a
// sidecar timeout against committed versions; timer is the per-batch backstop
// that rolls the batch back if no commit/abort notification arrives in time --
// nil when a per-TxID notifier owns the timeout (see trackInflight).
type inflightBatch struct {
	txID     string
	included []*types.Transaction
	rws      blocks.ReadWriteSet
	specVers map[string]uint64
	timer    *time.Timer
	// submittedAt is when this batch entered the in-flight registry, set in
	// trackInflight immediately before SubmitFabricTx. resolveInflight subtracts
	// it from the commit-notification arrival to measure submit->commit wall time
	// (see Gateway.commitLatencyNanos / CommitLatencyStats) -- the commit-path
	// cost the ENDORSE-TIMING execution split cannot see. Set once, never mutated,
	// so any goroutine may read it lock-free.
	submittedAt time.Time
}

// runExecutor is the drain loop. Each cycle drains up to maxBatchSize txs,
// two-phase-executes + merges them into one committer tx, applies that tx's
// writes to the cross-batch cache, records it in-flight, submits it, and
// returns IMMEDIATELY -- it does NOT wait for the commit. The next cycle runs at
// once, executing against the cache (which now carries the prior in-flight
// batch's writes). Backpressure comes from the in-flight window (inflightSlots):
// once maxInflight batches are outstanding, the next acquire blocks until one is
// confirmed. Commit/abort outcomes are resolved asynchronously by
// resolveInflight (driven by HandleTx/Handle).
//
// When g.pipelined is set (see SetPipelined) it runs the pipelined loop instead,
// overlapping the concurrent warm pass of batch N+1 with the serial
// authoritative pass of batch N; the serial and pipelined paths share the submit
// boundary (submitBatch), so a batch commits identically either way.
func (g *Gateway) runExecutor(ctx context.Context) {
	defer g.wg.Done()
	if g.pipelined {
		g.runExecutorPipelined(ctx)
		return
	}
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
	// cycle and never mutates mid-execution. If any INVALIDATION was applied, a
	// dropped later writer may have erased a key a surviving earlier in-flight
	// batch also wrote (VersionedCache keeps only the latest writer per key), so
	// rebuild the cache from the survivors to restore their speculative writes.
	// Commits never need this: they resolve earliest-first, so a committed key a
	// later survivor also wrote already carries the survivor's writerTx and is
	// left untouched by the committed-drop. Both happen here, on the executor
	// goroutine at the boundary -- the cache is never mutated mid-batch.
	if _, invalidated := g.cache.DrainEvictions(); len(invalidated) > 0 {
		g.rebuildCacheFromInflight()
	}

	// Same boundary, same goroutine: run the read-only cache's maintenance
	// (evict this gateway's freshly-written keys, admit hot staged candidates,
	// enforce MFU capacity). No-op unless a read-only cache was enabled. Kept
	// here so read-only entries, like write-cache entries, only ever change
	// between batches -- never mid-execution while warm workers are reading.
	g.cache.MaintainReadOnly()

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

	// Hand the result to the shared submit boundary. On batchExcluded (every
	// drained tx was excluded -- e.g. a lone nonce-gap tx with no filler yet)
	// wait for new work rather than busy-re-draining the same excluded txs; a
	// predecessor's commit also signals arrivals (see resolveInflight), so a
	// gap-filling commit re-wakes us. batchSubmitted / batchFailed just return
	// (the loop re-drains immediately or after submitBatch's own backoff).
	if g.submitBatch(ctx, batch, included, terminal, end, rws) == batchExcluded {
		g.waitForWork(ctx)
	}
	// No await-commit: return so the next cycle runs immediately. resolveInflight
	// (driven by HandleTx/Handle or the timeout) finalizes or rolls back later.
}

// batchResult is submitBatch's outcome, so its callers (serial executeCycle and
// pipelined pipelineIteration) can distinguish the three post-submit control
// flows: a submitted in-flight batch, a fully-excluded batch (caller may wait
// for work), and a failed submit/decode (submitBatch already backed off).
type batchResult int

const (
	batchSubmitted batchResult = iota // a committer tx was submitted; it is in flight
	batchExcluded                     // included == 0: nothing to submit, no error
	batchFailed                       // committer-TxID/submit/shutdown failure (already backed off)
)

// submitBatch is the shared submit boundary of both executor loops: given a
// batch's authoritative-pass result (end, included, terminal, rws), it evicts
// terminally-excluded txs, and -- if any tx was included -- extracts the
// committer TxID, applies the batch's writes to the cross-batch cache, captures
// the spec versions, acquires an in-flight slot, records the batch in flight,
// removes the included txs from pending, and submits. It returns IMMEDIATELY
// after submit, WITHOUT awaiting the commit (resolveInflight finalizes async).
//
// It must run on the executor goroutine: it mutates the cross-batch cache
// (ApplyWrites) and reads it back (spec versions), which is only safe when no
// warm reads are in flight. The pipelined caller therefore invokes it only
// after its warm barrier (see pipelineIteration). Serial and pipelined execution
// share this method verbatim, so a batch commits byte-identically either way.
func (g *Gateway) submitBatch(ctx context.Context, batch, included, terminal []*types.Transaction, end sdk.Endorsement, rws blocks.ReadWriteSet) batchResult {
	// Evict terminally-excluded txs (nonce too low, ...) unconditionally: the
	// exclusion reflects ledger state from before this batch ran, so it holds
	// regardless of this batch's outcome; left pending they would leak.
	if len(terminal) > 0 {
		g.pending.Remove(hashesOf(terminal))
	}

	if len(included) == 0 {
		return batchExcluded
	}

	fabricTxID, err := committerTxID(end.Proposal)
	if err != nil {
		logger.Errorf("extract committer tx id (%d txs): %v", len(batch), err)
		g.backoff(ctx) // txs stay pending; re-drained next cycle
		return batchFailed
	}

	// Apply this batch's writes to the cross-batch cache BEFORE submitting, so
	// the next cycle's endorsement reads them without waiting for the commit.
	g.cache.ApplyWrites(fabricTxID, rws)

	// Capture the spec version the cache just assigned each written key. This is
	// race-free: the executor goroutine is the ONLY writer of the cache, so
	// between ApplyWrites and this readback no other write intervenes, and this
	// batch is the latest writer of each of its keys -- so the readback yields
	// exactly this batch's spec versions. queryCommitStatus later compares these
	// against committed versions to adjudicate a sidecar timeout.
	specVers := make(map[string]uint64, len(rws.Writes))
	for _, w := range rws.Writes {
		if rec, ok := g.cache.Read(w.Key); ok {
			specVers[w.Key] = rec.Version
		}
	}

	// Backpressure: block until an in-flight slot is free (resolveInflight
	// frees one on each commit/abort/timeout). On shutdown just return; the
	// cache is discarded with the gateway.
	select {
	case g.inflightSlots <- struct{}{}:
	case <-ctx.Done():
		return batchFailed
	}

	// Record in-flight (with the timeout backstop or, when a notifier is wired,
	// its per-TxID subscription via g.Watch) and remove the included txs from
	// pending BEFORE submit, so the next cycle cannot re-drain them and a commit
	// notification cannot race ahead of the registry entry.
	g.trackInflight(fabricTxID, included, rws, specVers)
	g.pending.Remove(hashesOf(included))

	if err := g.SubmitFabricTx(ctx, end); err != nil {
		logger.Errorf("batch submit failed (tx %s, %d txs): %v", fabricTxID, len(batch), err)
		g.resolveInflight(fabricTxID, false) // roll back: re-add included, evict, free the slot
		g.backoff(ctx)
		return batchFailed
	}
	return batchSubmitted
}

// warmResult carries a background warm goroutine's outcome back to the
// pipeline's barrier join (a value type so the barrier is a single channel
// receive). Exactly one of wb / err is meaningful.
type warmResult struct {
	wb  *WarmedBatch
	err error
}

// runExecutorPipelined is the pipelined drain loop (selected by SetPipelined). It
// overlaps the I/O-bound concurrent WARM pass of batch N+1 with the CPU-bound
// serial AUTHORITATIVE pass of batch N, collapsing the per-batch wall from
// warm+auth to max(warm,auth)+boundary. It carries ONE warmed batch across
// iterations (prefetch depth 1: auth is the serial floor, so a deeper prefetch
// buys nothing -- see the warm-auth-pipelining design). The submit boundary is
// shared verbatim with the serial path (submitBatch), so a batch commits
// byte-identically either way.
//
// It does NOT call g.wg.Done -- runExecutor already defers it; this method is
// invoked in that same goroutine.
//
// warmed is the batch whose warm pass is complete and whose authoritative pass
// runs next; it is always a batch that has been warmed and RESERVED in the
// pending pool but not yet authorized. nil means the invariant is not
// established (startup, an empty drain, or an error) and drainAndWarm must
// (re-)establish it before an iteration can run.
func (g *Gateway) runExecutorPipelined(ctx context.Context) {
	var warmed *WarmedBatch
	// On shutdown (or any exit) with a batch warmed but never authorized, release
	// its pending reservation so its txs are re-drawable, and close its snapshot
	// so no query-service view leaks. pipelineIteration hands ownership of the
	// prior `warmed` off (auth consumes+closes it) before returning the next one,
	// so at any exit `warmed` is exactly the one un-authorized batch to clean up.
	defer func() {
		if warmed != nil {
			g.pending.Release(hashesOf(warmed.Txs()))
			_ = warmed.Close()
		}
	}()
	for ctx.Err() == nil {
		if warmed == nil {
			warmed = g.drainAndWarm(ctx) // (re-)establish the invariant
			continue
		}
		warmed = g.pipelineIteration(ctx, warmed)
	}
}

// drainAndWarm establishes the pipeline invariant: it drains (reserving) the
// next batch and runs its warm pass, returning the warmed handle. It returns nil
// -- leaving the invariant unestablished for the caller to retry -- when the
// pending pool is empty (after waiting for work) or the warm pass fails (after
// releasing the batch's reservation and backing off). Used for the first
// iteration and to recover after any iteration that could not carry a next
// batch forward.
func (g *Gateway) drainAndWarm(ctx context.Context) *WarmedBatch {
	txs := g.pending.DrainUpToReserved(int(g.maxBatchSize.Load()))
	if len(txs) == 0 {
		g.waitForWork(ctx)
		return nil
	}
	warmed, err := g.endorsers.WarmBatch(ctx, txs)
	if err != nil {
		logger.Errorf("pipelined warm failed (%d txs): %v", len(txs), err)
		g.pending.Release(hashesOf(txs)) // un-reserve so the txs are re-drawable
		g.backoff(ctx)
		return nil
	}
	return warmed
}

// pipelineIteration runs one overlapped iteration over an already-warmed batch N
// and returns the batch it warmed for the next iteration (N+1), or nil when it
// could not carry one forward (empty next drain, warm error, or auth error) --
// in which case the caller re-establishes the invariant via drainAndWarm.
//
// Ordering (see the design's loop a-g): boundary prep (evictions + RO
// maintenance) runs FIRST, on the executor goroutine with NO warm reads in
// flight (the previous iteration joined its warm at the barrier; this
// iteration's warm has not launched yet) -- equivalent to the serial cycle's top
// and to the design's step f of the prior iteration, but placed here so it runs
// on every path including a freshly re-established invariant. Then: (a) drain +
// reserve N+1; (b) launch warm(N+1); (c) auth(N) -- overlaps (b); (d) barrier
// join; (e) release N + shared submit boundary; (g) carry N+1 forward.
func (g *Gateway) pipelineIteration(ctx context.Context, warmed *WarmedBatch) *WarmedBatch {
	// Boundary prep: apply queued commit/abort evictions (rebuilding the cache
	// from survivors if any invalidation dropped a key an earlier survivor also
	// wrote) and run the read-only cache's MFU maintenance, exactly as the serial
	// cycle does at its top. Safe here: no warm reads are in flight, so these
	// structural cache mutations never race a concurrent warm.
	if _, invalidated := g.cache.DrainEvictions(); len(invalidated) > 0 {
		g.rebuildCacheFromInflight()
	}
	g.cache.MaintainReadOnly()

	// (a) Drain batch N+1, RESERVING the returned hashes so this non-destructive
	// peek cannot re-draw batch N (still pending until step e) nor the batch
	// already held as `warmed`.
	txsNext := g.pending.DrainUpToReserved(int(g.maxBatchSize.Load()))

	// (b) Launch the concurrent warm pass of batch N+1 so it overlaps auth(N). It
	// reads the LIVE cross-batch caches read-only (results discarded -- warm only
	// primes the per-view query-service read cache). Skipped on an empty drain.
	var warmFut chan warmResult
	if len(txsNext) > 0 {
		warmFut = make(chan warmResult, 1)
		go func() {
			wb, err := g.endorsers.WarmBatch(ctx, txsNext)
			warmFut <- warmResult{wb: wb, err: err}
		}()
	}

	// (c) Authoritative pass of batch N over its already-warmed snapshot,
	// concurrent with warm(N+1). It reads the LIVE caches read-only and writes
	// only its own batch overlay; AuthBatch consumes and closes batch N's
	// snapshot. This is the serial CPU floor the warm pass hides behind.
	end, included, terminal, rws, authErr := g.endorsers.AuthBatch(ctx, warmed)

	// (d) BARRIER: join warm(N+1). After this receive no warm reads are in flight,
	// so the boundary mutations below (step e's ApplyWrites, next iteration's
	// eviction drain) are safe. On a warm error, un-reserve batch N+1 so its txs
	// are re-drawable; warmedNext stays nil (re-established next iteration).
	var warmedNext *WarmedBatch
	if warmFut != nil {
		wr := <-warmFut
		if wr.err != nil {
			logger.Errorf("pipelined warm failed (%d txs): %v", len(txsNext), wr.err)
			g.pending.Release(hashesOf(txsNext))
		} else {
			warmedNext = wr.wb
		}
	}

	// (e) Boundary (executor goroutine, no warm reads in flight): clear batch N's
	// reservation, then hand its authoritative result to the shared submit
	// boundary exactly as the serial path does.
	g.pending.Release(hashesOf(warmed.Txs()))

	if authErr != nil {
		logger.Errorf("pipelined batch auth failed (%d txs): %v", len(warmed.Txs()), authErr)
		g.backoff(ctx)
		// Drop the batch we warmed but will not authorize this round: close its
		// snapshot AND un-reserve it so it is re-drawn (not skipped) next
		// iteration. Batch N's txs stay pending (released above) to be re-drawn.
		if warmedNext != nil {
			g.pending.Release(hashesOf(warmedNext.Txs()))
			_ = warmedNext.Close()
		}
		return nil
	}

	// batchExcluded (every tx excluded) is NOT a wait-for-work signal here as it
	// is in the serial cycle: the pipeline already has batch N+1 warmed and ready,
	// so it proceeds. batchFailed already backed off inside submitBatch.
	g.submitBatch(ctx, warmed.Txs(), included, terminal, end, rws)

	// (g) Carry batch N+1 forward as the next iteration's warmed batch (nil on an
	// empty drain -> the caller's drainAndWarm waits for work and re-establishes).
	return warmedNext
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
// first), arms its timeout backstop, and registers it for per-TxID commit
// notification. Called from executeCycle after a slot is acquired and before
// SubmitFabricTx, so a commit notification can never arrive before the entry
// exists.
//
// Timer ownership: when a per-TxID notifier is wired (g.notifier != nil) it owns
// the timeout -- txNotifier.Watch arms its own client-side backstop, and its
// onTimeout defers to the query-service fallback (queryCommitStatus) rather than
// blindly rolling back -- so no AfterFunc is armed here (b.timer stays nil).
// When no notifier is wired (block-sync / unwired path), the AfterFunc backstop
// below is the sole guarantee that a lost commit/abort cannot wedge the in-flight
// window: on it, resolveInflight rolls the batch back exactly as an MVCC abort
// would. The g.Watch call is a no-op on that path.
//
// Ordering: the AfterFunc backstop (unwired path) is armed AFTER the registry
// append below, symmetric with the notifier path (which arms its own timer only
// once Watch runs, also after the append). This closes the theoretical window
// where the timer could fire before the entry exists (a no-op into an empty
// registry, leaking the in-flight slot forever). The arm happens while still
// holding inflightMu (in the same critical section as the append), NOT after
// releasing it: cascadeFrom/resolveInflight read b.timer both under inflightMu
// and, for an already-detached suffix, after unlocking (see cascadeFrom) --
// either way they only ever observe b after acquiring inflightMu at least once,
// so b.timer must be fully written before that same lock is released here, or
// a concurrent cascade of an earlier in-flight batch (which can run on another
// goroutine at any time once this batch is appended) could read b.timer while
// it is being written, an unsynchronized data race. resolveInflight/cascadeFrom
// remain idempotent regardless, so a fast commit/cascade that resolves the
// entry the instant after this critical section releases the lock is harmless.
func (g *Gateway) trackInflight(txID string, included []*types.Transaction, rws blocks.ReadWriteSet, specVers map[string]uint64) {
	b := &inflightBatch{txID: txID, included: included, rws: rws, specVers: specVers, submittedAt: time.Now()}

	g.inflightMu.Lock()
	g.inflight = append(g.inflight, b)
	// Record the peak in-flight watermark (pure test observability; see
	// MaxInflightObserved). Under inflightMu there is no writer race; atomic.Int64
	// is used so the accessor can read it without taking the lock.
	if n := int64(len(g.inflight)); n > g.maxInflightObserved.Load() {
		g.maxInflightObserved.Store(n)
	}
	if g.notifier == nil {
		timeout := g.commitTimeout
		if timeout <= 0 {
			timeout = commitTimeoutDefault
		}
		b.timer = time.AfterFunc(timeout, func() { g.resolveInflight(txID, false) })
	}
	g.inflightMu.Unlock()

	// Register-then-submit: with the registry entry in place, subscribe to this
	// TxID's commit status (and arm the notifier's timer) before the caller
	// submits, so a fast commit notification always finds the entry. No-op when
	// the notifier is unwired.
	g.Watch(txID)
}

// resolveInflight finalizes (committed) or rolls back (aborted / timed out) the
// in-flight batch for txID, then frees its in-flight slot. It is idempotent:
// the timeout timer and the commit/abort notification race to call it, and only
// the first -- the one that removes the registry entry -- does the work.
//
// On commit the batch's writes are confirmed (NoteCommitted queues their cache
// eviction for the next batch boundary) and its included txs, already removed
// from pending at submit, stay gone.
//
// On NOT-committed (MVCC abort / timeout / submit failure) it delegates entirely
// to cascadeFrom: a later in-flight batch may have executed against this batch's
// now-invalidated speculative writes, so the whole registry suffix from txID
// onward must be invalidated and re-queued. The not-committed path does NO
// locking of its own -- cascadeFrom takes inflightMu -- so resolveInflight never
// holds inflightMu when it calls cascadeFrom (guards against a double lock).
func (g *Gateway) resolveInflight(txID string, committed bool) {
	if !committed {
		g.cascadeFrom(txID)
		return
	}

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
	if b.timer != nil { // nil when the notifier owns the timeout (see trackInflight)
		b.timer.Stop()
	}
	g.inflight = append(g.inflight[:idx], g.inflight[idx+1:]...)
	g.inflightMu.Unlock()

	<-g.inflightSlots // free the in-flight slot

	// Record submit->commit wall time (commit-path observability; see
	// CommitLatencyStats). b.submittedAt is immutable, so this lock-free read is
	// race-free. Only the committed path is measured: a rollback/timeout is not a
	// commit latency. Together with MaxInflightObserved vs the in-flight cap this
	// says whether the commit path is on the critical path (window saturated) or
	// hidden behind execution (window never fills).
	lat := time.Since(b.submittedAt)
	g.commitCount.Add(1)
	g.commitLatencyNanos.Add(int64(lat))
	for { // maintain the max lock-free (CAS retry loop; contention is negligible)
		cur := g.commitLatencyMax.Load()
		if int64(lat) <= cur || g.commitLatencyMax.CompareAndSwap(cur, int64(lat)) {
			break
		}
	}
	if logger.IsEnabledFor(zapcore.DebugLevel) {
		logger.Debugf("COMMIT-TIMING tx=%s submit->commit=%s inflight-peak=%d/%d",
			txID, lat.Round(time.Millisecond), g.maxInflightObserved.Load(), g.maxInflight)
	}

	g.cache.NoteCommitted(txID)

	// Wake the executor if idle: a commit may unblock retryable-excluded txs
	// whose predecessor just committed. Non-blocking, mirroring AddPending.
	select {
	case g.arrivals <- struct{}{}:
	default:
	}
}

// cascadeFrom rolls back the in-flight batch for txID AND every LATER batch --
// the contiguous registry suffix [idx:end]. A later batch may have executed
// against txID's now-invalidated speculative writes (the cache served them
// before the commit), so it cannot be trusted and must re-execute; the batches
// earlier than txID are a prefix of survivors and are untouched here.
//
// It runs on the timer/notification goroutine, so it must NOT mutate the cache
// entries directly (invariant 1: cache entries change only at a batch boundary
// on the executor goroutine). It only QUEUES work: NoteInvalidated for each
// departed batch (the boundary DrainEvictions drops their entries and then
// rebuildCacheFromInflight repairs any key a surviving earlier batch also
// wrote), re-adds their txs to pending, and releases their slots.
//
// Idempotency (invariants 3 & 4): detaching the suffix from g.inflight under
// inflightMu in one critical section makes the FIRST caller win. Concurrent
// timers/notifications for batches in the same suffix then find nothing (the
// entries are already gone) and are no-ops, so each batch's slot is released and
// its timer stopped exactly once -- by whichever caller detached it.
func (g *Gateway) cascadeFrom(txID string) {
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
		return // already resolved/cascaded -- idempotent
	}
	// Detach the suffix. The three-index slice (cap-limited to idx) ensures a
	// later append to g.inflight allocates a fresh backing array instead of
	// clobbering the detached suffix we still read below.
	suffix := g.inflight[idx:]
	g.inflight = g.inflight[:idx:idx]
	// Count this as one real cascade: the txID was found and a suffix is being
	// detached (pure test observability; see CascadeCount). The idempotent
	// not-found early return above never reaches here, so already-resolved IDs
	// are not counted.
	g.cascadeCount.Add(1)
	g.inflightMu.Unlock()

	for _, b := range suffix {
		if b.timer != nil { // nil when the notifier owns the timeout (see trackInflight)
			b.timer.Stop()
		}
		g.cache.NoteInvalidated(b.txID)
		for _, tx := range b.included {
			g.pending.Add(tx) // return to pending to retry
		}
		<-g.inflightSlots // release its slot (exactly once: we detached it)
	}

	// Wake the executor if idle: the cascade just re-queued work. Non-blocking,
	// mirroring AddPending.
	select {
	case g.arrivals <- struct{}{}:
	default:
	}
}

// rebuildCacheFromInflight snapshots the surviving in-flight registry into an
// ordered []ReapplySpec (oldest first) and rebuilds the cache from it. Called at
// the batch boundary in executeCycle after a drain that applied invalidations.
// Snapshot-then-call so inflightMu and the cache lock are never held together
// (invariant: never hold both at once -- avoids a lock-order coupling).
func (g *Gateway) rebuildCacheFromInflight() {
	g.inflightMu.Lock()
	specs := make([]ReapplySpec, len(g.inflight))
	for i, b := range g.inflight {
		specs[i] = ReapplySpec{TxID: b.txID, RWS: b.rws}
	}
	g.inflightMu.Unlock()
	g.cache.Rebuild(specs)
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
