# Warm(N+1) ‖ Auth(N) execution pipelining — design

**Status:** APPROVED for stage 1 (2026-07-30). Build the **barrier** mechanism
below. The default serial path is unchanged; this is an opt-in mode until the
rig validates it, then it becomes the default. The **snapshot-per-warm** variant
is deferred to stage 2 (see *Deferred alternative*).

**Goal:** Overlap the I/O-bound concurrent *warm* pass of batch N+1 with the
CPU-bound serial *authoritative* pass of batch N, collapsing the gateway's
per-batch wall from `warm + auth` to `max(warm, auth) + boundary`.

## Stage-1 decision: barrier, not snapshot-per-warm

Two lock-free mechanisms can pipeline warm behind auth:

- **Barrier (this design):** run `warm(N+1) ‖ auth(N)`, then **join before** the
  cross-batch cache mutations (ApplyWrites / eviction / RO-maintenance). During
  the overlap window nothing mutates the shared caches — both goroutines only
  *read* them — so concurrent Go-map reads are safe with **no lock and no
  snapshot**. warm(N+1) reads the *live* write-cache directly. Touches
  `VersionedCache` not at all.
- **Snapshot-per-warm (deferred):** hand the background warm an immutable
  frozen copy of the write-cache so the boundary can run without joining warm
  first. Requires a `CacheLayer` interface + `VersionedCache.Snapshot()` +
  a generalized layered reader.

The fresh measurement (below) shows **warm (~78 ms) < auth (~90 ms)**, so
`warm(N+1)` finishes *before* `auth(N)` and the barrier's join waits ≈ 0 ms —
the two mechanisms are throughput-equivalent at the current operating point
(~1.85× ceiling). The barrier is meaningfully less code (no cache-snapshotting
subsystem), so it is the stage-1 choice. Snapshot-per-warm's only edge is
robustness if warm ever *exceeds* auth (see *Deferred alternative*); that is a
stage-2 concern.

## Motivation (measured — clean stack, post QS-connection-pool)

Native ec2 (32 vCPU), synthetic, batch 1024, window 20000, `GOGC=500`,
`FABRIC_LOGGING_SPEC=info:evm.batch=debug`. Baseline at the code-default QS
connection pool ≈ **5116 EVM tx/s**, 20000/20000 committed, 0 rolled back.
Per-batch `ENDORSE-TIMING` (n=1024 on every steady-state batch):

```
snapshot ≈ 0.5 ms
warm     ≈ 78 ms  (median; range 73-113 ms). I/O-bound; concurrency = batch
                   size (~1024 workers over the 8-conn QS pool). Cumulative
                   readtime ≈ 63 s across the workers ⇒ ~800× effective read
                   concurrency. Results discarded — warm only primes the
                   per-view QS read cache.
auth     ≈ 90 ms  (median; range 84-131 ms). Serial, in-order MVCC over the
                   per-batch overlay. reads = 14336 but readtime only ~5 ms
                   (all cache HITS — warm primed the view) ⇒ ~86 ms is pure
                   single-threaded EVM interpreter CPU.
```

serial batch = warm + auth ≈ **168 ms** (~46% warm-I/O / ~51% auth-CPU). Commit
is already fully pipelined and hidden behind execution (async in-flight window,
`maxInflight=16`) and is **not** the bottleneck — the read path / execution is
(see the QS-resource-discovery findings: DB 99.995% cache-hit, 2/10 DB conns,
QS 1-2/32 cores — nothing downstream of the endorser is saturated).

**OVERLAP-SIM** (`ideal = max(warm, auth) ≈ auth ≈ 90 ms`): serial/ideal ceiling
≈ **1.74–2.0× (median ~1.85×)**. A **1-deep** prefetch (one warmed batch carried
across iterations) suffices to hide warm entirely behind the serial auth chain;
deeper prefetch adds nothing because auth is the serial floor. Note the sim's
*within-batch* in-order overlap number (~1.02–1.05×) is NOT this ceiling — that
number reflects work-stealing warm order ≠ tx order within one batch, which is
irrelevant to the cross-batch pipeline.

Why this is the last big EVM-side lever: auth's ~86 ms is near-irreducible EVM
work (DelegateCall proxy→impl, SLOADs, storage-slot keccak) and MUST stay serial
for MVCC ordering. The code-hash cache, RO-cache, GOGC tuning, and the QS
connection pool are already banked. Batch > 1024 is a separate stack-side lever.

## Current structure (what we split)

- `EVMEngine.ExecuteBatch(ctx, txs)` (endorser/execution/batch_executor.go):
  opens ONE query-service snapshot (`e.kvs.NewSnapshot(0)`), runs the warm pass
  (goroutine-per-tx work-stealing pool, `warmWorkers = len(txs)`), then the
  serial auth pass over an `overlayReader`, and `defer reader.Close()`s the
  snapshot. One snapshot per batch.
- `EndorsementClient.ExecuteBatch` (gateway/core/endorse.go): fans out to
  `e.endorsers []api.Service` (concurrent when len > 1; today N=1), each
  endorser's `ExecuteBatch` → `Engine.ExecuteMergedBatch` → warm+auth → sign
  (`f.builder.Endorse`). Signing is post-execution.
- Gateway `executeCycle` (gateway/core/executor.go), serial per cycle:
  1. `DrainEvictions` (+ `rebuildCacheFromInflight` if any invalidation),
  2. `MaintainReadOnly` (RO-cache MFU maintenance),
  3. `DrainUpTo(maxBatchSize)` (non-destructive FIFO peek),
  4. `endorsers.ExecuteBatch` (warm+auth),
  5. `ApplyWrites` + capture spec versions,
  6. acquire in-flight slot, `trackInflight`, `Remove(included)` from pending,
  7. `SubmitFabricTx`, return (commit resolved async by `resolveInflight`).

All cross-batch cache mutation (steps 1, 2, 5) happens **at the boundary on the
executor goroutine** — the invariant that makes the concurrent warm reads safe.

## Design

### 1. Engine API split (endorser/execution)

Introduce a warmed-batch handle and two methods; keep `ExecuteBatch` intact for
the serial path and the `len(txs)==1` fast path.

```go
// warmedBatch owns an open query-service snapshot that has already been warmed
// for txs. AuthBatch consumes it and closes the snapshot.
type warmedBatch struct {
    reader ReadStore              // the open snapshot (NOT yet closed)
    txs    []*types.Transaction
}

// WarmBatch opens a snapshot and runs the concurrent warm pass against it,
// returning the still-open snapshot. Caller MUST later call AuthBatch (or
// Close) to release it.
func (e *EVMEngine) WarmBatch(ctx context.Context, txs []*types.Transaction) (*warmedBatch, error)

// AuthBatch runs the serial authoritative pass over wb's already-warmed
// snapshot, then closes the snapshot. Returns the same []ExecutionResult
// ExecuteBatch does.
func (e *EVMEngine) AuthBatch(ctx context.Context, wb *warmedBatch) ([]endorsement.ExecutionResult, error)
```

`ExecuteBatch` becomes `WarmBatch` then `AuthBatch` (refactor the existing warm
and auth loops into these two, behavior unchanged). The engine's warm and auth
loops both read the *live* cross-batch caches exactly as today — no interface
change to the read path.

### 2. Endorser + client plumbing

- `endorser/core.Endorser` gains `WarmBatch`/`AuthBatch` mirroring its
  `ExecuteBatch`: WarmBatch runs `Engine.WarmBatch`; AuthBatch runs
  `Engine.AuthBatch` → marshal outcomes → `f.builder.Endorse` (signing stays in
  auth, where it belongs — it is post-execution).
- `api.Service` (endorser/api/service.go) gains `WarmBatch`/`AuthBatch`; both
  `*ecore.Endorser` and the test `EndorserWrapper` implement them.
- `EndorsementClient.WarmBatch`/`AuthBatch` (gateway/core/endorse.go) fan out
  across `e.endorsers []api.Service` and bundle the per-endorser `warmedBatch`
  handles into one gateway-level handle (`type WarmedBatch struct { per []*... }`),
  so the design handles N ≥ 1 endorsers (today N = 1). AuthBatch fans the handle
  back out, one sub-handle per endorser, and merges results as `ExecuteBatch`
  does today.

### 3. Pipelined gateway loop (barrier, opt-in)

A new `runExecutorPipelined`, selected when `g.pipelined` is set (default false →
current `runExecutor`). It carries one `warmed *WarmedBatch` across iterations
(depth 1):

```
// invariant entering an iteration: `warmed` is a fully-warmed batch N (snapshot
// open, warm pass done); boundary work for batch N-1 already applied; batch N's
// txs are RESERVED in the pending pool.
for ctx not done:
    a. txsNext := pending.DrainUpTo(maxBatchSize)   // skips reserved, reserves the
                                                    //   returned hashes ⇒ batch N+1
    b. if len(txsNext) > 0:
           warmFut := go endorsers.WarmBatch(txsNext)   // reads LIVE caches, read-only
    c. resultN, err := endorsers.AuthBatch(warmed)      // reads LIVE caches read-only;
                                                        //   writes only its batch overlay
    d. warmedNext := <-warmFut (or nil)                 // BARRIER: warm(N+1) fully done
    e. // boundary, executor goroutine, AFTER the barrier — no warm reads in flight:
       pending.Release(warmed.txs)                      // clear batch N's reservation
       handle resultN exactly as serial step 5-7:
         ApplyWrites(N) + specVers, acquire slot, trackInflight,
         Remove(included/terminal) from pending, SubmitFabricTx
    f. // next-boundary prep, still on executor goroutine:
       DrainEvictions (+ rebuild), MaintainReadOnly
    g. warmed = warmedNext
       if warmed == nil:   // pending was empty at step a
           waitForWork; drain+warm inline to re-establish the invariant
```

The concurrency window is **b–c–d**: warm(N+1) overlaps auth(N). Both are
**read-only** on the shared cross-batch caches (VersionedCache write-cache `Read`
+ RO-cache `get`/`stage`). No structural cache mutation happens in that window.

### 4. PendingPool reservation (new)

The serial loop relies on `Remove(included)` (step 6) running *before* the next
cycle's `DrainUpTo` (step 3), so the non-destructive FIFO peek never re-draws the
same batch. The pipeline breaks that ordering: step a drains batch N+1 while
batch N is still in pending (not `Remove`d until step e). Without a guard,
`DrainUpTo` would re-draw batch N.

Add a reservation set to `PendingPool`:

```go
type PendingPool struct {
    mu       sync.Mutex
    order    []Hash
    txs      map[Hash]*Tx
    reserved map[Hash]struct{}   // NEW: drained-but-not-yet-resolved
}

// DrainUpTo peeks up to max txs in FIFO order, SKIPPING reserved hashes, and
// marks the returned hashes reserved before returning them.
func (p *PendingPool) DrainUpTo(max int) []*Tx

// Release clears the reservation for hashes that remain in pending (e.g. a
// batch whose results were handled: included/terminal are Remove()d, the rest
// are Released so they can be re-drawn).
func (p *PendingPool) Release(hashes []Hash)

// Remove deletes hashes from order/txs AND clears any reservation (unchanged
// callers; reservation-clearing is additive).
func (p *PendingPool) Remove(hashes []Hash)
```

Reservation lifecycle per batch: **Reserve** at drain (step a) → **Release** the
whole drained batch at step e (clears the reservation) → then `Remove` the
included/terminal txs as the serial path does (staying/excluded txs return to
unreserved pending for re-draw). At most **two** batches are reserved at once
(one authing, one warming) — matching the two open snapshots. The serial path
never reserves (its `DrainUpTo`→`Remove` ordering is unchanged), so it is
unaffected.

### 5. Why the safety invariant is PRESERVED (the crux)

The current invariant (executor.go): *cache entries change only at a batch
boundary on the executor goroutine, never mid-execution while warm workers
read.* The barrier keeps it because the **join at step d** guarantees warm(N+1)
has fully returned before any boundary mutation (steps e, f) runs. Sequencing:

- Steps b, c (overlap): warm(N+1) and auth(N) only READ the shared caches. auth(N)
  writes only to its own batch-local `overlayReader`, never the shared cache.
  RO-cache `stage` appends under `stageMu` (already concurrency-safe with `get`);
  RO-cache `get` bumps an atomic counter. Two goroutines reading the same Go maps
  with no concurrent writer is safe.
- Step d: barrier — warm(N+1) done. No warm reads remain in flight.
- Steps e, f: `ApplyWrites(N)`, eviction drain, `MaintainReadOnly` (the only
  structural mutations) run with NO warm reads in flight, and before the NEXT
  iteration's warm(N+2) starts. So mutation is still strictly between warm
  passes, exactly as today.
- warm(N+1) reading the pre-`ApplyWrites(N)` cache is fine: warming is
  best-effort and its results are discarded. auth(N+1) reads batch N's writes
  authoritatively from the write-cache after `ApplyWrites(N)` (step e, which
  precedes auth(N+1) next iteration). A key that batch N wrote and warm(N+1)
  pre-read from the QS view holds a stale QS value in the *view* cache only; the
  write-cache takes precedence in auth(N+1), so correctness is unchanged. Keys
  not pre-warmed are served in-memory from the write-cache anyway (no gRPC).
- Async cascade/commit resolution (`resolveInflight`/`cascadeFrom` on
  notification goroutines) only QUEUES evictions (`NoteInvalidated`/
  `NoteCommitted`) and re-adds pending txs (unreserved) — it never touches cache
  entries directly (unchanged). Its queued evictions drain at step f, same as
  the serial path's step 1. No new race.

### 6. Dual-snapshot lifetime & query-service capacity

At most 2 snapshots open at once (N in auth, N+1 warmed). Snapshot N+1 opens at
step b of iteration N, closes at step c/`AuthBatch` of iteration N+1 → lifetime ≈
one iteration (~90–170 ms) « `max-view-timeout` (10 s). `max-active-views`
(4096) and the sidecar concurrent-stream cap both comfortably allow 2. No stack
config change needed.

### 7. Edge cases (all resolved on the executor goroutine, deterministic)

- **Empty next drain** (step a, len 0): skip the warm launch; `warmedNext = nil`;
  after handling N, `waitForWork`, then re-establish the invariant by
  draining+warming the next non-empty batch inline (step g).
- **`AuthBatch(N)` endorse/server error** (step c): backoff; batch N's txs stay
  pending (Release them so they can be re-drawn); the `warmedNext` snapshot must
  still be **closed** to avoid leaking a view (`defer`/explicit `Close` on the
  handle), then re-drain next iteration.
- **`included(N) == 0`** (all excluded): don't submit; evict terminals; Release +
  Remove as appropriate; keep `warmedNext`.
- **Submit failure** (step e): `resolveInflight(false)` rolls back exactly as
  serial; the affected txs re-enter pending unreserved.
- **Shutdown mid-overlap**: cancel ctx; `<-warmFut`; `Close` both snapshots;
  Release any reserved txs; return.
- **First iteration**: no `warmed` yet → drain+warm the first batch inline, then
  enter the loop.

### 8. Opt-in wiring

`Gateway.SetPipelined(bool)` (default **false**). `runExecutor` dispatches to
`runExecutorPipelined` when set. Wire the flag through `gateway/app` and
`integration/test_helpers.go`; add a `-pipeline` perf-test flag (and/or
`PERF_PIPELINE` env knob) so only benchmarks exercise it until the rig proves it.
After validation, flip the default to true (follow-up commit).

## Validation plan (gates — must all pass before commit)

1. `go test -race ./gateway/core/... ./endorser/execution/... ./endorser/core/...`
   — new pipelined single-cycle unit tests (deterministic driver, fake endorser)
   covering the b–c–d–e–f–g ordering, the reservation lifecycle, and every edge
   case (snapshot closed / txs released on every path), plus the existing suites.
2. ec2 fresh-stack replay, **both datasets**, window 20000, batch 1024,
   code-default QS pool, `-pipeline`:
   - **synthetic** (conflict-free ceiling): must hold **20000/20000 committed, 0
     rolled back** AND report throughput.
   - **historic** (high-conflict): must hold **20000/20000 committed, 0 rolled
     back** AND report throughput.
3. Control comparison on the same stack with `-pipeline` OFF (serial path) —
   confirm the serial baseline is unchanged and measure the speedup. Ship only if
   pipelined is faster AND both datasets are green.

Target: approach the ~1.85× OVERLAP-SIM ceiling on synthetic (commit is not the
bottleneck, so the execution speedup should largely pass through to tx/s).

## Deferred alternative: snapshot-per-warm (stage 2)

If, after stage 1, warm ever exceeds auth at the operating point (e.g. larger
batches, colder cache, a slower QS, or high warm-latency variance), the barrier's
join at step d starts to delay `submit(N)` by `max(0, warm(N+1) − auth(N))`,
throttling the commit pipeline. Snapshot-per-warm removes that coupling: hand the
background warm an immutable frozen copy of the write-cache (a plain map copy —
"stage 1 map copy is sufficient"), so steps e/f (boundary + submit) can run
*without* first joining warm(N+1). It needs:

- a `CacheLayer` read interface defined in `endorser/execution` (import
  direction: `gateway/core` imports `execution`, not vice-versa),
- `VersionedCache.Snapshot()` returning a frozen map-copy implementing
  `CacheLayer`,
- the layered reader generalized to take a `CacheLayer` (live for auth, frozen
  for pipelined warm) with RO-staging disabled for the frozen warm.

Snapshot copy cost ≈ 1–2% of auth; acceptable. Not built in stage 1 because it is
throughput-equivalent while warm < auth and is strictly more code.

## Rejected alternatives

- **Lock (RWMutex) on `VersionedCache.entries`:** puts a lock on the hot warm+auth
  read path every batch. Both barrier and snapshot-per-warm are lock-free and
  avoid this.
- **Overlap without the step-d barrier, against the live cache:** would let
  warm(N+1) run through the boundary mutation, violating the
  no-mutation-during-warm-reads invariant without the frozen snapshot. That is
  exactly what snapshot-per-warm makes safe; doing it against the live cache
  needs a lock. Rejected in favor of the barrier (stage 1) / snapshot (stage 2).
- **Incremental auth as warm progresses:** auth needs all prior txs' ordered
  writes; it cannot start before its own batch's warm completes. No benefit.
- **Bigger batches to amortize warm:** blocked by the stack-side orderer stall
  above 1024 — a separate lever.
- **Reduce auth CPU:** near-irreducible EVM interpreter work; the code-hash cache
  already banked the cheap win.
