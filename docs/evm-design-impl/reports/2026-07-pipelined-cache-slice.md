# Pipelined-Cache Execution Slice — Outcome Report

**Branch:** `bft-redesign` (not pushed). **Commits (visible on the branch):**
`54faa4f` (warm-concurrency perf lift), `73bed3d` (single-submitter invariant
doc), `9ea5470` (genuine-abort test), `2d51b2d` (receipt-index skip +
last-write-wins), `5f5adf1` (txIndex guard) — plus the earlier in-flight
registry / notifier / cache-rebuild work that landed in this slice's squashed
history.

**Design:** [specs/2026-07-27-pipelined-cache-execution-design.md](../specs/2026-07-27-pipelined-cache-execution-design.md).
**Plan:** [plans/2026-07-27-pipelined-cache-execution.md](../plans/2026-07-27-pipelined-cache-execution.md).

> This report was extracted into tracked docs from the gitignored SDD scratch
> (`.superpowers/sdd/2026-07-27-pipelined-cache-execution/`) so the engineering
> rationale survives into future sessions. `findings.md` §9 is the curated
> summary; this file carries the full detail, including the traps a future
> engineer/test-author would otherwise re-discover the hard way.

---

## 1. What the slice does

The pipelined executor removes the per-cycle `awaitCommit` barrier. Each
`executeCycle` ([gateway/core/executor.go](../../../gateway/core/executor.go)):

1. `DrainEvictions` at the **top** of the cycle, so the cross-batch cache
   reflects confirmed commit/rollback outcomes before this batch endorses;
2. drains the pending pool and endorses batch N against a `VersionedCache`
   primed with N−1's **uncommitted, speculative** writes;
3. evicts terminal (pre-execution-rejected) txs;
4. `ApplyWrites` — writes N's results into the cache with predicted spec
   versions;
5. acquires one in-flight slot (bounded by `MaxInflight`, default 16);
6. `trackInflight` registers N in the in-flight registry;
7. removes included txs from pending, submits the merged Fabric tx, and
   **returns** — the next cycle starts immediately.

A per-TxID notifier ([gateway/core/notifier.go](../../../gateway/core/notifier.go))
resolves each committer tx asynchronously via `resolveInflight(txID, valid)`:
committed → finalize, invalid → cascade rollback. `ExecuteBatch` grew a fifth
return value: the merged-namespace RWS.

This is the concrete **execution-ahead-of-commit** mechanism that findings.md §7
names as the durable throughput lever — execution cadence is decoupled from BFT
commit latency. **It is gated by `Gateway.Pipelined`; the serial default path is
unaffected.**

---

## 2. The single-submitter invariant

**Root cause it addresses (plausibly THE old ~80 tx/s stall).** Because batch N
is endorsed against N−1's *uncommitted* cache writes, the committer must
validate/commit committer txs **in submission order**. The default
`BatchSubmitter` runs 16 worker goroutines draining the endorsement channel
concurrently; that lets a dependent committer tx reach the orderer *before* the
predecessor whose writes it read → the predecessor's writes are not yet in the
committer's world state → MVCC abort → cascade storm → throughput collapse.

**Fix.** On the production pipelined path, orderer submission is clamped to a
**single** worker regardless of `Gateway.SubmitterCount`:
`orderedOrdererSubmitterCount(configured, logger)`
([gateway/app/wiring.go:37](../../../gateway/app/wiring.go#L37)) returns 1 and
warns once if configured `> 1`. It is computed once in
[gateway/app/app.go](../../../gateway/app/app.go) and reused for both
`NewNetworkSubmitters` and `BuildGateway`. The invariant is documented on the
config field itself ([gateway/config/config.go:57](../../../gateway/config/config.go#L57)).
The bypass/test path clamps independently ([gateway/app/ordered_delivery.go](../../../gateway/app/ordered_delivery.go),
[integration/test_helpers.go](../../../integration/test_helpers.go)).

`73bed3d` additionally documents that the controllable-submitter test wrap
records a single `ctl.inner` and therefore also requires `SubmitterCount==1` —
relaxing the clamp would need a per-submitter inner. Non-production /
independent-workload submitters retain the multi-worker path.

---

## 3. MVCC abort → cascade → cache rebuild

**`cascadeFrom`.** On `resolveInflight(txID, false)`, the executor detaches the
contiguous suffix `[idx..end]` of the in-flight registry (under the inflight
lock, via a cap-limited slice so the detached suffix can't be mutated), and for
each departed batch: stops its timer, marks it invalidated, re-queues its
`included` txs to the pending pool, and releases exactly one in-flight slot. It
runs on the timer/notification goroutine and never touches the cache entries
directly.

**Why a rebuild is required.** `VersionedCache` keeps only the **latest** writer
per key. Naively dropping an invalidated later batch B would erase a key K that a
surviving *earlier* batch A also wrote — losing A's still-in-flight write. So
after a boundary drain that applied invalidations, the cache is **rebuilt** from
the surviving in-flight batches: re-apply their writes in submission order into a
fresh empty map. The deterministic spec-version math reproduces the exact
speculative state. `Rebuild` runs under a single cache-lock acquisition, atomic
with respect to readers.

**Rebuild is needed only on the invalidation path, never on commit.** Commits
resolve FIFO (earliest-first); a committing batch is the earliest in-flight, and
any key a later survivor also wrote already carries that survivor's `writerTx`,
so the committed-drop does not touch it.

**Concurrency rules (all five must hold):**
1. the cache is mutated only at the cycle boundary, on the executor goroutine;
2. `Rebuild` is atomic vs. readers under one cache lock;
3. exactly-once slot release — registry-removal-under-lock is the idempotency
   gate;
4. concurrent cascade is idempotent — the first caller to detach a suffix wins;
5. never hold the inflight lock and the cache lock at once — snapshot the
   registry, release, then `Rebuild`.

**Conservatism (deliberate).** The cascade re-queues the whole suffix with no
per-key cross-batch dependency tracking. A re-queued tx that already committed
re-executes as nonce-too-low and is terminally excluded (never double-committed);
a genuinely dependent tx could not have committed valid under MVCC. The hazard
test was proven genuine by first committing a no-op `Rebuild` stub (survivor A's
key K erased → tests fail), then the real `Rebuild` (pass).

Also note `trackInflight`'s backstop timer must be armed **inside** the locked
critical section (after append, before unlock) — arming after unlock races the
`b.timer` write against a concurrent `cascadeFrom`/`resolveInflight` read of the
same field (a genuine data race).

---

## 4. Receipt-index / txIndex fix (`2d51b2d`, `5f5adf1`)

**Root cause.** When a batch MVCC-aborts and re-executes in a later block, the
same `tx_hash` is delivered twice — once with `tx.Valid==false` (the aborted
attempt, delivered in an earlier block with `status=0`) and once successfully
(the re-commit). Two bugs let the aborted attempt permanently shadow the real
receipt:
- `ConvertToDomain` ([gateway/core/chain.go:137](../../../gateway/core/chain.go#L137))
  created a receipt/log row for **every** Fabric tx regardless of `tx.Valid`;
- `InsertTransaction`/`InsertLog` used `ON CONFLICT DO NOTHING` (first-write-wins),
  so the aborted row stuck.

**Fix — Part A.** `ConvertToDomain` now skips committer-invalid (`tx.Valid==false`)
txs — no receipt/log rows — on both the single-tx and merged-batch paths
([gateway/core/chain.go:161](../../../gateway/core/chain.go#L161)). The (empty)
block row is still inserted. (This gates on the committer verdict `tx.Valid`, NOT
on EVM revert: a reverted tx has `tx.Valid==true` and still gets a `status=0`
receipt.)

**Fix — Part B.** Last-write-wins upserts
([gateway/storage/query.sql](../../../gateway/storage/query.sql) +
`query.sql.go`, hand-edited in lockstep because sqlc was unavailable):
`InsertTransaction` → `ON CONFLICT (tx_hash) DO UPDATE`
([query.sql:60](../../../gateway/storage/query.sql#L60)); `InsertLog` →
`ON CONFLICT (tx_hash, log_index) DO UPDATE` ([query.sql:184](../../../gateway/storage/query.sql#L184));
`InsertBlock` stays `DO NOTHING`.

Parts A and B are complementary: skipping invalid txs means each `tx_hash` is
inserted exactly once (at re-commit), so `DO UPDATE` cannot collide with any
other unique constraint.

**txIndex guard (`5f5adf1`).** A skipped invalid tx must **not** consume a slot in
the flat block-global `txIndex` — a valid tx following an invalid one must land
at `TxIndex 0`. The guard is proven non-vacuous (test fails with the
`ConvertToDomain` skip disabled).

**Schema note.** The flat `TxIndex` is block-global, contiguous, unique, and
eth-correct — it is NOT Fabric's `tx.Number`. A merged Fabric tx expands to N EVM
sub-txs; `SubIndex` is the position within the Fabric tx, and the `fabric_tx_id`
UNIQUE constraint was dropped (one `FabricTxID` → N txs; `tx_hash` is the
identity).

---

## 5. Genuine-abort integration test (`9ea5470`)

`integration/batch_test.go:714` (`TestGenuine…`) forces a real committer MVCC
abort **deterministically**, with no orderer-timing race:

- `NewLocalTestHarnessWithSubmitterControl`
  ([integration/controllable_submitter_test.go:119](../../../integration/controllable_submitter_test.go#L119))
  forces `SubmitterCount=1` and wraps the single submitter with a
  `controllableSubmitter`.
- The test buffers the first `holdN` `Submit` calls; `ReleaseReversed`
  ([controllable_submitter_test.go:99](../../../integration/controllable_submitter_test.go#L99))
  forwards them to the inner submitter in **reverse** order, so batch N+1 (tx1)
  lands in block 2 and batch N (tx0) in block 3.
- Block-sync delivers block 2 first; the committer validates N+1 against its own
  still-pre-tx0 world state → marks it invalid → `gw.Handle` → `resolveInflight(false)`
  → cascade → tx1 re-executes and re-commits.

Determinism comes from `SubmitterCount=1` FIFO + synchronous `ReleaseReversed`.

**Key insight:** even with the receipt bug present, the underlying **ledger
state was always correct** (final nonce = 2, recipient balance = 2× value) — it
was a receipt/query-layer shadowing bug, not a double-spend. Final passing
assertions: `Cascade==1`, `Inflight==0`, both receipts `0x1`, nonce 2, balance
2× value.

**`Arm()` gate.** The wrap needs an explicit `Arm()`
([controllable_submitter_test.go:64](../../../integration/controllable_submitter_test.go#L64)):
`NewStatePrimer` submits its priming commit through the **same** wrapped
submitter as the gateway's `BatchSubmitter`. Buffering unconditionally from
construction time deadlocks priming's blocking `Commit`; before `Arm()`, `Submit`
passes straight through. (Mutex discipline: release the lock before the blocking
inner `Submit`.)

---

## 6. Notifier + query-service fallback (soundness traps)

The per-TxID notifier replaces the old blind-timeout rollback. `Watch(txID)`
arms a client timer and registers the txID on the subscribe channel **before**
`SubmitFabricTx` (register-then-submit, so no notification is missed). When a
notifier is wired, `trackInflight` arms **no** `time.AfterFunc` (the notifier
owns the timeout); when the notifier is nil (the production `app.go` path today),
it keeps the `AfterFunc` backstop.

**Fallback soundness.** If a notification is dropped, `queryCommitStatus`
([gateway/core/api.go:411](../../../gateway/core/api.go#L411)) adjudicates by
reading the **raw committed `query.Store`** — the unwrapped second return of
`NewEndorserCore`, **not** the speculative `cacheWrap`. Reading the cache would
be the key trap. Fabric-X uses only `BlockNum` as a per-key monotonic version,
matching `applyLocked`'s spec-version math: a batch is committed iff for every
written key `committedVersion >= specVersion`. `specVers` is captured by readback
right after `ApplyWrites` (single-writer-safe on the executor goroutine), NOT at
fallback time (a later writer could overwrite the entry).

**Two fixed bugs:**
1. **False "committed" in the dangerous direction.** With an empty `specVers`
   (loop vacuously true) or a spec==0 key (`v >= 0` trivially true),
   `queryCommitStatus` would return `true` for an **un**committed batch and
   `NoteCommitted` it — dropping its txs with no cascade (unrecoverable). Only
   unreachable via the EVM read-modify-write invariant (every tx bumps its nonce;
   every written key is read first). Guarded with `len(specVers)==0 → false`.
2. **Executor wedge on a hung QS.** `queryCommitStatus` ran with
   `context.Background()` synchronously inside the serial `Handle`; with
   `ViewTimeout` default 0 (unbounded), a hung query service wedges the executor.
   Fixed with `context.WithTimeout(commitTimeout || default)`.

`STATUS_UNSPECIFIED` from the sidecar is the commit-timeout sentinel → resolve
via fallback. Any read error / missing key / below-spec version → `false`
(conservative rollback; self-corrects via MVCC). The exactly-once resolution
gate is a lock-guarded check-and-delete of `timers[txID]`, shared by
`Handle`/`onTimeout`.

**Handler wiring.** `AllTxStreamer` is load-bearing (it feeds the endorser DBs
their committed state in memory mode) and stays; the slice removes `gw` from its
handler list and adds the per-TxID notifier feeding `resolveInflight`. Receipts
do **not** flow through the gateway block handler (`Handle`/`HandleTx` only call
`resolveInflight`; receipts flow via the chain store + execution), so a
notifier-mode harness can safely drop `gw` from the block handlers.

---

## 7. Spec-version base backfill + a critical error-check bug

`VersionedCache.ApplyWrites` needs a real committed base version for every
written key; the "absent-from-read-set ⇒ spec version 0" fallback is only correct
for a genuinely-absent key. `StateDB.Result()` now backfills every write key
lacking a read via `getStateFromStore(key)`, recording the version exactly as a
normal read does.

**Critical latent bug this introduced.** The backfill read routes failures
through `setError`, but `classify()` never re-checked `state.Error()` after
`Result()` → a stale-view backfill read would silently drop the key and endorse
an **incomplete** RWS instead of aborting. Fixed by checking `state.Error()`
after each `state.Result()` in `classify()`.

Endorser micro-benchmarks after this change stay high: `ExecuteBatch_128`
≈207–214K tx/s, `Execute_1` ≈446–467K tx/s.

---

## 8. Test-harness fidelity trap (for future test authors)

In `fabrictest`-based local harnesses, the committer's `RecordGetter` must be
**`nil`** so the committer validates MVCC against its **own** synchronously-committed
world-state DB. Passing `endorsers[0].KVS` (the endorser DB, updated
*asynchronously* by the block synchronizer) makes a pipelined dependent tx get
validated before its predecessor's block reaches the endorser DB → spurious MVCC
conflicts, especially under `-race`. This fix also changed the **shared**
`NewLocalTestHarnessWithFactory` — blast radius is every local block-sync test.

**No-serialize-on-commit proof (deterministic).** The in-flight slot is acquired
**before** `trackInflight`→`Watch`, so with `MaxInflight=4` a full window makes
the 5th cycle block on the semaphore before it can `Watch` → `Watched()`
plateaus at exactly 4. A serialize-on-commit executor would stall at 1.

---

## 9. Known-good / environmental

- All five visible HEAD commits (`54faa4f`, `73bed3d`, `9ea5470`, `5f5adf1`,
  `2d51b2d`) are **local-only** (never pushed); the branch is kept as-is for live
  testing rather than merged.
- The pre-existing `-race` fire in `google.golang.org/grpc/grpclog.SetLoggerV2`
  (a global logger set by one test's setup while another tears down a connection)
  is unrelated — it reproduces on clean checkouts.
- The integration tests `TestEthereumTests`, `TestFablo`, `TestFabricX`,
  `TestSingleAdd11` (and `perf/TestShared`, `TestReplayJSONDataset*`) fail
  identically on clean checkouts (missing MSP keystore / uninitialized
  `testdata/ethereum-tests` submodule / missing crypto fixtures) — environmental,
  not regressions.
