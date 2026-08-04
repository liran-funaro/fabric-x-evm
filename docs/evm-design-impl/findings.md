# EVM-on-Fabric-X — Implementation Findings

Curated, durable findings from the performance-optimization work on the
two-phase batch-execution redesign. This is the file a new agent reads first
to understand **where the throughput went, what moved it, and what is still
open**. It is deliberately honest about the one unsolved problem (the
warm/auth pipeline).

> `evm-design/` is the user's **read-only** design document. Nothing here
> modifies it. Implementation findings, plans, specs, and reports live under
> `docs/evm-design-impl/` only.

**Cross-references**
- Design specs: [specs/](specs/) — the brainstormed designs (drop-internal-state-DB, two-phase execution, pipelined-cache, warm/auth pipelining).
- Implementation plans: [plans/](plans/) — the task-by-task plans those specs became.
- SDD reports: [reports/2026-07-exec-hotpath-optimization.md](reports/2026-07-exec-hotpath-optimization.md), [reports/2026-07-stage2.1-final-fix-report.md](reports/2026-07-stage2.1-final-fix-report.md).
- Run-book: [experiments.md](experiments.md) · Server setup: [server-setup.md](server-setup.md).

---

## 1. The redesign in one paragraph

The EVM gateway no longer keeps a synced internal state DB. Endorsers read
committed world state on demand from the Fabric-X **query service** under a
pinned view, and each batch of EVM txs is executed in **two phases**: a
concurrent **WARM** pass (one goroutine per tx discovers the keys it will
touch and bulk-primes them into a per-batch read cache) followed by a
**serial, in-order AUTHORITATIVE** pass over that primed cache (which
produces the MVCC read/write set that goes to the committer). A whole
execution batch is merged into **one** Fabric committer tx. See
[specs/2026-07-23-drop-internal-state-db-design.md](specs/2026-07-23-drop-internal-state-db-design.md)
and
[specs/2026-07-24-two-phase-batch-execution-design.md](specs/2026-07-24-two-phase-batch-execution-design.md).

---

## 2. Throughput journey (native ec2, x86_64 32 vCPU / 61 GB)

All numbers are the USDC replay, 50k txs, fresh stack per config, `0 rolled
back`, single-gateway sequential replay. The headline is EVM tx/s (individual
transfers); one merged batch == one committer (Fabric) tx.

| Milestone | EVM tx/s | What changed |
|-----------|---------:|--------------|
| Original knee (colima/QEMU, bs≤128) | ~15–80 | Read-concurrency + query-batching misconfiguration (not a hardware limit) |
| Warm-pass concurrency = batch size + QS window tuning | ~350–490 | `WarmWorkers = len(txs)`; QS `min-batch-keys`/`max-batch-wait` retuned |
| Native hardware (escape QEMU emulation tax) | ~1814 | Same code on real x86_64; 3.7× purely from per-block/per-read latency |
| Batch-size convergence (bs 512–1024) | ~2161 | Bigger batches amortize per-batch overhead |
| Code-hash cache (`58a0df8`) | ~2588–2662 | Cache immutable `keccak256(code)` per engine; cuts GC pressure globally |
| Read-only (MFU) cache (`1ec2cd7`) | **~3665–3815** | Serve the ~9 globally-hot immutable keys from memory; collapse warm-pass gRPC |
| + GC env knob (GOGC=400–800 + GOMEMLIMIT) | ~4085–4210 | Deployment knob, **zero code change** |
| Warm-workers restore + QS low-latency batching (`54faa4f`) | **~4.9k–6.0k** | `WarmWorkers=len(txs)` re-applied (a GOMAXPROCS cap, added as a scheduler-churn optimization in the fast in-memory regime, had regressed it in the real-QS I/O regime); QS `min-batch-keys` 1024→256, `max-batch-wait` 100ms→5ms so a single gateway's read wave flushes immediately instead of eating the 100ms window |

Cumulative code-side gain over the two-phase ceiling: **~2161 → ~3815 tx/s
(+77%)**, then a further **~+10%** from the GC env knob (deployment-time, no
code), then the warm-workers restore + QS batching (`54faa4f`) lifted the
native ec2 headline to **~4.9k–6.0k EVM tx/s** (the current serial headline).
Latest verified run (2026-08-04, ec2, `PERF_REPLAY_WINDOW_SIZE=50000`,
bs=1024, `GOGC=500 GOMEMLIMIT=48GiB`, 0 rolled back both datasets): **synthetic
4872 tx/s** (50000/50000 in 10.3s) / **historic 5617 tx/s** (50000/50000 in
8.9s); a prior same-week run hit 5.2k / 6.0k (normal shared-box variance —
prefer reporting a range). The synthetic (conflict-free) lift is `54faa4f`'s
warm/QS change; the `73bed3d`/`9ea5470`/`5f5adf1`/`2d51b2d` commits are
correctness for the pipelined/high-conflict path (§9), not raw-throughput
levers. See [[evm-perf-stall-findings]] for the full run log and
[[evm-current-serial-headline]].

---

## 3. Where the time goes (profiled, `evm.batch=debug` ENDORSE-TIMING)

Per full batch at bs=1024 **with** the code-hash + RO caches in place:

- **WARM ≈ 124–130 ms wall** — I/O-bound on query-service `GetRows`. ~14 336
  reads/batch across 1024 concurrent workers; RO-cache serves the hot keys
  free, but the ~36% long-tail per-account keys are unavoidable single reads.
- **AUTH ≈ 85–100 ms wall** — ~100% serial EVM CPU (readtime only a few ms;
  warm pre-filled the view cache). ~98 µs/tx. USDC is a proxy contract, so
  `DelegateCall` proxy→impl dominates.
- **snapshot ≈ 0.5 ms.**

**Warm and auth are now balanced.** Both scale ~linearly with batch size, so
**batch size is not a throughput lever** above ~1024 — it only changes how
many committer txs / orderer blocks a run produces. This corrected an earlier
"warm is ~constant → big batches converge toward ~10k" model, which was wrong.

**The read path is not resource-bound.** During a live replay: Postgres cache
hit **99.995%** (0 disk reads), committer-db ~0.2 cores, query-service ~1–2 of
32 cores, ≤2 of 10 DB connections in use. The warm ceiling is the
endorser→QS **transport/serialization** path and QS read-processing, not
DB/disk/CPU/connection saturation. See [[evm-qs-db-resource-discovery]] and
[[evm-lock-profiling-findings]] (which also **disproved** a redundant-cache-lock
hypothesis — do not pursue a lock-removal/COW refactor; those RLocks cost ~0).

**Two measurement lenses — don't conflate them.** The warm/auth split above is
the *endorser's* server-side `ENDORSE-TIMING` debug log. The *gateway's*
Prometheus phase histograms are mode-dependent, because the serial executor
issues one fused `ExecuteBatch` gRPC (warm+auth happen back-to-back inside the
endorser, one call) while the pipelined executor issues separate
`WarmBatch`/`AuthBatch` RPCs:
- **serial (default)** → `gateway_endorse_phase_seconds` (the combined
  warm+auth cost, ≈165–190 ms/batch on ec2); `gateway_warm_phase_seconds` and
  `gateway_auth_phase_seconds` are **empty**.
- **pipelined (`-pipeline`)** → `gateway_warm_phase_seconds` +
  `gateway_auth_phase_seconds`; `gateway_endorse_phase_seconds` is **empty**.
So on the default (serial) dashboard the populated series is *endorse*, not
warm/auth — the gateway cannot split a single fused RPC.

---

## 4. Committed optimizations (branch `bft-redesign`, not pushed)

| Commit | Change | Effect |
|--------|--------|--------|
| `6221599` | endorser→QS **connection pool** (round-robin GetRows over N conns, default 8) | +~20% peak; knee at 4–8 conns, N=32 regresses (per-worker conns would be worse) |
| `58a0df8` | **code-hash cache** on `EVMEngine` (cache immutable `keccak256(code)`, version-validated) | +~23–24%; outsized because it slashes GC across all warm workers |
| `1ec2cd7` | **read-only MFU cache** (serve globally-hot immutable keys; evict-on-write, re-admit; boundary-only mutation) | +~42%; zero new MVCC aborts |
| `92d9ba8` | arma `RequestMaxBytes` 1 MB → 16 MB (+ Preferred/Absolute bumps) | **Real liveness fix**: unblocks batches >~1024 EVM txs from a total stack stall |
| `3ca917a` | query-service rate-limit disabled (`requests-per-second: 0`) | Benchmark config; warm burst approaches the 5000 rps DoS default |
| `54faa4f` | warm-pass concurrency = batch size (`WarmWorkers=len(txs)`) + QS `min-batch-keys` 1024→256 / `max-batch-wait` 100ms→5ms | Current serial headline lift to **~4.9k–6.0k** tx/s; restores RQ1 ("phase-1 concurrency = batch size") after a GOMAXPROCS cap had regressed it in the real-QS regime |

The GC setting is **not** hardcoded — Go reads `GOGC`/`GOMEMLIMIT` from the
env, so it is a deploy-time knob (recommended `GOGC=400–800` + `GOMEMLIMIT`
sized to host). Hardcoding aggressive GC would surprise operators / risk OOM.

**`RequestMaxBytes` is a production concern, not just a benchmark tweak:**
production sets the batch cap from `Gateway.MaxBatchSize`, where `0 =
unbounded drain-all`, so any large/unbounded-batch deployment hits the exact
>1 MB-merged-tx freeze under burst load without this fix.

---

## 5. Environment findings (colima vs native)

The single biggest confound in this project was the **local docker
environment**, not the code:

1. **colima `mountType: sshfs`** wedged under the orderer's block-ledger
   appends (a ~330× latency cliff on one block). Fixed by moving all
   committer+orderer data to docker **named volumes** on the VM's ext4 disk
   (`6792cca`; `HOST_DATA=1` opts back into host bind-mounts for inspection).
   +36% sustained.
2. **Under-provisioned colima VM (2 CPU / 2 GB on a 64 GB host)** caused a
   hard commit-freeze at ~42k txs (memory exhaustion → docker daemon
   unresponsive → gateway sees `invalid or stale view` → `DeadlineExceeded`).
   Fixed by `colima start --cpu 8 --memory 32`. **Memory** removed the wedge;
   **CPU count did not raise steady throughput** (that was BFT commit cadence
   under QEMU).
3. **Native x86_64 (ec2)** removed the QEMU emulation tax entirely: identical
   block count, **3.7×** the throughput. This is why all authoritative
   numbers are taken on native hardware.

Lesson for a new agent: **run experiments on native hardware** (ec2 or
equivalent). Treat colima numbers as relative-curve only, and suspect the
environment before the code when throughput cliffs appear.

---

## 6. The warm/auth **pipeline** — UNSOLVED (a trilemma) ⚠️

> This is the most important open item and the easiest to get wrong. The
> plan and older notes framed this optimistically ("livelock = query-service
> commit-visibility lag; the `EVM_PIPE_EVICT_HOLD_DEPTH` fix is clean and
> faster than serial"). **That framing is stale and was disproven.** What
> follows is the current, honest state as of **2026-08-03**. See
> [[evm-warm-auth-pipeline-findings]] and
> [specs/2026-07-29-warm-auth-pipelining-design.md](specs/2026-07-29-warm-auth-pipelining-design.md).

**The idea.** Because warm(≈78–130 ms) ≈ auth(≈85–100 ms) and auth must stay
serial for MVCC ordering, overlapping **warm(N+1) I/O behind auth(N) CPU**
should roughly halve per-batch wall (ceiling ~1.7–1.85×). `Gateway.Pipelined`
(the `-pipeline` flag) is the opt-in executor for this; **default is OFF, so
the serial production path is unaffected.**

**Why it is hard — the trilemma.** Auth reads via a *reopened* query view.
You can have any **two** of:
1. **overlap** warm(N+1) with auth(N) — the whole point;
2. **auth reads current committed state** — correctness (read-versions must
   match what the committer sees);
3. **auth reuses warm's primed reads** — the pipeline's only measured speedup.

`(1)+(3)` → stale reads → livelock; `(1)+(2)` → re-fetch everything → 28–46×
*slower* than serial and stalls at bs=1024; `(2)+(3)` → serial (no overlap).

**Five fixes, all failing the same bistable symptom** (bs=128 reliability
gate, "never livelock at any batch size AND never slower than serial"):
empty-cache reopen → view clone → selective clone invalidation (fix A) →
fixed-count eviction hold (`EVM_PIPE_EVICT_HOLD_DEPTH`, fix B) → nil-view.
`EVM_PIPE_EVICT_HOLD_DEPTH` looked viable on a single run but was
**non-reproducible on the confirmation sweep (1 clean / 4)** — do not trust
the old "clean + faster than serial" claim. **nil-view** (`EVM_QS_NIL_VIEW`,
read query-service *current-committed* state, removing the 100 ms
view-aggregation lag at its source) has **no serial regression** and is clean
**2/3** runs at ~1.44× — a **keeper ingredient but not sufficient** on its own.

**The decisive diagnostic (v2 direction-bucket sweep, `common/pipediag`).**
Instrumentation bucketed every read of an aborted batch by the direction of
its mismatch vs acked-committed. Across 6 runs (4 livelocks, 2 clean):
- `qslag_total = 0` in every livelock → **H1 (QS visibility lag) REFUTED.**
- `under = 0` in every livelock → the **stale-read / under-read mechanism is
  REFUTED.** This is the *exact* mechanism all five prior fixes targeted.
- The residual conflict is a speculative **OVER-read**: the write-cache spec
  version (`apply()` predicts `committed_version = readBase + chain_position`,
  correct only if submission order == commit order with no chain member
  aborting). When that breaks, downstream chained readers record versions
  **above** what commits → MVCC abort → cascade (matches the bistable
  retry-storm signature).
- **Caveat:** the `over` aggregate is dominated by cascade *victims*, so it
  confirms propagation, not the first ignition. But `under=0` rules out
  staleness as either — so ignition is most consistent with **commit-order ≠
  submission-order** or a partial in-chain abort.

**Fix direction (NOT chosen — user decides; do NOT build a 6th fix
unilaterally).** The problem is the spec-version **chaining/speculation**
layer, the *opposite* direction from every prior fix. Candidates:
(a) never let auth record a spec version above committed for a not-yet-committed
predecessor; (b) **guarantee commit order == submission order** (connects to
the single-submitter invariant / controllable-submitter work — commits
`54faa4f`, `73bed3d`, `9ea5470`, `2d51b2d` on `bft-redesign`); (c) detect
chain mis-prediction and re-derive read versions. The only *provably-correct*
untried candidate is **per-key visibility-gated eviction** (don't evict a
committed write until a fresh nil-view read confirms `Version ≥ committed`),
which nil-view makes cheap.

**The ordered-gate / reorder hypothesis was explored and set aside** — it was
a proxy for "has the view caught up," and the v2 diagnostic shows staleness is
not the mechanism. The disproven deferred-eviction plan
(`glistening-sniffing-hollerith.md`) is dead.

**Bottom line for a new agent:** the serial executor is healthy and is the
shipped default. The pipeline is opt-in, unsolved, and its performance claims
in older docs are not reliable. Before touching it, re-read
[[evm-warm-auth-pipeline-findings]] in full and get a user decision on fix
**direction** — the trap is re-attacking stale reads, which the data says is
the wrong direction.

---

## 7. Levers ruled out (don't re-try without new evidence)

- **QS→Postgres connection pool** (max-connections 10→40): no-op; QS coalesces
  reads over ~2 connections. The warm ceiling is read-*processing*, not
  connection concurrency.
- **warmWorkers cap** (< len(txs)): throughput collapses below ~256; no
  per-worker-EVM-reuse benefit — the alloc/GC churn is hidden behind warm I/O.
- **min-batch-size knob**: no-op for the flooded perf test (n is always full);
  only bites under real paced/bursty arrival.
- **gRPC MaxRecvMsgSize** (already 100 MB): never the batch-size stall; that
  was `RequestMaxBytes` (§4).
- **Lock removal / atomic.Pointer COW** on the caches: profiled at ~0 cost.

The one durable remaining code lever is **execution-ahead-of-commit / overlay
carry-forward** (execute N+1, N+2… against `[last snapshot ⊕ accumulated
writes]` without waiting for each batch to commit through BFT), which
decouples execution cadence from commit latency and cuts warm reads. It is a
real redesign, speculative (needs rollback on committer MVCC-abort + a size
bound), and awaits user go-ahead.

---

## 8. Known pre-existing issues

- **VersionedCache RPC race** ([[evm-versioned-cache-rpc-race]]): a `-race`
  failure between an RPC-query `Read` and commit-apply on the cross-batch
  cache. Reproduces on baseline (not introduced by pipeline work); deferred.
- **`overlayReader.Get` delete-then-read version** ([[evm-bft-redesign-project]]):
  an in-batch delete of a pre-existing key drops `under.Version`; dormant
  while EOV uses N==1, to be fixed when the OEV/BFT slice exercises N>1.

---

## 9. The pipelined-cache slice — single-submitter invariant, MVCC-abort cascade, receipt fix

> The `-pipeline` executor of §6 overlaps **warm(N+1)‖auth(N)** and is unsolved.
> This section is a *different* piece of work: the **pipelined-cache** slice
> (`Gateway.Pipelined`), which removes the per-cycle commit barrier so batch N is
> endorsed against N−1's *uncommitted* speculative writes — the concrete
> **execution-ahead-of-commit** lever §7 names. It is opt-in; the serial default
> is unaffected. Commits `54faa4f` (perf), `73bed3d`, `9ea5470`, `5f5adf1`,
> `2d51b2d` on `bft-redesign`. Full detail (notifier fallback, spec-version
> backfill, harness traps, concurrency rules) in
> [reports/2026-07-pipelined-cache-slice.md](reports/2026-07-pipelined-cache-slice.md);
> design in [specs/2026-07-27-pipelined-cache-execution-design.md](specs/2026-07-27-pipelined-cache-execution-design.md).

**Single-submitter invariant — the likely root cause of the old ~80 tx/s stall.**
Because N is endorsed against N−1's uncommitted cache writes, the committer must
validate/commit committer txs **in submission order**. The default
`BatchSubmitter` drains its endorsement channel with 16 concurrent workers, which
lets a dependent tx reach the orderer before the predecessor whose writes it read
→ MVCC abort → cascade storm → collapse. Fix: on the pipelined path, orderer
submission is clamped to a **single** worker regardless of `Gateway.SubmitterCount`
(`orderedOrdererSubmitterCount`, warn-once if >1; documented on the config field).
`73bed3d` records that the controllable-submitter test wrap likewise requires
`SubmitterCount==1`.

**MVCC abort → cascade → cache rebuild.** `cascadeFrom` detaches the contiguous
in-flight suffix `[idx..end]`, re-queues each departed batch's included txs, and
releases one slot each. Because `VersionedCache` keeps only the **latest** writer
per key, dropping an invalidated later batch would erase a key a surviving earlier
batch also wrote — so after an invalidation the cache is **rebuilt** from the
surviving in-flight batches, re-applying their writes in submission order. Rebuild
is needed only on invalidation, never on commit (commits resolve FIFO; the
committing batch is the earliest in-flight). The cascade is deliberately
conservative: it re-queues the whole suffix; an already-committed re-queued tx
re-executes as nonce-too-low and is terminally excluded (never double-committed).

**Receipt-index / txIndex fix (`2d51b2d`, `5f5adf1`).** When a batch aborts and
re-executes later, the same `tx_hash` arrives twice — once `tx.Valid==false`
(aborted, `status=0`) and once at re-commit. Previously `ConvertToDomain` wrote a
receipt for every Fabric tx and `InsertTransaction`/`InsertLog` used `ON CONFLICT
DO NOTHING`, so the aborted attempt permanently **shadowed** the real receipt.
Fix: `ConvertToDomain` skips committer-invalid txs (no receipt/log rows; block row
still inserted), and the inserts became last-write-wins (`DO UPDATE`; `InsertBlock`
stays `DO NOTHING`). A skipped invalid tx must **not** advance the flat
block-global `txIndex` (guard `5f5adf1`; proven non-vacuous). This gates on the
committer verdict, not on EVM revert (a revert has `tx.Valid==true` → `status=0`
receipt).

**Genuine-abort test (`9ea5470`).** `NewLocalTestHarnessWithSubmitterControl`
(`SubmitterCount=1`) buffers submits and `ReleaseReversed` sends them out of order,
so N+1 lands in an earlier block than N; block-sync delivers N+1 first, the
committer MVCC-aborts it, the cascade re-executes and re-commits it. **Key
insight:** even with the receipt bug present, the ledger state was always correct
(final nonce and balance right) — it was a receipt/query-layer bug, not a
double-spend.
