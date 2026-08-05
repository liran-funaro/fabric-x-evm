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

---

## 10. Run length is disk-bound, not time-bound (measured 2026-08-04)

A full-stack run writes **~6.3–6.5 KB of disk per committed EVM transaction**,
i.e. **~2.1 GB/min (~125 GB/h) at the ~5.7k tx/s headline**. `ec2` has a single
100 GB disk (~88 GB free after `clean-x`), so with ~12 GB of headroom the budget
is a fixed **~12 million EVM transactions per run**.

What is finite is the **transaction count, not the wall-clock time**, so duration
and throughput trade off directly:

| Rate | Max duration |
|------|-------------:|
| unpaced (~5,700 tx/s) | ~35–40 min |
| ~1,000 tx/s | ~3.5 h |
| ~460 tx/s | 8 h |

**Where it goes:** 9 synchronized copies of the block data — 4 orderer batchers +
4 assemblers + the committer sidecar ledger, each growing in lockstep (~1.3 GB
per copy per 5 min at full rate). Postgres (`committer-db`) stays ~400 MB: the
historic trace touches a bounded ~151k accounts, so **state is bounded while
history is not**. Container logs are irrelevant (~43 MB). That 9× replication is
the 4-party BFT property itself, and there is **no ledger pruning/retention
setting** in `testdata/shared_config.yaml` or the orderer config, so it cannot be
traded away.

**Implication:** any request for a multi-hour run at full throughput needs a
bigger volume — it is not a tuning problem. Compute the duration from the disk
budget (`usable_GB / 2.1 GB-per-min`) rather than picking a wall-clock target.
For deliberately long runs, `-target-tps` paces submission below the ceiling
(default 0 = unpaced, so every number above stays comparable).

Discovered while producing the client demo video
([demo-video.md](demo-video.md)); it is a property of the stack, not of the demo.

---

## 11. Throttled runs collapse at ~110k committer txs — cause NOT yet found (2026-08-04) ⚠️

**Symptom.** A deliberately throttled run (`-target-tps 500`, historic dataset,
serial executor) ran perfectly for **48 minutes** — 500 tx/s, ~14 EVM/batch, **0
rolled-back batches** — then produced bursts of MVCC aborts, recovered once, and
collapsed on the second burst: in-flight climbed to the 100 000 cap, commits went
to zero, and every committer tx was aborting (`rollbacks/s == committer tx/s`).
Final: 2 256 740/2 356 740 committed, **7 334 rolled-back batches**, 7 175
`gateway_spec_abort_total{class="unclassified"}`. The same code at full rate
(Run A: 1024 EVM/batch, 5 729 tx/s) committed **10.4 M txs with 0 rollbacks**, so
this is not cumulative state or history volume.

**What the evidence ruled out.**
- *Read path:* `queryservice_database_batch_queueing_time` stayed flat at 2–3 ms
  across the whole run, including through the collapse. Not QS/DB latency.
- *Crash / resource:* all 22 containers healthy, 16 GB disk still free, no OOM.
- *External CPU:* the Grafana renderer (running dashboard captures on the same
  host) sat at 0.2–0.3 % when the first abort landed; its peak was 0.83 of 32
  cores. Not observer interference.
- *Monotonic degradation:* the failure is **episodic** — 8 clean minutes between
  the first and second burst, with the rollback counter frozen — not a ramp.
  `endorse` p99 rose *with* the aborts (0.03 s → 0.45 s) because retries inflate
  batch size (QS batch query size 7.7 → 103 keys), so it is a consequence.

**Root cause.** `executeCycle` returns **without waiting for the commit**, and
each batch executes against the `VersionedCache` carrying the prior in-flight
batch's writes — so on the **serial** path too, batch N+1 may read N's
*uncommitted* writes, and the committer must therefore see dependent committer
txs **in submission order**. That is exactly the single-submitter invariant of §9,
but `orderedOrdererSubmitterCount` clamps submission to one worker only on the
**production** path (`gateway/app.buildApp`). `BuildGateway` — which the perf
harness uses (`integration/test_helpers.go`) — takes an explicit worker count, and
the harness flag `-orderers` **defaults to 64**.

Batch size is what hides or exposes it:

| Regime | Endorse | Batches queued at once | Reordering | Result |
|---|---|---|---|---|
| Full rate, 1024 EVM/batch | ~200 ms | one | no opportunity | 10.4 M txs, 0 aborts |
| Throttled, ~14 EVM/batch | ~30 ms | several | 64 workers race | abort → cascade |

Throttling does not conflict *directly*; it shrinks batches, which shortens
endorse, which lets several batches sit in `endorsementChan` simultaneously, which
gives 64 concurrent submitter workers the chance to deliver a dependent committer
tx ahead of its predecessor. `unclassified` is the expected abort class: this is a
submission-order abort, not one of the classified stale-read hypotheses.

### The submission-order hypothesis was TESTED AND REFUTED

`-orderers 1` (single submitter, verified on the process command line) did **not**
prevent it. The retry collapsed the same way:

| Run | `-orderers` | Onset | EVM txs at onset | **Committer txs at onset** |
|---|---|---:|---:|---:|
| B | 64 | t+48 min | ~1.44 M | ~102 000 |
| D | **1** | t+52 min | ~1.56 M | ~111 800 |
| A | 64 | never (10.4 M txs) | — | 10 166 *total* |

So concurrent submission is **not** the cause. What both throttled runs share is
the **committer-tx (batch) count at onset: ~102 000–112 000** — and Run A, which
was clean, never exceeded ~10 000 batches in its whole life. Run D's ledger height
at onset was 112 699, i.e. one block per committer tx.

The collapse shape is identical in both: in-flight sits flat at ~50 until the
cliff, then commits stop dead (Run D: frozen at 1 561 113 with in-flight climbing
past 72 000 and the batch counter frozen at 111 829). Run B recovered from its
first burst and ran 8 clean minutes before a second, terminal one; Run D did not
recover at all.

### What is still unknown

The trigger correlates with **cumulative committer-tx count**, not with elapsed
time, tx count, submitter concurrency, read-path latency, or host resources — but
that rests on only **two** data points, and "batch count" is confounded with "the
throttled small-batch regime" because no full-rate run has ever reached 100 k
batches. Do not treat ~110 k as a real constant yet.

### The batch-count lead is ALSO unsupported (1000 tx/s run, 2026-08-04)

Ran it. It refutes the batch-count reading too:

| Run | Rate | Onset | EVM txs at onset | Committer txs at onset | Avg batch |
|---|---:|---:|---:|---:|---:|
| B | 500 | t+48 min | ~1.44 M | ~102 000 | 14.0 |
| D | 500 (`-orderers 1`) | t+52 min | ~1.56 M | ~111 800 | 14.0 |
| Diag | **1000** | **t+36 min** | **2.08 M** | **71 379** | **29.2** |

At 1000 tx/s the executor produced **bigger** batches (29.2 vs 14.0 EVM/batch),
so the committer-tx *rate* was unchanged (~34/s vs ~36/s) — and onset arrived
**earlier in both time and batch count**. So no candidate is constant across the
three runs: elapsed time (48/52/36 min), EVM txs (1.44/1.56/2.08 M), and
committer txs (102/112/71 k) all differ. The collapse shape is identical every
time: in-flight flat, then commits stop dead and never resume.

`Replay complete: 2084305/2184305 EVM txs committed in 2544.3s across 71379
committer txs (avg 29.2 EVM/batch); 2568 rolled-back batches, 0 submit failures`

### Status: cause UNKNOWN. Two hypotheses tested, both refuted.

**Important confound to resolve first.** Every clean run (A, C, `final`) is a
**30-minute, full-rate** run, and every collapsing run is longer than 35 minutes.
Run length at full rate is capped near 30 min by the §10 disk ceiling, so no
full-rate run has ever been *given the chance* to reach t+36 min. The simplest
explanation consistent with all five runs is therefore **not** "throttling breaks
it" but "runs past ~35–50 minutes break it, and full-rate runs are too short to
show it." That would make this a general endurance defect rather than a
throttled-mode one — and it is untested, because testing it needs a bigger disk.

Ruled out by evidence: read path (QS queueing flat at 2–3 ms through the
collapse), crashes/resource exhaustion (all 22 containers healthy, ≥16 GB free),
observer CPU (renderer at 0.2 % at the first abort), submitter concurrency
(`-orderers 1`), and monotonic degradation (episodic — Run B ran 8 clean minutes
between bursts). Abort class is `unclassified`.

Next steps, in order: (1) attach a larger volume and run **full rate for 60+
minutes** to settle the confound above — this is the one experiment that
distinguishes "endurance defect" from "throttled-mode defect"; (2) capture the
gateway's `evm.batch=debug` ENDORSE-TIMING and the stale-read key classes across
the collapse, since `unclassified` means the existing classifier does not
recognize the pattern; (3) sample goroutine/heap profiles either side of the
onset for a per-batch leak.

Other candidates not yet excluded: a per-batch resource leak in the gateway
(goroutines/fds/cache entries), committer-side per-block state, or DB/orderer
degradation at a size threshold. `-orderers` was left at its default of 64 —
the invariant concern in §9 is real for the pipelined path, but it is **not**
what causes this.
