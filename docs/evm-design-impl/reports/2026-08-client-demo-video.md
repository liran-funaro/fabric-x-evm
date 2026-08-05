# Client Demo Video — Results (2026-08-04)

What was built, what the runs measured, and the two findings the work turned up.

Design: [../specs/2026-08-04-client-demo-video-design.md](../specs/2026-08-04-client-demo-video-design.md) ·
Plan: [../plans/2026-08-04-client-demo-video.md](../plans/2026-08-04-client-demo-video.md) ·
Run-book: [../demo-video.md](../demo-video.md)

---

## 1. Deliverables

All under `$EVM_PERF_DATA/demo/<label>/video/` on the experiment host.

| Run | Config | Artifact | Status |
|---|---|---|---|
| **A — headline** | full rate, 30 min, `-orderers 64` | `demo-full.mp4` (1× real time, 30 min, 8.1 MB), `demo-highlight.mp4` (86 s) | **client-ready** |
| **B — endurance (first attempt)** | 500 tx/s, `-orderers 64` | `demo-full.mp4` (85 min), `demo-highlight.mp4` | **not client-ready** — records the stall of §4; kept as evidence |
| **D — endurance retry** | 500 tx/s, `-orderers 1` | none (stopped) | **failed** — collapsed at t+52 min, refuting §4's hypothesis |
| **C — full-stack headline** | full rate, 30 min, both dashboards | `demo-full/-highlight`, `stack-full/-highlight` | superseded by `final` |
| **`final` — re-render** | full rate, 30 min, both dashboards, **no in-flight panel** | `demo-full/-highlight`, `stack-full/-highlight` | **SHIPPED — show this one** |

Both videos are 1920×1080 H.264 / yuv420p with an ffmpeg-drawn title card, timed
captions and a closing totals card. The full video is **1× real time and unedited
in pace**; the pipeline asserts its body duration equals the run duration within
1%, because a silently sped-up "real time" video is invisible to the eye on a
slow-moving dashboard.

Two dashboards, both filmed from the same stored run:
- `evm-demo` — client-facing: EVM tx/s, total committed, elapsed runtime,
  failures, throughput, commit latency, committer tx/s, in-flight, failures/s.
- `evm-demo-stack` — committer + orderer: ledger block height, consensus
  decisions/s, blocks/s, invalid transactions, committer throughput by verdict,
  and per-party consensus / assembler / batcher-mempool plus committer pipeline
  queue depths.

Palettes were validated with the dataviz validator against Grafana's actual dark
panel surface `#181b1f`, not chosen by eye (worst adjacent CVD ΔE 9.4 for the
client set, 8.4 for the 4-party set; latency ramp monotone, single hue).

## 2. Run A — the throughput headline (verified)

```
Replay complete: 10408713/10408713 EVM txs committed in 1816.9s across
10166 committer txs (avg 1023.9 EVM/batch); 0 rolled-back batches,
0 submit failures | 5729 EVM tx/s
Commit-path timing: submit->commit avg 129ms max 413ms | peak in-flight 2/16
```

- **10.4 million real Ethereum transactions** (Jan-2020 USDC trace, replayed
  continuously) in 30 minutes at **5,729 EVM tx/s**.
- **Zero** rolled-back batches, **zero** submit failures.
- In-flight held at exactly 100,000 for the whole run — the closed loop
  (`-max-outstanding`) is precise, and `submitted − committed == 100,000` exactly,
  so the release accounting is balanced.
- Verified: body 1816 s == run 1817 s (1× real time); closing-card totals agree
  with the replay log to 0.036% (the gap is commits landing after the last scrape).

## 3. Run C — the shipped deliverable (client + full-stack, verified)

```
Replay complete: 10394318/10394318 EVM txs committed in 1817.8s across
10152 committer txs (avg 1023.9 EVM/batch); 0 rolled-back batches,
0 submit failures | 5718 EVM tx/s
Commit-path timing: submit->commit avg 128ms max 450ms | peak in-flight 2/16
```

Four videos, all 1× real time verified (body 1818 s == run 1818 s) with totals
cross-checked to 0.026%: `demo-full` / `demo-highlight` (client dashboard) and
`stack-full` / `stack-highlight` (committer + orderer). **This is the set to show
clients.**

**It reproduces Run A to within 0.2%** (5,718 vs 5,729 tx/s; 10.39 M vs 10.41 M
txs; zero rollbacks both times) — two independent runs of the same config, which
is worth more than one.

The full-stack video shows ledger height climbing, invalid transactions pinned at
zero, committer pipeline queues flat, and all four BFT parties tracking in
lockstep. One honest detail it surfaces: batcher mempools drift up to ~25 entries
over 30 minutes (party2, the leader, stays at 0) — negligible in absolute terms
but a real upward trend worth watching on a longer run.

## 3b. Run D — the endurance retry FAILED (and refuted the hypothesis)

2.5 h at 500 tx/s with `-orderers 1`, verified on the process command line. It
collapsed at t+52 min exactly like Run B: commits froze at 1 561 113, in-flight
climbed past 72 000, batch counter stuck at 111 829, 877 rolled-back batches and
climbing. It did **not** recover (Run B had recovered from its first burst).

Stopped early rather than spend 90 minutes rendering a second stall. Evidence
kept at `~/stall-evidence/` on the host. See §4.

## 4. Finding — throttled runs wedge at ~110k committer txs (cause UNKNOWN)

Full detail in [findings.md §11](../findings.md).

| Run | `-orderers` | Onset | EVM txs | **Committer txs at onset** |
|---|---|---:|---:|---:|
| B | 64 | t+48 min | ~1.44 M | ~102 000 |
| D | **1** | t+52 min | ~1.56 M | ~111 800 |
| A / C (full rate) | 64 | never | 10.4 M clean | ~10 150 *total* |

**My first attribution was wrong and is recorded as such.** I proposed the §9
single-submitter invariant (the serial executor endorses batch N+1 against N's
uncommitted cache writes, and `orderedOrdererSubmitterCount` clamps submission to
one worker only on the production path while the harness defaults `-orderers` to
64). Run D tested it with `-orderers 1` and collapsed identically, so concurrent
submission is **not** the cause. Don't re-run that experiment.

Ruled out by evidence: read path (QS queueing flat at 2–3 ms through the
collapse), crashes/resources (all 22 containers healthy, 16 GB free), observer CPU
(renderer at 0.2 % when the first abort landed), submitter concurrency, and
monotonic degradation (the failure is episodic — Run B had 8 clean minutes with a
frozen rollback counter between bursts).

Best remaining lead: onset tracks **cumulative committer-tx count** (~110 k), not
elapsed time or tx count. Caveat: two data points, and batch count is confounded
with the throttled small-batch regime, since no full-rate run has exceeded ~10 k
batches. Abort class is `unclassified`.

**Decisive next experiment (~45 min, queued):** 1000 tx/s, everything else
identical. Batches accumulate ~2× faster, so a batch-count threshold predicts
onset at ~t+25 min while a time-based cause predicts ~t+50 min.

## 4b. Run `final` — the shipped re-render (2026-08-05)

The "Transactions in flight" panel was dropped at the owner's request: it sat flat
at exactly 100,000 for the whole run, which is the `-max-outstanding` cap rather
than any property of the system, so it carried no information. The two survivors
in that row widened to 12 columns so the grid has no hole.

```
Replay complete: 10338188/10338188 EVM txs committed in 1817.7s across
10097 committer txs (avg 1023.9 EVM/batch); 0 rolled-back batches,
0 submit failures | 5687 EVM tx/s
Commit-path timing: submit->commit avg 129ms max 439ms | peak in-flight 2/16
```

**Three independent runs of this config now measure 5,729 / 5,718 / 5,687 EVM
tx/s** with zero rollbacks every time — a 0.7% spread. The headline does not rest
on one lucky run. Run A's folder was removed from the delivery set because its
videos still show the withdrawn panel; its numbers survive here and its videos
remain on the host under the `headline` label.

All four videos verified: 1828.0 s container (4 s card + 1818 s body + 6 s close),
body == run to 1×, totals cross-checked to 0.016%.

**Pipeline flaw fixed in the same change.** Every run had shared one TSDB
directory, which `run_demo.sh` wiped at startup — so a later run destroyed an
earlier one's metrics and its videos could never be re-rendered. That defeated the
entire purpose of persisting the TSDB, and it is why this cosmetic panel change
required a fresh 30-minute run instead of a 40-minute re-render. The directory is
now per label (`DEMO_TSDB_DIR`, honoured by the compose overlay and the Makefile
chown, verified on the live container mount), so any past run stays re-renderable
at ~100 MB per 30 minutes.

**Known cosmetic issue, not changed.** In "EVM transaction throughput" the legend
lists Committed and Submitted, but only one line is visible: at steady state they
coincide exactly, so Committed is drawn underneath Submitted. The coincidence *is*
the message (nothing is dropped), but it cannot be seen. The fix is a dash pattern
on Submitted, as already done for the 4-party series on the stack dashboard. Left
alone because it was not requested, and a re-render now costs ~40 min with no new
run.

## 5. Finding — run length is disk-bound, not time-bound

Full detail in [findings.md §10](../findings.md). **~6.5 KB of disk per committed
EVM tx**, from **9 synchronized copies** of the block data (4 batchers + 4
assemblers + sidecar ledger). Postgres stays ~400 MB — state is bounded (~151k
accounts), history is not. On a single 100 GB disk that is a fixed **~12 M
transaction budget per run**, so duration and throughput trade off directly:
~35 min at the 5.7k ceiling, ~3 h at 500 tx/s, 8 h only at ~460 tx/s.

**A multi-hour run at full throughput is not possible on this host.** It needs a
bigger volume, not tuning. Combined with §4 — throttling to reach multi-hour hits
the wedge — **no clean multi-hour run on the real dataset was achievable.** The
shipped deliverable is therefore the full-rate 30-minute pair (client +
full-stack). For a long *clean* video, `-dataset synthetic` (documented
conflict-free) would work, but it is synthetic load and that trade is the owner's
call.

## 6. Bugs found and fixed while building

| Bug | Why it mattered |
|---|---|
| Completion condition compared against `totalToSubmit` (window × wrap count, ~1.5×10¹⁰) | A duration-stopped run would never satisfy its own done condition and would **hang after a successful run** |
| Feeder blocked in `Acquire` past its deadline | `wg.Wait()` waits on `workChan`, which only closes when the feeder returns — the drain and stall-detection path was unreachable, hanging for hours |
| `window()` used an instant query for the run-start gauge | Prometheus writes a stale marker within one scrape interval of the loadgen exiting, so the render — which always runs after — could never find the run |
| Coarse range step located the run's end to ±1 step | 15 s short on a 20-minute run is 1.2%, tripping the render's own 1× assertion |
| Grafana 11.6 preinstalls `grafana-lokiexplore-app`; its 404 threw an uncaught browser exception | **Every** render timed out at `panelsRendered` |
| Preflight checked disk *before* the teardown that frees it | Rejected viable runs for "16G free, need 20G" when the next command returned 88G — killed two queued runs |
| `rm -rf` on the Prometheus TSDB as the host user | Prometheus writes as `nobody`; EPERM under `set -e` killed the run before the stack came up |
| Stat tiles SI-scaled and sparkline-squeezed the headline number | "6K ops/s" instead of "5,849" — the one figure a client reads |
| Four BFT parties overlap exactly in lockstep | Only the last-drawn series was visible; the multiplicity that *is* the point was invisible until each party got a dash pattern |
| Invalid-tx series does not exist when nothing is invalid | Rendered as "No data" rather than zero; pinned with `or vector(0)` |

## 7. Not verified

The spec's regression check — re-running the standard 50k config with
`-max-outstanding` **unset** — was not run. The default path is inert by
construction (`newInflightLimiter(0)` returns nil; every method no-ops on nil;
`*targetTPS > 0` guards the pacer), and flow control *on* measured 5,729 tx/s
against the documented 4.9–6.0k baseline. But the OFF config itself was not
measured. One command when the box is free.
