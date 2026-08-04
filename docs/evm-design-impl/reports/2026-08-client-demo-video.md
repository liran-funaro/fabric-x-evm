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
| **D — endurance retry** | 500 tx/s, 2.5 h, `-orderers 1` | client + full-stack videos | see §3 |
| **C — full-stack headline** | full rate, 30 min, both dashboards | client + full-stack videos | see §3 |

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

## 3. Runs D and C

*To be completed when the runs report. Run D is the root-cause test of §4 as well
as the multi-hour deliverable: 2.5 h at 500 tx/s with `-orderers 1`. Confirmation
is staying clean well past the 48-minute mark where Run B first aborted.*

## 4. Finding — throttled runs violate the single-submitter invariant

Full detail in [findings.md §11](../findings.md). Summary: the serial executor
returns from `executeCycle` **without waiting for the commit** and executes each
batch against the previous in-flight batch's *uncommitted* cache writes, so
dependent committer txs must reach the orderer **in submission order**.
`orderedOrdererSubmitterCount` enforces that only on the production path;
`BuildGateway` (the perf harness) takes an explicit count and `-orderers` defaults
to **64**. Big batches hide it (endorse ~200 ms ⇒ one batch in the channel at a
time); throttling shrinks batches (~14 txs, ~30 ms endorse) so several queue at
once and 64 workers can reorder them.

**Open decision for the owner:** either `BuildGateway` should clamp like
`buildApp`, or `-orderers` should default to 1. Not changed here — every number in
findings §2 was measured at 64, and at full rate the clamp is a no-op in practice,
but that should be measured before changing a default all recorded results rest on.

## 5. Finding — run length is disk-bound, not time-bound

Full detail in [findings.md §10](../findings.md). **~6.5 KB of disk per committed
EVM tx**, from **9 synchronized copies** of the block data (4 batchers + 4
assemblers + sidecar ledger). Postgres stays ~400 MB — state is bounded (~151k
accounts), history is not. On a single 100 GB disk that is a fixed **~12 M
transaction budget per run**, so duration and throughput trade off directly:
~35 min at the 5.7k ceiling, ~3 h at 500 tx/s, 8 h only at ~460 tx/s.

**A multi-hour run at full throughput is not possible on this host.** It needs a
bigger volume, not tuning. This is why the deliverable is a *pair*: a full-rate
headline and a throttled endurance run.

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
