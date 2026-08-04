# Client Demo Video — Overnight Real-Dataset Run on Grafana

**Status:** design approved 2026-08-04, not yet implemented.

Produce a client-facing video of the Grafana dashboard during a multi-hour
overnight run of the **real** (non-synthetic) USDC dataset on the native `ec2`
experiment host, plus a short highlight cut.

Related: run-book [../experiments.md](../experiments.md) · host setup
[../server-setup.md](../server-setup.md) · current numbers
[../findings.md](../findings.md) §2.

---

## 1. Deliverables

| Artifact | Content |
|---|---|
| `demo-full.mp4` | The whole overnight run at **1× real time**, unedited in pace — the presenter speeds up or scrubs as needed. |
| `demo-highlight.mp4` | ~90 s cut from the same frame pipeline: title card → slow opening → accelerated sweep → closing totals card. |
| `docs/evm-design-impl/demo-video.md` | Operator run-book (pre-flight, smoke, overnight, render, teardown). |
| `$EVM_PERF_DATA/demo/` | Frames, run log, watchdog log, and the run's `Replay complete:` / `Commit-path timing:` lines as evidence. |

Both videos: 1920×1080, H.264, **no audio**, with an ffmpeg-drawn title card and
timed on-screen captions.

"Multi-hour" describes the experiment **and** the full video: an ~8-hour run
produces an ~8-hour real-time recording, with the ~90 s highlight as the short
artifact for anyone who won't sit through it.

### Video parameters

| | `demo-full.mp4` | `demo-highlight.mp4` |
|---|---|---|
| Playback speed | **1× — real time, no speed-up** | ramp ≈24× → ≈600× |
| Video length | equals the run (8 h run → 8 h video) | ~90 s |
| **Content** frame rate | **0.5 fps** (1 frame per 2 s of run) | 24 fps |
| Output frame rate | 10 fps | 30 fps |
| Frames rendered | ~14 400 (8 h) | ~2 160 |
| Time step per frame | uniform 2 s | ramp 1 s → ~25 s |
| Sliding window | 15 min | 15 min |
| Size | measured in the smoke run; expect ~100–400 MB | ~15–30 MB |

**`demo-full.mp4` is real time and unedited in pace** — the presenter speeds up
or scrubs in their own player as needed, and nothing about the timeline is
massaged. Real time here means the time step between frames equals the time each
frame is held on screen: a frame every 2 s of run, displayed for 2 s. It is
encoded at a 10 fps **output** rate with each content frame duplicated 20×; x264
codes duplicates as near-empty P-frames, so this costs almost nothing and avoids
low-fps playback quirks in some players.

The low content rate is what keeps an 8-hour 1080p file reasonable. If the smoke
run's measured MB/hour projects past the acceptable size, the lever is the
content rate — 1 frame per 5 s is still exactly real time, just choppier — or a
higher CRF. **Never** speed the video up to shrink it.

All speed-up lives in the highlight cut, which is where an edited pace is
expected.

---

## 2. Constraints discovered during design

These findings drove the design and must not be re-litigated during
implementation without re-checking the code.

1. **The real dataset drains in ~27 s.** `-dataset historic` is 151 045
   transfers; at the current ~5.6k EVM tx/s headline
   ([findings.md](../findings.md) §2) a single pass is not a long run. Length
   comes from wrap-around replay
   ([replay_json_dataset_test.go:221-231](../../../integration/perf/replay_json_dataset_test.go#L221-L231)),
   which is already sound: balance priming and nonce bypass are per-tx
   ([balance_priming_executor.go:39-45](../../../endorser/testimpl/balance_priming_executor.go#L39-L45)),
   and the trace touches a bounded ~151k accounts, so replaying it for hours
   does not grow world state without bound.
2. **The harness would OOM overnight.** The replay has *no* flow control by
   design — it fires every tx and lets the gateway pending pool absorb the
   backlog
   ([replay_json_dataset_test.go:60-70](../../../integration/perf/replay_json_dataset_test.go#L60-L70),
   [:572-617](../../../integration/perf/replay_json_dataset_test.go#L572-L617)).
   Bounded for a 50k run; over hours the feeder outruns the drain rate by
   orders of magnitude and exhausts the 61 GB host. **Flow control is
   mandatory**, not a nicety.
3. **Prometheus keeps 1 h and no volume.** `--storage.tsdb.retention.time=1h`
   with no data mount
   ([compose.fabric-x.full.yaml:620-637](../../../config/compose/compose.fabric-x.full.yaml#L620-L637)),
   and `make stop-full` runs `down -v`. An overnight run's early hours would be
   discarded.
4. **No run-uptime metric.** The loadgen uses a custom `prometheus.NewRegistry()`
   with no default collectors ([metrics.go:67](../../../integration/perf/metrics.go#L67)),
   so `process_start_time_seconds` does not exist.
5. **Disk caps the run, and it binds hard.** *Measured in the smoke run
   (2026-08-04), not estimated:* a full-stack run consumes **~5.4 KB of disk per
   committed EVM transaction** — **~1.8 GB/min (~108 GB/h) at ~5.6k tx/s**.
   `ec2` has a single 100 GB disk (~81 GB free after `clean-x`), so the budget is
   **~13 million EVM transactions per run**. What is finite is the transaction
   count, not the wall-clock time, so **duration and throughput trade off
   directly**:

   | Rate | Max duration |
   |---|---|
   | ~5,600 tx/s (full) | ~35–40 min |
   | ~1,000 tx/s | ~3.7 h |
   | ~460 tx/s | 8 h |

   The growth is 9 synchronized copies of the block data (4 orderer batchers + 4
   assemblers + the committer sidecar ledger, each ~1.3 GB and climbing in
   lockstep). Postgres stays ~400 MB because the trace touches a bounded ~151k
   accounts — *state* is bounded, *history* is not. That 9× replication is the
   4-party BFT property being demonstrated, so it is not reducible, and there is
   no ledger pruning/retention setting to trade against it.

   **Consequence for this design: a multi-hour run at full throughput is not
   possible on this host.** An 8-hour run would require throttling to ~460 tx/s,
   showing a client ~12× less throughput than the system delivers — which defeats
   the demo's purpose more than a shorter run does. The run is therefore sized to
   **full throughput for as long as the disk allows**, and a genuinely multi-hour
   full-rate run needs a bigger volume (an infrastructure decision, not a
   workaround). See [[evm-ec2-disk-caps-run-length]].
6. **No ffmpeg on the Mac; `ec2` has only variable fonts** (`google-noto-vf`,
   `redhat-vf`), which `ffmpeg drawtext` handles badly. Everything runs in
   containers on `ec2`, with one static TTF shipped in.
7. **Metric resolution bounds the frame rate, not the playback speed.** Frames
   spaced closer together than the scrape interval render identical data, so the
   scrape interval sets the *maximum useful content frame rate* — it does not
   prevent 1× playback. Real time at a low content rate is therefore fine: the
   `loadgen` job drops to `scrape_interval: 1s` (that job only — 1 s on the ~24
   orderer/committer targets would add measurement overhead), which supports up
   to 1 fps of distinct frames; the full video's 0.5 fps sits comfortably inside
   that with 2 samples per frame. Every metric the demo dashboard uses,
   `gateway_*` included, is registered in the loadgen's own registry
   ([metrics.go:166-187](../../../integration/perf/metrics.go#L166-L187)) and
   scraped by that one job, so no other scrape config changes.

---

## 3. Harness changes

`integration/perf/replay_json_dataset_test.go`, `integration/perf/metrics.go`.

### 3.1 `-max-outstanding N` — bounded in-flight, closed loop

Default `0` = off, preserving today's behaviour byte-for-byte so existing
measurements are unaffected. When positive, the feeder blocks until
`submitted − committedEVM − submitFailed < N`, so the system self-paces to
whatever it can actually sustain and runs indefinitely at its true ceiling.

- Extracted as a small `inflightLimiter` type — `Acquire(ctx) error` /
  `Release(n int)` — so the pacing logic is unit-testable without the stack.
- **Rolled-back batches release nothing.** Their EVM txs are re-batched and
  credited when a later batch commits, matching the existing accounting
  ([replay_json_dataset_test.go:660-669](../../../integration/perf/replay_json_dataset_test.go#L660-L669)).
- `Acquire` selects on `ctx.Done()`, so a genuine stall cannot deadlock the
  feeder past the existing stall path.
- Demo value: `100000`.

The dashboard's flat in-flight line is the visible consequence: saturated but
stable.

### 3.2 `PERF_REPLAY_DURATION` — wall-clock stop

E.g. `8h`. The feeder stops feeding at the deadline; the harness then drains
outstanding work and prints final stats as usual. This is what makes "overnight"
reliable — a wrap count cannot be chosen without knowing tx/s in advance, but a
stop time can. Used together with `PERF_REPLAY_WINDOW_SIZE=0` (the whole real
trace) and a large `PERF_REPLAY_WRAP_COUNT`, so duration is what actually ends
the run.

**The completion condition must change with it.** Today the run finishes when
`committedEVM + submitFailed >= totalToSubmit`
([replay_json_dataset_test.go:657](../../../integration/perf/replay_json_dataset_test.go#L657)).
Under a duration stop, `totalToSubmit` (window × wrap count) is deliberately far
larger than what will actually be fed, so that comparison would never fire. The
feeder therefore publishes its **final fed count** when it stops, and the
completion goroutine compares against that once it is known. Without this, an
otherwise-successful overnight run would hang instead of printing its stats.

### 3.3 `loadgen_run_start_timestamp_seconds`

New gauge, set once at `startTime`. Drives the uptime tile as
`time() - loadgen_run_start_timestamp_seconds`.

---

## 4. Monitoring changes

New overlay `config/compose/compose.fabric-x.demo.yaml`, applied by `DEMO=1`,
mirroring the existing `HOST_DATA=1` overlay pattern
([Makefile:25-27](../../../Makefile#L25-L27)). Opt-in, so ordinary measured runs
keep today's throwaway monitoring.

- **Prometheus retention `30d`**, and `/prometheus` **bind-mounted** to
  `$EVM_PERF_DATA/demo/prometheus-data` with `:Z` for SELinux. A bind mount (not
  a named volume) survives `down -v`, so the video stays re-renderable for weeks
  without re-running the experiment. Prometheus runs as `nobody`, so the start
  script `chown 65534:65534` the directory on first create.
- **`scrape_interval: 1s` on the `loadgen` job only** (see constraint 7).
- **`grafana-image-renderer` container** plus `GF_RENDERING_SERVER_URL` /
  `GF_RENDERING_CALLBACK_URL` on Grafana. Started only for demo runs and *used*
  only after the experiment ends, so it never competes for CPU with the
  measurement.
- Provision `evm-demo.json` alongside the existing three dashboards.

---

## 5. Client dashboard — `config/monitoring/grafana/evm-demo.json`

New dashboard, uid `evm-demo`. The engineer-facing `evm-loadgen` dashboard is
left untouched.

Colors follow the `dataviz` skill's reference palette and were **validated with
`scripts/validate_palette.js` against Grafana 11's actual dark panel surface
`#181b1f`** — not chosen by eye. Categorical set `#3987e5,#d95926,#199e70,#9085e9`:
all checks PASS (worst adjacent CVD ΔE 9.4, worst adjacent normal-vision ΔE
24.6, all ≥3:1 on surface). Latency ramp `#184f95,#2a78d6,#9ec5f4` validated
`--ordinal`: monotone lightness, single hue (3° spread), light end 2.13:1.

### Caption strip (text panel, always visible)

> Real Ethereum workload — Jan-2020 USDC transfer trace, 151,045 transactions
> replayed continuously · Fabric-X with 4-party BFT ordering · 32 vCPU / 61 GB

This is what makes the video self-explanatory without narration.

### KPI row — 4 stat tiles

Values render in **primary ink (white), never a series color**; only the failure
tile is colored, and it is paired with a word so color never carries meaning
alone.

| Tile | Query |
|---|---|
| EVM transactions / sec | `loadgen_throughput_tx_per_second` |
| Total transactions committed | `loadgen_transaction_committed_total` |
| Elapsed runtime | `time() - loadgen_run_start_timestamp_seconds` |
| Failures | `gateway_batches_rolled_back_total` — `0` maps to "0 — none" in status-good `#0ca30c`; `>0` in status-critical `#d03b3b` |

### Charts

One measure per axis throughout — **no dual-axis panel anywhere**.

| Panel | Series and color |
|---|---|
| EVM transaction throughput | committed `#3987e5` · submitted `#d95926` (categorical slots 1–2); legend present |
| Commit latency (finality) | p50 `#184f95` · p95 `#2a78d6` · p99 `#9ec5f4` — one hue ordered by magnitude, brightest = p99 because it matters most and sits furthest from the dark surface |
| Committer transactions / sec | aqua `#199e70`, single series → no legend box, the title names it |
| Transactions in flight | violet `#9085e9` — the flat line proving the closed loop is stable |
| Failures over time | rolled-back batches `#d03b3b` · aborted txs `#ec835a` (status tokens, both meaning "bad"); flat at zero |

Underlying metrics all already exist ([metrics.go:20-60](../../../integration/perf/metrics.go#L20-L60))
except the new run-start gauge (§3.3). No per-point value labels; recessive
grid and axes; Grafana dark theme.

---

## 6. Render pipeline — `scripts/demo/`

Runs on **`ec2`** (so `/render` and the frames are local to Grafana), **after**
the experiment finishes, against the persisted TSDB.

1. **Window discovery** — query Prometheus for the run's first and last sample.
2. **Frame math** — for each frame *i*: `to = t₀ + step·i`, `from = to − 15 min`
   (sliding window → the video reads exactly like watching the dashboard live).
   For the full video the step is uniform and equals the frame's on-screen
   duration (`step = 1 / content_fps`), which is what makes playback 1×; the
   render script derives one from the other rather than taking both as
   independent inputs, so the two can't drift out of sync and silently produce a
   sped-up "real time" video. The highlight uses a non-uniform step ramp; see §1.
3. **Frame render** — `GET /render/d/evm-demo?from=<ms>&to=<ms>&width=1920&height=1080&kiosk&theme=dark&tz=UTC`,
   4–6 in parallel via `xargs -P`, to `frames/%06d.png`.
4. **Assembly** — containerized ffmpeg on `ec2` (no host installs). Body video
   from the frame sequence, then title card, timed `drawtext` captions gated
   with `enable='between(t,a,b)'`, and a closing totals card, joined with
   `concat`. Fonts: the ffmpeg image already ships
   `/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf`, so the font travels with
   the tool that draws with it — no host install, no download, no network
   dependency at render time (constraint 6).
5. **Delivery** — rsync only the two mp4s back to the Mac.

### Card and caption text

**Title card** (4 s) — the run's identity, so the file stands alone:

> **EVM on Fabric-X** — sustained throughput on a real Ethereum workload
> Jan-2020 USDC transfer trace · 151,045 transactions replayed continuously
> 4-party BFT ordering · 32 vCPU / 61 GB · &lt;N&gt;-hour continuous run

**Timed captions** over the body, one at a time, ~5 s each: (a) "Real mainnet
transaction trace — not synthetic load"; (b) "Every transaction ordered by a
4-party BFT consensus"; (c) "Throughput holds flat across the whole run"; (d)
"Zero rolled-back batches".

**Closing card** (5 s) — final totals. The render script reads these from the
**last Prometheus sample** of `loadgen_transaction_committed_total`,
`loadgen_batch_committed_total`, `gateway_batches_rolled_back_total` and the
elapsed-runtime expression, and cross-checks them against the run log's
`Replay complete:` line; a mismatch fails the render rather than shipping a
wrong number.

Every substituted value (`<N>`-hour, the totals) comes from the run, never
hand-typed.

Because frames are rendered from stored metrics rather than captured live, both
videos are fully re-renderable — a bad caption or a wrong speed costs a re-render,
not another night.

---

## 7. Run-book — `docs/evm-design-impl/demo-video.md`

- **Pre-flight** — sync local→remote (local is source of truth; never edit on
  the remote), `scripts/setup.sh`, `DEMO=1 make clean-x init-x start-full`,
  `chown` the TSDB dir, confirm every Prometheus target UP — including the
  `loadgen` job, which needs the `docker0 → host:2112` iptables allow on RHEL
  ([prometheus.yml:166-167](../../../config/monitoring/prometheus.yml#L166-L167))
  — and confirm `/render` returns a PNG.
- **Smoke run first (~20 min), non-negotiable** — exercises the entire pipeline
  through to a finished mp4, and measures actual ledger GB/hour so the overnight
  duration is sized against the 89 GB free instead of guessed. Never bet a night
  on an unexercised pipeline.
- **Overnight** — in `tmux`, tee'd to a log:

  ```bash
  PERF_REPLAY_DURATION=8h PERF_REPLAY_WINDOW_SIZE=0 PERF_REPLAY_WRAP_COUNT=100000 \
  GOGC=500 GOMEMLIMIT=48GiB \
  go test -timeout 12h -tags=perf -run '^TestReplayJSONDataset$' -v -count=1 \
    ./integration/perf/... -gateway-config ../config/gateway/fabx-full.yaml \
    -dataset historic -max-batch-size 1024 -max-outstanding 100000 -enable-metrics
  ```

  Serial executor (the default). `-pipeline` is unsolved
  ([findings.md](../findings.md) §6) and must not appear in a client demo.
  `GOGC=500` with a high `GOMEMLIMIT` — never `GOGC=off` for a long-lived run.
- **Watchdog** — 60 s loop logging `df -h /`, container health and
  `docker stats --no-stream` to `$EVM_PERF_DATA/demo/watchdog.log`; graceful
  stop if free space falls below 10 GB, so a disk-full at hour 6 does not
  silently ruin the take.
- **Morning** — capture `Replay complete:` and `Commit-path timing:`, then render
  both videos.
- **Teardown** — only after the mp4s exist. The bind-mounted TSDB survives
  `down -v` regardless.

---

## 8. Testing

| What | How |
|---|---|
| `inflightLimiter` | Unit tests, no stack needed (TDD): in-flight never exceeds N; `Release` unblocks a waiting `Acquire`; `Acquire` returns on ctx cancel; rolled-back batches release nothing |
| Duration stop | Unit test on the deadline helper (parse + expiry), and on the completion condition using the feeder's final fed count rather than `totalToSubmit` (§3.2) — the failure mode is a hung run, so it must be covered |
| Closing-card totals | Unit test that a mismatch between the Prometheus totals and the log's `Replay complete:` line fails the render |
| Dashboard queries | Script asserting every panel expression returns data from the live Prometheus — catches a typo'd metric name *before* the night, not after |
| No regression | Re-run the standard 50k `historic` run with `-max-outstanding` unset; tx/s must match the recorded ~5617 ([findings.md](../findings.md) §2) |
| Render pipeline | The smoke-run mp4 is the test: assert frame count equals the computed N and mp4 duration is within tolerance |
| **Real-time invariant** | Assert the full video's duration equals the run's wall-clock duration within 1% — the one check that catches an accidentally sped-up "real time" video, which is otherwise easy to miss by eye |
| Palette | Already validated — `validate_palette.js` PASS on `#181b1f` for both the categorical set and the ordinal latency ramp |

---

## 9. Risks

| Risk | Mitigation |
|---|---|
| Disk fills mid-run | Smoke run measures GB/hour; watchdog stops gracefully below 10 GB free |
| Frames consume disk on top of the ledger | ~16.5k PNGs (~4 GB) for both videos; rendered *after* the run, and deleted once the mp4s verify. Budgeted against the same 89 GB |
| Render wall-clock | ~16.5k frames at 6-way parallel ≈ 45–60 min, unattended. Real time raises frame count ~6× versus a time-lapse — a cost of the 1× requirement, not a problem |
| Full video too large to share | Lower the content rate (1 frame / 5 s is still exactly real time) or raise CRF; never speed it up |
| Run stalls or dies overnight | `tmux` + tee'd log + watchdog log make morning triage fast; the TSDB persists, so a partial run is still renderable — a 5 h run is still a good demo |
| Render pipeline defect found in the morning | Frames come from stored metrics, so re-rendering is minutes; the smoke run should have caught it already |
| `-max-outstanding` perturbs measurements | Default `0` = off; regression check re-runs the standard measured config |
| Client reads wrap-around as inflated | Caption strip states plainly that the real trace is *replayed continuously*; the run-book records the exact command |

---

## 10. Out of scope

- Voice-over narration (captions only; a script could be added later).
- Any change to the `evm-loadgen` engineer dashboard.
- Fixing the unsolved warm/auth pipeline ([findings.md](../findings.md) §6) —
  the demo runs the serial executor.
- The pre-existing `VersionedCache` RPC `-race` failure — unrelated to this work.
