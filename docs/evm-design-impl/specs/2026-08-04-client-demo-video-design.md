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
| `demo-full.mp4` | The whole overnight run as a time-lapse of the client dashboard. |
| `demo-highlight.mp4` | ~90 s cut from the same frame pipeline: title card → slow opening → accelerated sweep → closing totals card. |
| `docs/evm-design-impl/demo-video.md` | Operator run-book (pre-flight, smoke, overnight, render, teardown). |
| `$EVM_PERF_DATA/demo/` | Frames, run log, watchdog log, and the run's `Replay complete:` / `Commit-path timing:` lines as evidence. |

Both videos: 1920×1080, H.264, **no audio**, with an ffmpeg-drawn title card and
timed on-screen captions.

"Multi-hour" describes the **experiment**, not playback length: the full video
represents ~8 hours in ~8 minutes.

### Video parameters

| | `demo-full.mp4` | `demo-highlight.mp4` |
|---|---|---|
| Video length | 1 s per 1 min of run (8 h run → 8 min) | ~90 s |
| **Content** frame rate | **5 fps** | 24 fps |
| Output frame rate | 30 fps | 30 fps |
| Frames rendered | ~2 400 | ~2 160 |
| Time step per frame | uniform 12 s (≈60× speed) | ramp 1 s (≈24×) → ~25 s (≈600×) |
| Sliding window | 15 min | 15 min |
| Estimated size | ~10–25 MB | ~15–30 MB |

The full video uses a deliberately low **content** frame rate because the
dashboard changes slowly — 5 fps keeps both the file and the render time small.
It is encoded at a 30 fps **output** rate with duplicated frames; x264 codes
those as near-empty P-frames, so the file stays small while avoiding low-fps
playback quirks in some players.

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
5. **Disk is finite.** `ec2` has 89 GB free on `/` (Docker root included), 32
   vCPU, 61 GB RAM. ~157k merged committer txs overnight is feasible but not
   unbounded — the smoke run measures actual GB/hour and sizes the night.
6. **No ffmpeg on the Mac; `ec2` has only variable fonts** (`google-noto-vf`,
   `redhat-vf`), which `ffmpeg drawtext` handles badly. Everything runs in
   containers on `ec2`, with one static TTF shipped in.
7. **Metric resolution bounds the slowest useful playback speed.** A literal 1×
   real-time segment is impossible: consecutive frames closer together than the
   scrape interval render identical data. The `loadgen` job drops to
   `scrape_interval: 1s` (that job only — 1 s on the ~24 orderer/committer
   targets would add measurement overhead), making ~24× the finest useful step.
   The "live" feel comes from the smoothly scrolling window, not from 1× speed,
   and is not labelled as real-time.

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
   (sliding window → the video reads as a live scrolling dashboard). The
   highlight uses a non-uniform step ramp; see the parameter table in §1.
3. **Frame render** — `GET /render/d/evm-demo?from=<ms>&to=<ms>&width=1920&height=1080&kiosk&theme=dark&tz=UTC`,
   4–6 in parallel via `xargs -P`, to `frames/%06d.png`.
4. **Assembly** — containerized ffmpeg on `ec2` (no host installs). Body video
   from the frame sequence, then title card, timed `drawtext` captions gated
   with `enable='between(t,a,b)'`, and a closing totals card, joined with
   `concat`. Fonts: one static `DejaVuSans.ttf` shipped once to
   `$EVM_PERF_DATA/demo/assets/` (constraint 6).
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
| Palette | Already validated — `validate_palette.js` PASS on `#181b1f` for both the categorical set and the ordinal latency ramp |

---

## 9. Risks

| Risk | Mitigation |
|---|---|
| Disk fills mid-run | Smoke run measures GB/hour; watchdog stops gracefully below 10 GB free |
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
