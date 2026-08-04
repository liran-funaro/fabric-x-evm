# Client Demo Video — Run-Book

How to produce the two client-facing videos of the Grafana dashboard during a
long run of the **real** USDC dataset.

Design: [specs/2026-08-04-client-demo-video-design.md](specs/2026-08-04-client-demo-video-design.md) ·
Plan: [plans/2026-08-04-client-demo-video.md](plans/2026-08-04-client-demo-video.md) ·
General run-book: [experiments.md](experiments.md) · Host: [server-setup.md](server-setup.md)

## Deliverables

| File | What it is |
|---|---|
| `$EVM_PERF_DATA/demo/video/demo-full.mp4` | The whole run at **1× real time**, unedited in pace. An 8 h run is an 8 h video — the presenter speeds up or scrubs in their own player. |
| `$EVM_PERF_DATA/demo/video/demo-highlight.mp4` | ~90 s cut: title card → slow open → accelerating sweep → closing totals. |
| `$EVM_PERF_DATA/demo/SUMMARY.txt` | Config, headline numbers, video sizes, gate/watchdog results. |
| `$EVM_PERF_DATA/demo/replay.log` | Full replay log (the `Replay complete:` line is the headline). |

Both videos are 1920×1080, H.264, no audio, with an ffmpeg-drawn title card,
timed captions and a closing totals card.

## The one-command path

On the experiment host (see [server-setup.md](server-setup.md) — sync local→remote
first; **never edit code on the remote**):

```bash
export EVM_PERF_DATA="$HOME/workspace/evm-perf-data"
bash scripts/setup.sh          # once: fetches the contract + both datasets

tmux new -s demo -d 'export EVM_PERF_DATA=$HOME/workspace/evm-perf-data; \
  cd ~/workspace/fabric-x-evm && \
  bash scripts/demo/run_demo.sh --duration 8h 2>&1 | tee ~/demo-run.log'
```

`run_demo.sh` does everything: preflight → fresh `DEMO=1` stack → replay →
render → `SUMMARY.txt` → `DONE` marker. It needs no attention overnight.

**Always smoke it first.** A 20-minute run exercises the entire pipeline to a
finished mp4 and tells you the ledger's GB/hour, which is what sizes the night:

```bash
bash scripts/demo/run_demo.sh --duration 20m
```

Then size the real run: `(89 GB free − 15 GB headroom) ÷ measured GB/hour`.

Useful flags: `--outstanding N` (in-flight bound, default 100000), `--dataset`,
`--batch-size`, `--no-render`.

## What the run actually does

```
PERF_REPLAY_WINDOW_SIZE=0        # the whole 151,045-transfer real trace
PERF_REPLAY_WRAP_COUNT=100000    # a ceiling, not a goal
PERF_REPLAY_DURATION=8h          # what actually ends the run
GOGC=500 GOMEMLIMIT=48GiB        # never GOGC=off for a long-lived run
-dataset historic -max-batch-size 1024 -max-outstanding 100000 -enable-metrics
```

Serial executor (the default). **Never `-pipeline`** — it is unsolved
([findings.md](findings.md) §6) and must not appear in a client demo.

Three things exist only for this run, all opt-in so measured configs are
unaffected:

- **`-max-outstanding`** bounds submitted-but-uncommitted txs. Mandatory here:
  the replay's normal fire-everything feeder outruns the drain rate without
  bound and would exhaust the host hours in. It also makes the dashboard's
  in-flight line flat, which is the honest "saturated but stable" picture.
- **`PERF_REPLAY_DURATION`** stops the feeder on wall-clock time. A wrap count
  cannot be chosen to land on a target end time; a stop time can.
- **`DEMO=1`** persists the Prometheus TSDB to
  `$EVM_PERF_DATA/demo/prometheus-data` (a bind mount, so `down -v` cannot
  delete it), raises retention to 30 d, and adds `grafana-image-renderer`.

## Two guards that save the night

- **Panel gate, T+120 s.** Asserts the `loadgen` scrape target is UP and every
  dashboard panel returns data, then kills the run if not. Without it, a DOWN
  target or a renamed metric yields a run that completes perfectly and renders
  hours of empty panels. Result lands in `panel-gate.log` and `SUMMARY.txt`.
- **Disk watchdog, every 60 s.** Stops the replay below 10 GB free so a
  disk-full at hour six leaves a usable partial run. Logs `df` and
  `docker stats` to `watchdog.log` for fast morning triage.

Rendering runs **even if the replay fails** — a crashed 5-hour run is still a
good demo.

## Re-rendering without re-running

The TSDB outlives the run, so fix a caption or a length in minutes:

```bash
bash scripts/demo/render_video.sh                 # both
bash scripts/demo/render_video.sh --mode highlight
bash scripts/demo/render_video.sh --keep-frames   # retain PNGs
```

Tunables live at the top of `scripts/demo/demo_lib.py`:
`FULL_CONTENT_FPS` (0.5 = one frame per 2 s of run), `HIGHLIGHT_TARGET_SECONDS`,
`WINDOW_SECONDS` (the 15-minute sliding window).

**To shrink `demo-full.mp4`, lower `FULL_CONTENT_FPS`** — 0.2 (one frame per 5 s)
is still exactly 1× real time, just choppier — **or raise the encoder CRF. Never
speed the video up.** The pipeline asserts the finished body duration equals the
run duration within 1%, because that error is invisible to the eye on a
slow-moving dashboard.

## Triage

| Symptom | Cause and fix |
|---|---|
| `PANEL GATE FAILED … target is DOWN` | RHEL blocks docker0→host. `sudo iptables -I INPUT -i docker0 -p tcp --dport 2112 -j ACCEPT` |
| `PANEL GATE FAILED … no data` for one expression | A metric was renamed. Cross-check `integration/perf/metrics.go` against `config/monitoring/grafana/evm-demo.json` |
| Prometheus won't start under `DEMO=1` | TSDB dir not owned by `nobody`. `docker run --rm -v "$EVM_PERF_DATA/demo/prometheus-data":/v busybox chown -R 65534:65534 /v` |
| `error: Grafana /render failed` | Renderer container missing — the stack was started without `DEMO=1` |
| `FRAME FAILED` / frames under 5 kB | Renderer timeout. Lower `PARALLEL` (default 6) or raise `RENDERING_TIMEOUT` in the demo overlay |
| `FAIL: body is Ns for a Ms run` | The real-time invariant broke. Do **not** ship it; the frame step and content fps have drifted apart in `demo_lib.py` |
| `no commit headroom for 5m0s` | The stack wedged; the feeder stopped so the stall path could report. Read `replay.log` |
| `loadgen_run_start_timestamp_seconds not found` | Replay ran without `-enable-metrics`, or the scrape target never came up |
| Video shows empty panels early on | Expected for the first moments — the 15-minute sliding window starts mostly empty and fills in |

## Teardown

The stack is deliberately left **up** after a run so the dashboard stays live.

```bash
DEMO=1 make stop-full clean-x
```

The bind-mounted TSDB survives this, so the videos remain re-renderable. To
reclaim that space: `rm -rf "$EVM_PERF_DATA/demo/prometheus-data"`.
