# Client Demo Video — Implementation Plan

> Executes [specs/2026-08-04-client-demo-video-design.md](../specs/2026-08-04-client-demo-video-design.md).
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce `demo-full.mp4` (1× real-time recording of an overnight run of
the real USDC trace on the client dashboard) and `demo-highlight.mp4` (~90 s cut),
with the run and render fully unattended on `ec2`.

**Architecture:** Four independent slices. (1) The perf harness gains opt-in flow
control, a wall-clock stop, and a run-start gauge so a multi-hour run is possible
at all. (2) A `DEMO=1` compose overlay persists the Prometheus TSDB and adds
`grafana-image-renderer`. (3) A new client-facing Grafana dashboard `evm-demo`.
(4) A render pipeline that replays the stored TSDB frame-by-frame into mp4s, plus
an orchestrator that chains run → render so nothing needs a human overnight.

**Tech Stack:** Go 1.26.5 (harness), docker compose, Prometheus 3.3.1, Grafana
11.6.0 + grafana-image-renderer, Python 3.12 (frame math), containerized ffmpeg.

## Global Constraints

- **Local is the source of truth.** Never edit code on `ec2`; always edit locally
  and rsync local→remote. The remote tree is disposable.
- **No `Co-Authored-By` trailer** in commits. **Never push** `bft-redesign`.
- Commit only files this plan touches; `go.sum` has an unrelated pre-existing
  modification that must stay uncommitted.
- `-tags=perf` is a no-op in this repo — no file is tag-gated. New test files in
  `integration/perf` compile in ordinary `go build ./...`.
- Existing measured configs must be unaffected: every new knob defaults to off.
- The demo runs the **serial** executor. Never `-pipeline` (unsolved, findings §6).
- `GOGC=500` + high `GOMEMLIMIT`; never `GOGC=off` for a long-lived run.
- Full video is **1× real time**. Shrink levers are content frame rate and CRF,
  never playback speed.

---

### Task 1: `inflightLimiter`

Bounds un-completed transactions so a multi-hour run cannot exhaust memory.
Channel-based rather than `sync.Cond` because `Acquire` must honour context
cancellation, which `Cond.Wait` cannot.

**Files:**
- Create: `integration/perf/inflight_limiter.go`
- Test: `integration/perf/inflight_limiter_test.go`

**Interfaces:**
- Produces: `newInflightLimiter(max int) *inflightLimiter`,
  `(*inflightLimiter).Acquire(ctx context.Context) error`,
  `(*inflightLimiter).Release(n int)`. A `nil` receiver is the disabled case and
  every method must tolerate it, so callers need no branch.

- [ ] **Step 1: Write the failing tests**

```go
func TestInflightLimiterDisabledWhenNonPositive(t *testing.T) {
	require.Nil(t, newInflightLimiter(0))
	require.Nil(t, newInflightLimiter(-1))
	var l *inflightLimiter // nil receiver must be safe
	require.NoError(t, l.Acquire(context.Background()))
	l.Release(5)
}

func TestInflightLimiterAdmitsUpToMax(t *testing.T) {
	l := newInflightLimiter(2)
	require.NoError(t, l.Acquire(context.Background()))
	require.NoError(t, l.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, l.Acquire(ctx), "third acquire must block past the bound")
}

func TestInflightLimiterReleaseUnblocksAcquire(t *testing.T) {
	l := newInflightLimiter(1)
	require.NoError(t, l.Acquire(context.Background()))

	acquired := make(chan error, 1)
	go func() { acquired <- l.Acquire(context.Background()) }()

	select {
	case <-acquired:
		t.Fatal("acquire returned while the bound was full")
	case <-time.After(50 * time.Millisecond):
	}

	l.Release(1)
	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("release did not unblock the waiting acquire")
	}
}

func TestInflightLimiterAcquireHonoursContext(t *testing.T) {
	l := newInflightLimiter(1)
	require.NoError(t, l.Acquire(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	require.ErrorIs(t, l.Acquire(ctx), context.Canceled)
}

func TestInflightLimiterOverReleaseDoesNotAddCapacity(t *testing.T) {
	l := newInflightLimiter(1)
	l.Release(10) // nothing held; must not create slots
	require.NoError(t, l.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, l.Acquire(ctx), "over-release must not raise the bound")
}
```

- [ ] **Step 2: Run to verify failure** — `go test ./integration/perf/ -run TestInflightLimiter -v`. Expected: compile failure, `undefined: newInflightLimiter`.

- [ ] **Step 3: Implement**

```go
// inflightLimiter bounds how many submitted-but-not-yet-completed EVM txs the
// feeder may have outstanding, turning the replay's fire-everything feeder into
// a closed loop that self-paces to whatever the stack can sustain. Without it a
// multi-hour run outruns the drain rate and exhausts the host's memory.
//
// A nil *inflightLimiter is the disabled case: every method is a no-op, so the
// caller needs no branch and the default (off) path stays exactly as it was.
type inflightLimiter struct {
	// slots holds one token per in-flight tx. Capacity is the bound; Acquire
	// fills a token and Release drains one. A buffered channel (rather than
	// sync.Cond) is what makes Acquire cancellable via select.
	slots chan struct{}
}

func newInflightLimiter(maxInflight int) *inflightLimiter {
	if maxInflight <= 0 {
		return nil
	}
	return &inflightLimiter{slots: make(chan struct{}, maxInflight)}
}

// Acquire blocks until the in-flight count is below the bound, or ctx ends.
func (l *inflightLimiter) Acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns n slots. Releasing more than are held is ignored rather than
// raising the bound: a rolled-back batch's txs are re-batched and credited
// later, so double-counting them would silently defeat the cap.
func (l *inflightLimiter) Release(n int) {
	if l == nil {
		return
	}
	for range n {
		select {
		case <-l.slots:
		default:
			return
		}
	}
}
```

- [ ] **Step 4: Run to verify pass** — `go test ./integration/perf/ -run TestInflightLimiter -v -race`. Expected: all PASS.

- [ ] **Step 5: Commit** — `perf(harness): add ctx-aware inflightLimiter for long runs`

---

### Task 2: wire flow control, duration stop, and effective total into the replay

**Files:**
- Modify: `integration/perf/replay_json_dataset_test.go`
  - flags block (~line 96)
  - `replayConfig` + `loadReplayConfigFromEnv` (~lines 186-234)
  - feeder (~lines 600-617), completion goroutine (~lines 629-673)
  - progress log (~line 703), submission/stall/final logs (~lines 724-803)
- Test: `integration/perf/replay_effective_total_test.go`

**Interfaces:**
- Consumes: `newInflightLimiter`, `Acquire`, `Release` from Task 1.
- Produces: `effectiveTotal(fedTotal, totalToSubmit int64) int64` — the
  denominator for all reporting: the feeder's final fed count once known
  (`fedTotal >= 0`), else the configured target.

- [ ] **Step 1: Write the failing test for the completion denominator**

The bug this guards: under a duration stop, `totalToSubmit` is window × wrap
count (~1.5×10¹⁰) and is never reached, so the existing
`committed+failed >= totalToSubmit` condition never fires and a *successful*
overnight run hangs instead of printing its stats.

```go
func TestEffectiveTotalUsesFedCountOnceFeederStops(t *testing.T) {
	// Feeder still running: fall back to the configured target.
	require.Equal(t, int64(1_000), effectiveTotal(-1, 1_000))
	// Feeder stopped early (duration stop): the fed count is the real total.
	require.Equal(t, int64(42), effectiveTotal(42, 1_000))
	// Fed nothing at all is still a known total, not "unknown".
	require.Equal(t, int64(0), effectiveTotal(0, 1_000))
}
```

- [ ] **Step 2: Run to verify failure** — `go test ./integration/perf/ -run TestEffectiveTotal -v`. Expected: `undefined: effectiveTotal`.

- [ ] **Step 3: Add the flag, config fields, and helper**

Flag, beside the other flags:

```go
// maxOutstanding bounds submitted-but-not-committed EVM txs. 0 (default) keeps the
// historical fire-everything feeder so measured configs are unchanged; a positive
// value turns the replay into a closed loop that self-paces to the sustainable
// rate, which is what makes a multi-hour demo run possible without exhausting RAM.
var maxOutstanding = flag.Int("max-outstanding", 0, "max submitted-but-uncommitted EVM txs (0 = unbounded, historical behavior)")
```

`replayConfig` gains:

```go
	// duration, when > 0, stops the feeder after this much wall-clock time
	// regardless of how many transfers remain. This is what makes an overnight
	// run reliable: tx/s cannot be predicted well enough to pick a wrap count,
	// but a stop time is exact.
	duration time.Duration
```

In `loadReplayConfigFromEnv`, after the wrap-count block:

```go
	if v := os.Getenv("PERF_REPLAY_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		assert.NoError(t, err, "PERF_REPLAY_DURATION must be a Go duration (e.g. 8h, 20m)")
		assert.True(t, d > 0, "PERF_REPLAY_DURATION must be > 0")
		cfg.duration = d
	}
```

Helper (package-level, next to `runReplayTest`):

```go
// effectiveTotal is the denominator for all progress and final reporting.
// fedTotal is -1 while the feeder is still running and the count of transfers
// actually fed once it stops. Under a duration stop the configured
// totalToSubmit is deliberately unreachable, so reporting and the completion
// condition must both switch to the fed count as soon as it is known.
func effectiveTotal(fedTotal, totalToSubmit int64) int64 {
	if fedTotal >= 0 {
		return fedTotal
	}
	return totalToSubmit
}
```

- [ ] **Step 4: Run to verify pass** — `go test ./integration/perf/ -run TestEffectiveTotal -v`. Expected: PASS.

- [ ] **Step 5: Wire the limiter and duration stop into the feeder**

Before the feeder, alongside the other counters:

```go
	limiter := newInflightLimiter(*maxOutstanding)
	// fedTotal is -1 until the feeder stops, then the number of transfers fed.
	// Read via effectiveTotal; written once by the feeder.
	fedTotal := int64(-1)
	if cfg.duration > 0 {
		t.Logf("Duration-bounded run: feeding for %s (wrap target %d is a ceiling, not a goal)", cfg.duration, totalToSubmit)
	}
	if *maxOutstanding > 0 {
		t.Logf("Closed-loop flow control: max %d outstanding EVM txs", *maxOutstanding)
	}
```

Feeder body becomes:

```go
	feederWg.Go(func() {
		defer close(workChan)
		cursor := 0
		var fed int64
		defer func() {
			atomic.StoreInt64(&fedTotal, fed)
			// The last completion may already have been processed before
			// fedTotal became known, in which case nobody will re-check the
			// condition -- so check it here too, or the run hangs.
			if atomic.LoadInt64(&committedEVM)+atomic.LoadInt64(&submitFailed) >= fed {
				signalDone()
			}
		}()
		deadline := time.Time{}
		if cfg.duration > 0 {
			deadline = startTime.Add(cfg.duration)
		}
		for i := int64(0); i < totalToSubmit; i++ {
			if !deadline.IsZero() && !time.Now().Before(deadline) {
				t.Logf("Duration %s reached; feeder stopping after %d transfers", cfg.duration, fed)
				return
			}
			// Closed loop: wait for headroom before feeding another tx.
			if err := limiter.Acquire(ctx); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case workChan <- workItem{index: i, transfer: window[cursor]}:
				fed++
			}
			cursor++
			if cursor >= len(window) {
				cursor = 0
			}
		}
	})
```

`signalDone` and `doneCh` are declared *after* the feeder today; move their
declaration above the feeder so the closure can reference them.

- [ ] **Step 6: Release slots on completion**

In the submitter, on `SendTransaction` failure the tx will never commit, so its
slot must come back:

```go
					if err := wrappedGateway.SendTransaction(ctx, tx); err != nil {
						t.Logf("Transfer %d: SendTransaction error: %v", item.index, err)
						atomic.AddInt64(&submitFailed, 1)
						limiter.Release(1)
						continue
					}
```

In the completion goroutine's COMMITTED branch, after crediting `n`:

```go
						limiter.Release(n)
```

The rollback branch releases nothing — those EVM txs are still outstanding and
are credited when a later batch commits them.

Replace the completion condition with the effective total:

```go
						if total := effectiveTotal(atomic.LoadInt64(&fedTotal), totalToSubmit); newTotal+atomic.LoadInt64(&submitFailed) >= total {
							signalDone()
						}
```

- [ ] **Step 7: Switch reporting to the effective total**

Four call sites currently print `totalToSubmit`; each becomes
`effectiveTotal(atomic.LoadInt64(&fedTotal), totalToSubmit)`: the progress log
(~703), the submission-complete log and the all-failed shortcut (~724-730), the
stall message (~752), and the `Replay complete:` line plus the returned totals
(~788-803).

- [ ] **Step 8: Verify no regression to the default path**

Run: `go build ./... && go vet ./integration/perf/ && go test ./integration/perf/ -run 'TestInflightLimiter|TestEffectiveTotal' -race -v`
Expected: build clean, all PASS. The default path is unchanged because
`newInflightLimiter(0)` is nil and `cfg.duration` is 0.

- [ ] **Step 9: Commit** — `perf(harness): opt-in flow control + wall-clock stop for multi-hour runs`

---

### Task 3: run-start gauge

**Files:**
- Modify: `integration/perf/metrics.go` (struct ~line 42, constructor ~line 120, register ~line 166, methods near the other setters)
- Modify: `integration/perf/replay_json_dataset_test.go` (after `startTime := time.Now()`)

**Interfaces:**
- Produces: `(*LoadgenMetrics).SetRunStart(t time.Time)` — sets
  `loadgen_run_start_timestamp_seconds`, which the dashboard's uptime tile reads
  as `time() - loadgen_run_start_timestamp_seconds`. Needed because the loadgen
  uses a custom registry with no default collectors, so
  `process_start_time_seconds` does not exist.

- [ ] **Step 1: Add the gauge** — struct field `runStart prometheus.Gauge`; constructor entry:

```go
		runStart: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "loadgen_run_start_timestamp_seconds",
			Help: "Unix timestamp when the replay's measurement window started (uptime = time() - this)",
		}),
```

add `m.runStart` to the `registry.MustRegister(...)` list, and:

```go
// SetRunStart records when the measurement window began, so dashboards can show
// elapsed runtime. The loadgen uses its own registry with no default collectors,
// so process_start_time_seconds is not available.
func (m *LoadgenMetrics) SetRunStart(t time.Time) {
	m.runStart.Set(float64(t.UnixNano()) / float64(time.Second))
}
```

- [ ] **Step 2: Call it** — immediately after `startTime := time.Now()`:

```go
	if metrics != nil {
		metrics.SetRunStart(startTime)
	}
```

- [ ] **Step 3: Verify** — `go build ./... && go vet ./integration/perf/`. Expected: clean.

- [ ] **Step 4: Commit** — `monitoring: add loadgen_run_start_timestamp_seconds for the uptime tile`

---

### Task 4: monitoring — 1 s loadgen scrape, DEMO overlay, Makefile wiring

**Files:**
- Modify: `config/monitoring/prometheus.yml` (loadgen job, ~line 163)
- Create: `config/compose/compose.fabric-x.demo.yaml`
- Modify: `Makefile` (`COMPOSE_FULL_FILES` ~line 25, `start-full` ~line 191)

- [ ] **Step 1: 1 s scrape on the loadgen job only**

Every metric the demo dashboard uses — `gateway_*` included — is registered in
the loadgen's own registry, so this one job covers the whole dashboard. The ~24
orderer/committer targets stay at 5 s, since 1 s there would add overhead during
the measured run.

```yaml
  - job_name: "loadgen"
    # 1s so a 1x real-time video has a distinct frame every 2s (see
    # docs/evm-design-impl/specs/2026-08-04-client-demo-video-design.md).
    # Only this job: it is a single target with ~30 series, and every metric the
    # demo dashboard reads lives here.
    scrape_interval: 1s
    static_configs:
      - targets: ["host.docker.internal:2112"]
```

- [ ] **Step 2: Create the DEMO overlay**

```yaml
# Demo overlay -- applied by `DEMO=1 make start-full`, on top of
# compose.fabric-x.full.yaml. Two jobs:
#   1. Make the run's metrics outlive the run. The base file keeps 1h with no
#      data mount, and `make stop-full` runs `down -v`, so an overnight run's
#      early hours would be gone. A BIND mount (not a named volume) survives
#      `down -v`, keeping the video re-renderable for weeks.
#   2. Add grafana-image-renderer so the render pipeline can turn the stored
#      TSDB into PNG frames. It is only used AFTER the experiment finishes, so
#      it never competes for CPU with the measurement.
services:
  prometheus:
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.retention.time=30d"
      - "--web.enable-remote-write-receiver"
    volumes:
      - ./config/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro,Z
      - ./testdata/crypto:/tls:ro,Z
      - ${EVM_PERF_DATA}/demo/prometheus-data:/prometheus:Z

  grafana:
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
      - GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/evm-demo.json
      - GF_RENDERING_SERVER_URL=http://renderer:8081/render
      - GF_RENDERING_CALLBACK_URL=http://grafana:3000/
      - GF_LOG_FILTERS=rendering:debug
    volumes:
      - ./config/monitoring/grafana/grafana-datasource.yml:/etc/grafana/provisioning/datasources/prometheus.yml:ro,Z
      - ./config/monitoring/grafana/grafana-dashboards.yml:/etc/grafana/provisioning/dashboards/dashboard.yml:ro,Z
      - ./config/monitoring/grafana/committer.json:/var/lib/grafana/dashboards/committer.json:ro,Z
      - ./config/monitoring/grafana/orderer.json:/var/lib/grafana/dashboards/orderer.json:ro,Z
      - ./config/monitoring/grafana/evm-loadgen.json:/var/lib/grafana/dashboards/evm-loadgen.json:ro,Z
      - ./config/monitoring/grafana/evm-demo.json:/var/lib/grafana/dashboards/evm-demo.json:ro,Z

  renderer:
    image: grafana/grafana-image-renderer:3.12.6
    pull_policy: missing
    restart: unless-stopped
    container_name: renderer
    environment:
      - ENABLE_METRICS=false
      - RENDERING_TIMEOUT=60
    networks:
      - fabric-x
```

Compose **replaces** `command` and `environment` wholesale for a service rather
than merging, so both are restated in full; `volumes` merge by container path,
but are restated for clarity.

- [ ] **Step 3: Makefile wiring**

```make
DEMO ?=
ifeq ($(DEMO),1)
COMPOSE_FULL_FILES += -f config/compose/compose.fabric-x.demo.yaml
endif
```

and in `start-full`, before `up -d` — Prometheus runs as `nobody`, so a fresh
bind-mounted TSDB directory must be owned by uid 65534 or it will not start:

```make
	@if [ "$(DEMO)" = "1" ]; then \
		test -n "$$EVM_PERF_DATA" || { echo "Error: DEMO=1 requires EVM_PERF_DATA"; exit 1; }; \
		mkdir -p "$$EVM_PERF_DATA/demo/prometheus-data"; \
		$(DOCKER) run --rm -v "$$EVM_PERF_DATA/demo/prometheus-data":/v busybox chown -R 65534:65534 /v; \
	fi
```

- [ ] **Step 4: Verify** — `DEMO=1 EVM_PERF_DATA=/tmp/evmdemo docker compose --project-directory . -f config/compose/compose.fabric-x.full.yaml -f config/compose/compose.fabric-x.demo.yaml config -q`. Expected: no output (valid).

- [ ] **Step 5: Commit** — `monitoring: DEMO=1 overlay persisting the TSDB + image renderer`

---

### Task 5: client dashboard `evm-demo`

**Files:**
- Create: `config/monitoring/grafana/evm-demo.json`
- Create: `scripts/demo/check_dashboard_queries.sh`

Colors are fixed by the spec and were validated with the dataviz skill's
`validate_palette.js` against Grafana's dark panel surface `#181b1f` — do not
substitute. Categorical: committed `#3987e5`, submitted `#d95926`, committer
`#199e70`, in-flight `#9085e9`. Latency ordinal ramp: p50 `#184f95`, p95
`#2a78d6`, p99 `#9ec5f4`. Status: good `#0ca30c`, critical `#d03b3b`, serious
`#ec835a`.

- [ ] **Step 1: Author the dashboard** — uid `evm-demo`, dark theme, `refresh: 5s`,
  `time: now-15m → now`. Panels, in order: caption text panel; four stat tiles
  (EVM tx/s, total committed, elapsed runtime, failures); then EVM throughput,
  commit latency, committer tx/s, in-flight, failures-over-time. Stat values use
  `colorMode: none` with fixed text color so numbers wear ink, not series color;
  the failures tile alone is colored and carries a `0 → "0 — none"` value
  mapping so color never means anything on its own. Model the JSON on
  `config/monitoring/grafana/evm-loadgen.json` for schema compatibility.

- [ ] **Step 2: Write the query-validation script**

Catches a typo'd metric name before the overnight run rather than after.

```bash
#!/usr/bin/env bash
# Assert every PromQL expression in the demo dashboard returns data.
# Run against a live stack WITH a replay in progress -- an idle stack has no
# loadgen series, so an empty result then is expected, not a failure.
set -euo pipefail
DASHBOARD="${1:-config/monitoring/grafana/evm-demo.json}"
PROM="${PROM:-http://localhost:9090}"

mapfile -t exprs < <(python3 -c '
import json,sys
d=json.load(open(sys.argv[1]))
for p in d["panels"]:
    for t in p.get("targets") or []:
        if t.get("expr"): print(t["expr"])
' "$DASHBOARD")

fail=0
for e in "${exprs[@]}"; do
  n=$(curl -sG "$PROM/api/v1/query" --data-urlencode "query=$e" \
      | python3 -c 'import json,sys; r=json.load(sys.stdin); print(len(r.get("data",{}).get("result",[])) if r.get("status")=="success" else -1)')
  if [ "$n" -lt 0 ]; then echo "QUERY ERROR: $e"; fail=1
  elif [ "$n" -eq 0 ]; then echo "NO DATA:     $e"; fail=1
  else echo "ok ($n series): $e"; fi
done
exit $fail
```

- [ ] **Step 3: Verify the JSON parses and every panel has a target** —
  `python3 -c 'import json;d=json.load(open("config/monitoring/grafana/evm-demo.json"));print(d["uid"],len(d["panels"]))'`. Expected: `evm-demo` and the panel count.

- [ ] **Step 4: Commit** — `monitoring: add client-facing evm-demo dashboard`

---

### Task 6: render pipeline

**Files:**
- Create: `scripts/demo/demo_lib.py` — window discovery, frame schedule, totals
- Create: `scripts/demo/render_video.sh` — frame render + ffmpeg assembly

**Interfaces:**
- Produces (`demo_lib.py` CLI subcommands, all emitting plain text for shell use):
  - `window` → `<start_epoch> <end_epoch>` for the run in the TSDB
  - `frames --mode full|highlight --start S --end E` → one `from_ms to_ms` pair
    per line, in order
  - `totals --start S --end E` → `KEY=VALUE` lines for the closing card
- The **real-time invariant**: for `--mode full` the step between frames is
  derived from the content frame rate (`step = 1/content_fps`), never taken as a
  separate input, so the two cannot drift and silently produce a sped-up video.

- [ ] **Step 1: Write the failing test for the frame schedule**

```python
# scripts/demo/test_demo_lib.py
import demo_lib

def test_full_mode_is_exactly_real_time():
    # 60s of run at 0.5 content fps -> 30 frames, each held 2s => 60s of video.
    frames = demo_lib.frame_schedule("full", start=1000.0, end=1060.0)
    assert len(frames) == 30
    video_seconds = len(frames) / demo_lib.FULL_CONTENT_FPS
    assert abs(video_seconds - 60.0) < 0.01, "full mode must be 1x real time"

def test_full_mode_step_matches_frame_duration():
    frames = demo_lib.frame_schedule("full", start=0.0, end=100.0)
    step = frames[1][1] - frames[0][1]          # to_ms delta
    assert abs(step - 1000.0 / demo_lib.FULL_CONTENT_FPS) < 1e-6

def test_sliding_window_is_fixed_width():
    for fr, to in demo_lib.frame_schedule("full", start=0.0, end=100.0):
        assert abs((to - fr) - demo_lib.WINDOW_SECONDS * 1000) < 1e-6

def test_highlight_covers_whole_run_and_fits_target_length():
    frames = demo_lib.frame_schedule("highlight", start=0.0, end=8 * 3600.0)
    assert abs(frames[-1][1] / 1000.0 - 8 * 3600.0) < 60.0, "must reach the run's end"
    video_seconds = len(frames) / demo_lib.HIGHLIGHT_CONTENT_FPS
    assert 60.0 <= video_seconds <= 120.0, "highlight should land near 90s"

def test_highlight_accelerates():
    frames = demo_lib.frame_schedule("highlight", start=0.0, end=8 * 3600.0)
    first = frames[1][1] - frames[0][1]
    last = frames[-1][1] - frames[-2][1]
    assert last > first * 5, "highlight must ramp from slow to fast"
```

- [ ] **Step 2: Run to verify failure** — `cd scripts/demo && python3 -m pytest test_demo_lib.py -q` (or `python3 -m unittest`). Expected: `ModuleNotFoundError` / failures.

- [ ] **Step 3: Implement `demo_lib.py`** with module constants
  `FULL_CONTENT_FPS = 0.5`, `HIGHLIGHT_CONTENT_FPS = 24`,
  `HIGHLIGHT_TARGET_SECONDS = 90`, `WINDOW_SECONDS = 900`, a `frame_schedule`
  that derives the full-mode step as `1.0 / FULL_CONTENT_FPS` and builds the
  highlight's geometric step ramp so the frame count is
  `HIGHLIGHT_TARGET_SECONDS * HIGHLIGHT_CONTENT_FPS` while the last frame lands
  on `end`, plus `window()` and `totals()` querying Prometheus over HTTP.

- [ ] **Step 4: Run to verify pass** — same command. Expected: all PASS.

- [ ] **Step 5: Write `render_video.sh`** — renders each frame via
  `GET /render/d/evm-demo?from=&to=&width=1920&height=1080&kiosk&theme=dark&tz=UTC`
  with `xargs -P6`, retrying a frame once on failure and aborting if any frame is
  missing or under 5 kB (a blank/error PNG must not reach the video); then
  assembles with containerized ffmpeg (`-framerate <content_fps>`, `-r 10` for
  full / `-r 30` for highlight, `-crf 23`, `-pix_fmt yuv420p`), draws the title
  card, timed captions and closing totals card using
  `$EVM_PERF_DATA/demo/assets/DejaVuSans.ttf`, and concatenates.

- [ ] **Step 6: Assert the real-time invariant on the output**

```bash
# In render_video.sh, after encoding demo-full.mp4:
dur=$(ffprobe_in_container -v error -show_entries format=duration -of csv=p=0 "$OUT/demo-full.mp4")
run=$(python3 -c "print($END - $START)")
python3 - "$dur" "$run" <<'EOF'
import sys
dur, run = float(sys.argv[1]), float(sys.argv[2])
if abs(dur - run) / run > 0.01:
    sys.exit(f"FAIL: full video is {dur:.0f}s for a {run:.0f}s run -- not 1x real time")
print(f"ok: full video {dur:.0f}s == run {run:.0f}s (1x)")
EOF
```

- [ ] **Step 7: Commit** — `scripts(demo): frame schedule + render pipeline for the demo video`

---

### Task 7: unattended orchestrator

Chains preflight → run → render → summary so the deliverables exist by morning
whether or not any interactive session survives.

**Files:**
- Create: `scripts/demo/run_demo.sh`

- [ ] **Step 1: Write the orchestrator** — takes `--duration` (e.g. `8h`) and
  `--outstanding` (default `100000`); then:
  1. Preflight: `EVM_PERF_DATA` set, datasets present, ≥20 GB free, font asset
     present, ports free.
  2. `DEMO=1 make clean-x init-x start-full`, wait for Prometheus targets UP
     including `loadgen` (which needs the `docker0 → host:2112` iptables allow
     on RHEL), and for `/render` to return a PNG.
  3. Start the disk/health watchdog (Step 2) in the background.
  4. Run the replay with `-dataset historic -max-batch-size 1024
     -max-outstanding N -enable-metrics`, `PERF_REPLAY_WINDOW_SIZE=0`,
     `PERF_REPLAY_WRAP_COUNT=100000`, `PERF_REPLAY_DURATION=<duration>`,
     `GOGC=500 GOMEMLIMIT=48GiB`, tee'd to `$EVM_PERF_DATA/demo/replay.log`.
  5. On exit — **including failure** — render both videos from whatever is in the
     TSDB. A crashed 5-hour run still makes a good demo; discarding it would be
     the worse outcome.
  6. Write `$EVM_PERF_DATA/demo/SUMMARY.txt` (the `Replay complete:` and
     `Commit-path timing:` lines, video durations and sizes, watchdog warnings)
     and touch `$EVM_PERF_DATA/demo/DONE`.
  Leave the stack **up** so the dashboard can still be viewed live; teardown is
  the operator's call.

- [ ] **Step 2: Watchdog inside the orchestrator**

```bash
watchdog() {
  while :; do
    avail=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
    printf '%s avail=%sG containers_up=%s\n' "$(date -Is)" "$avail" \
      "$(docker ps -q | wc -l)" >> "$WATCHDOG_LOG"
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' >> "$WATCHDOG_LOG" 2>/dev/null || true
    if [ "$avail" -lt 10 ]; then
      echo "WATCHDOG: only ${avail}G free -- stopping the run early" >> "$WATCHDOG_LOG"
      pkill -INT -f 'TestReplayJSONDataset' || true
      return
    fi
    sleep 60
  done
}
```

- [ ] **Step 3: Shellcheck / dry-run** — `bash -n scripts/demo/*.sh`. Expected: clean.

- [ ] **Step 4: Commit** — `scripts(demo): unattended run+render orchestrator with disk watchdog`

---

### Task 8: run-book

**Files:**
- Create: `docs/evm-design-impl/demo-video.md`
- Modify: `docs/evm-design-impl/README.md` (link it)

- [ ] **Step 1: Write the run-book** — prerequisites, the one-command unattended
  path (`tmux` + `run_demo.sh`), the manual step-by-step equivalent, how to
  re-render without re-running, expected artifacts, and a triage table (loadgen
  target DOWN → iptables; Prometheus won't start → TSDB dir ownership; blank
  frames → renderer URL/timeout; video not 1× → the invariant check).
- [ ] **Step 2: Commit** — `docs(demo): add the demo-video run-book`

---

### Task 9: smoke run on ec2 (~20 min)

Non-negotiable gate: exercises the entire pipeline to a finished mp4 and measures
ledger GB/hour so the overnight duration is sized against the 89 GB free.

- [ ] **Step 1: Sync local→remote** — rsync per server-setup.md (excludes `.git/`,
  `data/`, `*.log`), then ship the font asset once:
  `scp /System/Library/Fonts/Supplemental/Arial.ttf` is not redistributable —
  instead fetch DejaVuSans on the remote into
  `$EVM_PERF_DATA/demo/assets/DejaVuSans.ttf`.
- [ ] **Step 2: Run** `scripts/demo/run_demo.sh --duration 20m` under tmux.
- [ ] **Step 3: Verify** — `DONE` exists; `demo-full.mp4` is ~20 min ±1%;
  `demo-highlight.mp4` ~90 s; both play; `check_dashboard_queries.sh` passed
  during the run; no `NO DATA` panels.
- [ ] **Step 4: Measure** — record GB consumed over the 20 min from the watchdog
  log, extrapolate to GB/hour, and pick the overnight duration leaving ≥15 GB
  headroom against 89 GB.
- [ ] **Step 5: Fix anything the smoke run surfaced, re-sync, and re-verify.**

---

### Task 10: the demo runs

**Revised after the smoke run measured the disk cost.** At ~6.5 KB of disk per
committed EVM tx (9 replicated ledger copies), the budget is ~12M transactions
per run, so duration and throughput trade off directly and a multi-hour run at
full throughput is impossible on this host. Rather than silently pick one, run
both and let the user choose:

- [ ] **Run A — headline (full rate, ~30 min).** The throughput claim.
  `run_demo.sh --duration 30m --label headline`. Unpaced, so the dashboard shows
  the true ceiling (~5.7k tx/s) and ~10M transactions.
- [ ] **Run B — endurance (multi-hour, throttled).** The duration claim.
  `run_demo.sh --duration 6h --target-tps 500 --label endurance`. Deliberately
  paced; `SUMMARY.txt` says so explicitly, so a paced rate can never be read as
  the ceiling.
  - Sizing is deliberately left to the watchdog rather than calculated: disk cost
    has a per-transaction and a per-batch term, and at ~500 tx/s batches are
    ~80 txs instead of 1024, so the per-batch term per transaction rises ~13×.
    One data point cannot separate the two terms, so ask for 6h and let the
    watchdog stop the run if disk runs short — that yields the longest run the
    disk actually supports instead of an estimate of it. A truncated run still
    renders (render-on-failure), so a 4h result is a 4h video, not a lost night.
- [ ] **Verify each** — `SUMMARY.txt`, both mp4s play, the full video's body
  duration equals its run duration within 1%, closing-card totals agree with the
  replay log.
- [ ] **Report** both, with the disk trade-off stated plainly so the choice of
  which to show a client is the user's, not mine.

---

## Self-Review

**Spec coverage:** §3.1 flow control → Tasks 1-2. §3.2 duration stop + completion
denominator → Task 2. §3.3 uptime gauge → Task 3. §4 monitoring overlay,
retention, bind mount, 1 s scrape, renderer → Task 4. §5 dashboard → Task 5. §6
render pipeline, cards and captions, derived totals → Task 6. §7 run-book →
Task 8 (operationalized by Task 7). §8 testing → tests inside Tasks 1, 2, 5, 6
plus the Task 9 gate. §9 risks → watchdog and render-on-failure in Task 7.

**Type consistency:** `newInflightLimiter` / `Acquire` / `Release` and
`effectiveTotal(fedTotal, totalToSubmit)` are used with the same signatures in
Tasks 1-2. `SetRunStart` matches the metric name the Task 5 dashboard queries.
`FULL_CONTENT_FPS` / `HIGHLIGHT_CONTENT_FPS` / `WINDOW_SECONDS` are referenced
identically in the Task 6 tests and implementation.

**Known gap, deliberate:** the exact `evm-demo.json` panel JSON is described
rather than transcribed (Task 5 Step 1); it is ~400 lines of Grafana schema whose
correctness is verified by Step 3's parse check and `check_dashboard_queries.sh`
rather than by review of a literal blob in this plan.
