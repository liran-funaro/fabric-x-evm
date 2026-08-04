#!/usr/bin/env python3
"""Frame scheduling and Prometheus queries for the client demo video.

Used by render_video.sh. Three subcommands, all emitting plain text for shell use:

    demo_lib.py window                                  -> "<start_epoch> <end_epoch>"
    demo_lib.py frames --mode full --start S --end E    -> "<from_ms> <to_ms>" per line
    demo_lib.py totals --start S --end E                -> "KEY=VALUE" per line

Design notes that matter:

* The full video is 1x REAL TIME. Its frame step is DERIVED from the content
  frame rate (step = 1 / FULL_CONTENT_FPS) rather than taken as an independent
  input, so the step and the frame's on-screen duration cannot drift apart and
  silently produce a fast-forwarded video that still looks plausible.

* Frames closer together than the Prometheus scrape interval would render
  identical data, so SCRAPE_INTERVAL_SECONDS is the floor on any step. This caps
  the useful frame RATE; it does not constrain playback speed.

* The highlight ramps its step geometrically from that floor, so it opens at the
  finest honest resolution and accelerates to sweep the whole run inside its
  target length. For an 8h run at 90s/24fps the tail reaches ~54s per frame; the
  ramp end is a consequence of covering the run, not a tunable target.
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

# --- Full video: 1x real time -------------------------------------------------
# 0.5 fps = one frame per 2s of run, each held 2s on screen. Low on purpose: the
# dashboard changes slowly, and this is what keeps an 8-hour 1080p file to a
# sane size. To shrink further, LOWER THIS (1 frame / 5s is still exactly 1x) or
# raise the encoder CRF -- never speed the video up.
FULL_CONTENT_FPS = 0.5

# --- Highlight cut ------------------------------------------------------------
HIGHLIGHT_CONTENT_FPS = 24
HIGHLIGHT_TARGET_SECONDS = 90

# Sliding window each frame displays. Fixed width, so the video reads exactly
# like watching the live dashboard.
WINDOW_SECONDS = 900

# The loadgen job's scrape interval (config/monitoring/prometheus.yml). Steps
# finer than this render duplicate frames.
SCRAPE_INTERVAL_SECONDS = 1.0

PROM = os.environ.get("PROM", "http://localhost:9090")


# --------------------------------------------------------------------------- #
# Frame scheduling
# --------------------------------------------------------------------------- #

def _geometric_steps(total, n, min_step):
    """n positive steps in geometric progression starting at min_step, summing to total.

    Returns a uniform schedule instead when total is too small for the ramp
    (a short or crashed run), so a partial run still renders.
    """
    if n <= 1:
        return [total]
    if total <= n * min_step:
        return [total / n] * n

    # Solve sum(min_step * r**i for i in range(n)) == total for r > 1 by bisection.
    # Monotonic in r, so bisection is exact enough and needs no derivatives.
    def total_for(r):
        return min_step * (r ** n - 1.0) / (r - 1.0)

    # The upper bracket has to be derived, not guessed: with n in the thousands a
    # naive hi=2.0 evaluates 2**2160 and overflows. The sum is at least its
    # largest term, min_step * r**(n-1), so any solution satisfies
    # r <= (total/min_step) ** (1/(n-1)) -- which keeps r**n near total/min_step.
    lo = 1.0 + 1e-15
    hi = (total / min_step) ** (1.0 / (n - 1))
    if hi <= lo:
        return [total / n] * n
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if total_for(mid) < total:
            lo = mid
        else:
            hi = mid
    r = (lo + hi) / 2.0

    steps = [min_step * (r ** i) for i in range(n)]
    # Normalize away the bisection residual so the last frame lands exactly on
    # the run's end rather than a few seconds short or long.
    scale = total / sum(steps)
    return [s * scale for s in steps]


def frame_count(mode, duration):
    """Number of frames to render for a run of `duration` seconds."""
    if mode == "full":
        return max(1, int(duration * FULL_CONTENT_FPS))
    if mode == "highlight":
        target = int(round(HIGHLIGHT_TARGET_SECONDS * HIGHLIGHT_CONTENT_FPS))
        # Never ask for more frames than the scrape resolution can distinguish;
        # a 30s run gets a 30-frame highlight, not 2160 duplicates.
        resolvable = max(1, int(duration / SCRAPE_INTERVAL_SECONDS))
        return max(1, min(target, resolvable))
    raise ValueError(f"unknown mode: {mode}")


def video_seconds(mode, duration):
    """Playback length of the finished video for a run of `duration` seconds."""
    fps = FULL_CONTENT_FPS if mode == "full" else HIGHLIGHT_CONTENT_FPS
    return frame_count(mode, duration) / fps


def frame_schedule(mode, start, end):
    """[(from_ms, to_ms)] for each frame, in order.

    `start` and `end` are epoch seconds. Each frame shows a fixed WINDOW_SECONDS
    window ending at that frame's point in the run.
    """
    duration = max(0.0, end - start)
    n = frame_count(mode, duration)

    if mode == "full":
        # DERIVED from the content fps -- this is the real-time invariant.
        step = 1.0 / FULL_CONTENT_FPS
        steps = [step] * n
    else:
        steps = _geometric_steps(duration, n, SCRAPE_INTERVAL_SECONDS)

    frames = []
    t = start
    for s in steps:
        t = min(t + s, end)
        frames.append(((t - WINDOW_SECONDS) * 1000.0, t * 1000.0))
    return frames


# --------------------------------------------------------------------------- #
# Prometheus
# --------------------------------------------------------------------------- #

def _query(expr, at=None):
    params = {"query": expr}
    if at is not None:
        params["time"] = f"{at:.3f}"
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(f"prometheus query failed: {expr}: {body.get('error')}")
    return body["data"]["result"]


def _query_range(expr, start, end, step):
    params = {"query": expr, "start": f"{start:.3f}", "end": f"{end:.3f}",
              "step": f"{step:.3f}"}
    url = f"{PROM}/api/v1/query_range?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=120) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(f"prometheus range query failed: {expr}: {body.get('error')}")
    return body["data"]["result"]


def server_time():
    """Prometheus' own clock. `time()` comes back as resultType "scalar", whose
    result is [timestamp, "value"] rather than the list-of-series an instant
    vector returns -- indexing it like a vector raises TypeError."""
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": "time()"})
    with urllib.request.urlopen(url, timeout=30) as r:
        data = json.load(r)["data"]
    if data.get("resultType") == "scalar":
        return float(data["result"][1])
    return float(data["result"][0]["value"][1])


def _scalar(expr, at=None, default=0.0):
    res = _query(expr, at)
    if not res:
        return default
    return float(res[0]["value"][1])


def window():
    """(start_epoch, end_epoch) of the run held in the TSDB.

    Start comes from loadgen_run_start_timestamp_seconds (its value IS the run
    start). End is the last timestamp at which the loadgen reported a commit
    counter, which is where the data actually stops -- using "now" instead would
    tack dead air onto the end of the video.
    """
    # last_over_time, NOT a bare instant query. The loadgen process is gone by
    # the time we render, and Prometheus writes a stale marker for its series
    # within one scrape interval of the target going down -- after which an
    # instant query at `now` returns nothing at all. A range-vector selector
    # ignores stale markers, so it still finds the run.
    res = _query("last_over_time(loadgen_run_start_timestamp_seconds[30d])")
    if not res:
        raise RuntimeError(
            "loadgen_run_start_timestamp_seconds not found in the last 30d -- was "
            "the replay run with -enable-metrics, and did the loadgen scrape "
            "target come UP? (check $EVM_PERF_DATA/demo*/panel-gate.log)")
    start = float(res[0]["value"][1])

    now = server_time()
    span = max(now - start, 1.0)
    step = max(1.0, span / 10000.0)  # stay under Prometheus' 11k-point cap

    # A range query is unaffected by staleness at `now`: it returns the real
    # samples, so its last point is where the run's data actually stops.
    series = _query_range("loadgen_transaction_committed_total", start, now, step)
    if not series or not series[0]["values"]:
        raise RuntimeError("no loadgen_transaction_committed_total samples in the TSDB")
    end = float(series[0]["values"][-1][0])

    # The coarse pass only locates the end to within one step, and that error
    # propagates straight into the video's length: 15s short on a 20-minute run
    # is 1.2%, which trips the render's own 1x real-time assertion. Refine to
    # 1-second resolution (the loadgen scrape interval) around the coarse answer.
    if step > 1.0:
        lo = max(start, end - 2 * step)
        hi = min(now, end + 2 * step)
        fine = _query_range("loadgen_transaction_committed_total", lo, hi, 1.0)
        if fine and fine[0]["values"]:
            end = float(fine[0]["values"][-1][0])

    if end <= start:
        raise RuntimeError(f"degenerate run window: start={start} end={end}")
    return start, end


def totals(start, end):
    """Final headline numbers for the closing card, read at the run's end."""
    committed = _scalar("loadgen_transaction_committed_total", end)
    batches = _scalar("loadgen_batch_committed_total", end)
    rolled_back = _scalar("gateway_batches_rolled_back_total", end)
    aborted = _scalar("loadgen_transaction_aborted_total", end)
    elapsed = end - start
    return {
        "COMMITTED": int(committed),
        "BATCHES": int(batches),
        "ROLLED_BACK": int(rolled_back),
        "ABORTED": int(aborted),
        "ELAPSED_SECONDS": int(elapsed),
        "TX_PER_SECOND": int(committed / elapsed) if elapsed > 0 else 0,
    }


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("window", help="print '<start_epoch> <end_epoch>' for the run")

    f = sub.add_parser("frames", help="print '<from_ms> <to_ms>' per frame")
    f.add_argument("--mode", required=True, choices=["full", "highlight"])
    f.add_argument("--start", required=True, type=float)
    f.add_argument("--end", required=True, type=float)

    t = sub.add_parser("totals", help="print 'KEY=VALUE' final numbers")
    t.add_argument("--start", required=True, type=float)
    t.add_argument("--end", required=True, type=float)

    p = sub.add_parser("plan", help="print a human summary of what would be rendered")
    p.add_argument("--start", required=True, type=float)
    p.add_argument("--end", required=True, type=float)

    a = ap.parse_args(argv)

    if a.cmd == "window":
        s, e = window()
        print(f"{s:.3f} {e:.3f}")
    elif a.cmd == "frames":
        for fr, to in frame_schedule(a.mode, a.start, a.end):
            print(f"{int(fr)} {int(to)}")
    elif a.cmd == "totals":
        for k, v in totals(a.start, a.end).items():
            print(f"{k}={v}")
    elif a.cmd == "plan":
        dur = a.end - a.start
        print(f"run_seconds={dur:.0f}")
        for mode in ("full", "highlight"):
            print(f"{mode}: frames={frame_count(mode, dur)} "
                  f"video_seconds={video_seconds(mode, dur):.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
