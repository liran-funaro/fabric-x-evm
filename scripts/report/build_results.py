#!/usr/bin/env python3
"""Build report/results.json from the post-fix batch-size sweep log.

Parses the RESULT lines emitted by sweep_batch_pipeline.sh:
    RESULT <ds> <bs> <mode> rc=<rc> tput=<n> com=<c>/<t> rb=<r>
(mode is "serial" or "-pipeline"), computes the batch_scan / headline / verdict,
and merges them with the hand-written post-fix prose embedded below. The prose
is the single source of narrative truth; the numbers come entirely from the
sweep so the headline and the scan table can never disagree.

The parser is idempotent to duplicate RESULT lines (the sweep summary re-greps
its own log), because cells are keyed by (dataset, batch-size, mode).

Usage: python3 build_results.py sweep_batch.log > results.json

HONEST FRAMING (do not soften): the sweep shows the auth read-layering fix
works ABOVE a batch-size floor on conflict-heavy traffic, not unconditionally.
Synthetic (no cross-batch conflict) wins at every batch size. Historic (hot-key
USDC) livelocks at bs <= 256 and is a clean win at bs >= 512 -- the residual
depth-1 limitation. Prose and verdict fields state this plainly.

The ww_scan block is carried over from the prior synthetic runs: a
conflict-free warm-worker knee the fix does not move.
"""
import json
import re
import sys

CHOSEN_BS = 1024
WINDOW = 20000

RESULT_RE = re.compile(
    r"^RESULT\s+(?P<ds>\w+)\s+(?P<bs>\d+)\s+(?P<mode>\S+)\s+rc=(?P<rc>\d+)\s+"
    r"tput=(?P<tput>\d+)\s+com=(?P<com>\d+)/(?P<tot>\d+)\s+rb=(?P<rb>\d+)"
)


def parse_sweep(path):
    """-> {ds: {bs: {'off','on','on_com','on_tot','rb','committed'}}}"""
    data = {}
    with open(path) as f:
        for line in f:
            m = RESULT_RE.match(line.strip())
            if not m:
                continue
            ds, bs, tput = m["ds"], int(m["bs"]), int(m["tput"])
            com, tot, rb = int(m["com"]), int(m["tot"]), int(m["rb"])
            cell = data.setdefault(ds, {}).setdefault(bs, {})
            if m["mode"] == "serial":
                cell["off"] = tput
            else:  # -pipeline
                cell.update(on=tput, on_com=com, on_tot=tot, rb=rb,
                            committed=(com == tot and rb == 0))
    return data


def batch_scan(data, ds):
    rows = []
    for bs in sorted(data.get(ds, {})):
        c = data[ds][bs]
        rows.append({
            "bs": bs,
            "off": c.get("off", 0),
            "on": c.get("on", 0),
            "committed": c.get("committed", False),
            "on_com": c.get("on_com", 0),
            "on_tot": c.get("on_tot", WINDOW),
            "rb": c.get("rb", 0),
        })
    return rows


def verdict_for(rows):
    """Two distinct facts, never conflated:
      safe          -- committed at EVERY batch size (strict dataset verdict)
      headline_safe -- committed at the chosen operating batch size
    plus the correctness floor and the speedup range over COMMITTED cells only
    (a livelock cell's on/off ratio is ~0 and meaningless as a "speedup").
    """
    committed = [r for r in rows if r["committed"]]
    spds = [r["on"] / r["off"] for r in committed if r["off"]]
    chosen_row = next((r for r in rows if r["bs"] == CHOSEN_BS), None)
    return {
        "safe": len(committed) == len(rows) and bool(rows),
        "headline_safe": bool(chosen_row and chosen_row["committed"]),
        "threshold_bs": min((r["bs"] for r in committed), default=None),
        "fail_sizes": [r["bs"] for r in rows if not r["committed"]],
        "spd_min": min(spds) if spds else 0,
        "spd_max": max(spds) if spds else 0,
        "chosen_spd": (chosen_row["on"] / chosen_row["off"]
                       if chosen_row and chosen_row["off"] and chosen_row["committed"] else 0),
    }


def headline_for(rows):
    r = next((r for r in rows if r["bs"] == CHOSEN_BS), rows[-1] if rows else {})
    return {k: r.get(k, d) for k, d in
            (("off", 0), ("on", 0), ("committed", False),
             ("on_com", 0), ("on_tot", WINDOW), ("rb", 0))}


def fmt(x):
    return f"{x:.2f}"


def rng(lo, hi):
    """Format a speedup range, collapsing to one value when the ends round equal."""
    return fmt(lo) if fmt(lo) == fmt(hi) else f"{fmt(lo)}–{fmt(hi)}"


def join_sizes(sizes):
    """[128, 256] -> '128 and 256'; [128,256,512] -> '128, 256, and 512'."""
    s = [str(x) for x in sizes]
    if not s:
        return ""
    if len(s) == 1:
        return s[0]
    if len(s) == 2:
        return f"{s[0]} and {s[1]}"
    return ", ".join(s[:-1]) + f", and {s[-1]}"


def fail_detail(rows):
    """'bs=128: 975/20000 committed, 2682 rolled-back batches, 16 tx/s; ...'"""
    parts = []
    for r in rows:
        if not r["committed"]:
            parts.append(f"bs={r['bs']}: {r['on_com']:,}/{r['on_tot']:,} committed, "
                         f"{r['rb']:,} rolled-back batches, {r['on']:,} tx/s")
    return "; ".join(parts)


def main():
    data = parse_sweep(sys.argv[1])
    datasets = ["synthetic", "historic"]
    scans = {ds: batch_scan(data, ds) for ds in datasets}
    verdicts = {ds: verdict_for(scans[ds]) for ds in datasets}
    headlines = {ds: headline_for(scans[ds]) for ds in datasets}

    hv, sv = verdicts["historic"], verdicts["synthetic"]
    hh, sh = headlines["historic"], headlines["synthetic"]
    h_floor = hv["threshold_bs"]                       # 512
    h_fails = hv["fail_sizes"]                         # [128, 256]
    h_fail_txt = join_sizes(h_fails)                   # "128 and 256"
    h_fail_detail = fail_detail(scans["historic"])

    R = {
        "meta": {
            "subtitle": ("Overlapping the concurrent warm (cache-priming) pass of batch N+1 "
                         "with the serial authoritative (in-order MVCC) pass of batch N. Measured "
                         "on a 32-core EC2 host against a full Fabric-X stack; two workloads "
                         "evaluated independently, batch size 128–4096."),
            "npoints": sum(len(scans[ds]) for ds in datasets) * 2,
            "chosen_bs": CHOSEN_BS,
            "window": WINDOW,
            "footer": ("Generated from EC2 replay sweeps (TestReplayJSONDataset, window=20000, "
                       "GOGC=500, GOMEMLIMIT=48GiB) after the auth read-layering fix. Warm/auth "
                       "pipeline behind the Gateway.Pipelined flag; serial path is the control. "
                       "Each batch-size point is one full stack up → 20000-tx replay → stack "
                       "down. The warm-worker scan is carried over from the prior synthetic runs "
                       "(conflict-free; unaffected by the fix)."),
        },
        "datasets": datasets,
        "headline": headlines,
        "verdict": verdicts,
        "bottom_line": (
            f"<b>Bottom line:</b> the auth read-layering fix makes the warm/auth pipeline correct and a "
            f"throughput <b>win on both workloads at production batch sizes (bs ≥ {h_floor})</b> — synthetic "
            f"{rng(sv['spd_min'], sv['spd_max'])}× and historic {rng(hv['spd_min'], hv['spd_max'])}×, all "
            f"20000/20000 committed with 0 rollbacks. On the conflict-heavy historic USDC trace the pipeline "
            f"<b>still livelocks at small batches (bs {'≤ ' + str(max(h_fails)) if h_fails else 'n/a'})</b> — the "
            f"residual depth-1 limitation, not a regression. It stays behind <code>Gateway.Pipelined</code> with a "
            f"default of <b>off</b>; if enabled, keep bs ≥ {h_floor}."
        ),
        "summary": [
            (f"<b>The fix works at the operating point; a batch-size floor remains on conflict-heavy traffic.</b> "
             f"At the chosen operating point (batch size {CHOSEN_BS}) both workloads commit 20000/20000 with zero "
             f"rollbacks and the pipeline is a throughput win: synthetic <b>{fmt(sv['chosen_spd'])}×</b> "
             f"({sh['off']:,}→{sh['on']:,} tx/s) and historic <b>{fmt(hv['chosen_spd'])}×</b> "
             f"({hh['off']:,}→{hh['on']:,} tx/s). Across the full batch-size scan the pipeline wins on synthetic at "
             f"<i>every</i> size ({rng(sv['spd_min'], sv['spd_max'])}×) and on historic at every size ≥ {h_floor} "
             f"({rng(hv['spd_min'], hv['spd_max'])}×)."),
            (f"<b>The limit.</b> On the historic USDC trace — a handful of hot accounts touched by nearly every "
             f"transaction — the pipeline still livelocks below batch size {h_floor} ({h_fail_detail}). This is the "
             f"documented depth-1 residual: the fix defers a committed batch's writes and replays its warm-write "
             f"snapshot for exactly one boundary, which covers auth(N+1). When batches are small, a hot key written "
             f"by batch N can be skipped by N+1 and read by N+2 — two boundaries later — so auth records a "
             f"read-version that no longer matches committed state and the committer aborts, cascading into the "
             f"livelock. At bs ≥ {h_floor} the historic hot accounts are dense enough that every batch rewrites them, "
             f"keeping each conflicting read inside the depth-1 window. Synthetic has no cross-batch hot-key conflict "
             f"and is clean at every size."),
        ],
        "method": ("Each data point runs the full stack (orderer, committer/query-service, endorser, gateway) on a "
                   "32-core EC2 host and replays a fixed 20000-transaction window of the workload, folding EVM txs "
                   "into merged committer txs of the stated batch size. The <b>serial</b> path (control) runs the "
                   "warm and authoritative passes back-to-back; the <b>pipelined</b> path overlaps warm(N+1) with "
                   "auth(N) behind a barrier, with auth reopening a fresh committed view and replaying the warm "
                   "pass's in-flight writes. Warm-pass concurrency is held at the code default (one worker per tx in "
                   "the batch). The correctness gate for every point is 20000/20000 committed with 0 rolled-back "
                   "batches; a point that misses it is reported as a <b>failure</b> (red row), never tuned away or "
                   "excluded. The two datasets are reported separately and never consolidated into a single number."),
        "batch_scan": scans,
        "batch_note": {
            "historic": (
                f"<b>Historic livelocks at bs {'≤ ' + str(max(h_fails)) if h_fails else 'n/a'}</b> (red rows): the "
                f"depth-1 fix covers auth(N+1) reading batch N's writes, but not a hot key written by batch N, "
                f"skipped by N+1, and read by N+2 — a gap that opens only when batches are too small to rewrite the "
                f"hot accounts every batch. At bs ≥ {h_floor} the fix holds and the pipeline is a clean win "
                f"({rng(hv['spd_min'], hv['spd_max'])}×). The near-zero pipelined points at bs {h_fail_txt} are the "
                f"MVCC-abort livelock, not noise."),
        },
        "ww_scan": {
            "synthetic": [
                {"ww": 32, "tput": 3903, "rb": 0, "committed": True},
                {"ww": 64, "tput": 5377, "rb": 0, "committed": True},
                {"ww": 128, "tput": 6359, "rb": 0, "committed": True},
                {"ww": 256, "tput": 6481, "rb": 0, "committed": True},
                {"ww": 512, "tput": 6468, "rb": 0, "committed": True},
                {"ww": 0, "tput": 6325, "rb": 0, "committed": True},
            ]
        },
        "mechanism": [
            ("The pipelined executor launches warm(N+1)'s concurrent cache-priming pass at the top of the iteration, "
             "before batch N's optimistic writes are applied to the shared cache. So warm(N+1) legitimately reads a "
             "snapshot one batch of writes in the past — for a hot key it sees the value an in-flight predecessor "
             "wrote, pinned at that predecessor's spec version."),
            ("In the naive pipeline the authoritative pass <i>trusted</i> that stale warm read: it recorded warm's "
             "one-batch-old read-versions on hot keys, so when the committer validated batch N+1 against committed "
             "state the versions no longer matched and it aborted. The executor rolled the aborted txs back and "
             "re-warmed against a still-stale snapshot — the rollback livelock that collapsed historic throughput "
             "on every batch size before the fix."),
            ("The fix restores the invariant that auth is authoritative, up to one batch boundary. Before the "
             "authoritative pass, auth(N+1) reopens a <i>fresh</i> view of the latest committed state (sharing "
             "warm's read cache), so any key not in flight is read at its current committed version. Hot keys an "
             "in-flight predecessor wrote are read from the in-flight write-cache (held one boundary past commit) or "
             "a frozen per-batch snapshot of the warm-pass write-set. Either way auth(N+1) reads batch N's writes at "
             "their true (committed) version — never warm's stale one — so recorded read-versions match and no abort "
             "results. For reads that stay within this one-boundary window the pipeline is identical in effect to "
             "serial."),
            (f"<b>The residual and the batch-size floor.</b> The deferral and the snapshot both extend exactly one "
             f"boundary (matching prefetch depth 1), so they cover auth(N+1) but not a hot key written by batch N, "
             f"<i>not</i> rewritten by N+1, and read by N+2 — two boundaries later. By then N's writes are evicted "
             f"and its snapshot retired, so auth(N+2) reads from the query view at a version that can be stale "
             f"relative to the version the committer validates against, and aborts. On historic this gap opens only "
             f"when batches are small: at bs ≤ {max(h_fails) if h_fails else 0} the hot accounts are not touched in "
             f"every batch, the N→N+2 straddle occurs, and the abort cascade returns ({h_fail_detail}). At "
             f"bs ≥ {h_floor} the hot set is rewritten every batch, every conflicting read lands inside the depth-1 "
             f"window, and the pipeline commits cleanly and wins. So the fix raises the correctness floor to "
             f"bs ≥ {h_floor} on conflict-heavy traffic rather than removing the livelock unconditionally. The "
             f"authoritative phase where it does commit is I/O-cheap — measured auth read-time is a few "
             f"milliseconds, matching serial."),
        ],
        "choices": [
            ["Gateway.Pipelined", f"off (default); safe to enable at bs ≥ {h_floor}",
             (f"A correctness-neutral throughput win at bs ≥ {h_floor} on both workloads (synthetic "
              f"{rng(sv['spd_min'], sv['spd_max'])}×, historic {rng(hv['spd_min'], hv['spd_max'])}×, all "
              f"20000/20000, 0 rollbacks). NOT safe below that on conflict-heavy traffic: historic livelocks at bs "
              f"{h_fail_txt} ({h_fail_detail}). Default stays off as the conservative baseline; enable only with "
              f"bs ≥ {h_floor}.")],
            ["max-batch-size", f"{CHOSEN_BS}",
             (f"1024 sits at the throughput knee AND above the pipeline's bs ≥ {h_floor} correctness floor for "
              f"conflict-heavy workloads, so at the default operating point the pipeline commits 20000/20000 on both "
              f"datasets. For the pipeline path the batch size is therefore partly a conflict-avoidance floor, not "
              f"only a throughput knee.")],
            ["warm workers", "= batch size (default)",
             ("Held at the code default for this study so the batch-size scan varies one thing at a time; the "
              "warm-worker scan (synthetic) confirms the default is at/near the knee.")],
            ["prefetch / deferral depth", "1",
             (f"The deferral, the warm-write snapshot, and the prefetch all extend one batch boundary. This is what "
              f"bounds the fix: it is exactly why historic still livelocks at bs ≤ {max(h_fails) if h_fails else 0} "
              f"(the N→N+2 residual). Extending the deferral/snapshot to depth 2+ would lower the historic floor "
              f"below {h_floor}, but adds no throughput (the serial authoritative pass is the floor) and was not "
              f"needed at production batch sizes, so depth stays 1.")],
            ["orderer submitters", "1",
             ("Cross-batch MVCC submission order must be preserved — the pipeline endorses batch k+1 against batch "
              "k's uncommitted writes — so concurrent orderer submission could only reorder, never help.")],
        ],
        "conclusion": [
            (f"At production batch sizes (bs ≥ {h_floor}) the auth read-layering fix eliminates the historic "
             f"livelock and makes the warm/auth pipeline a correctness-neutral throughput win on both workloads "
             f"measured: historic {rng(hv['spd_min'], hv['spd_max'])}× and synthetic {rng(sv['spd_min'], sv['spd_max'])}×, "
             f"and at batch size {CHOSEN_BS} both commit 20000/20000 with 0 rollbacks (historic {fmt(hv['chosen_spd'])}×, "
             f"synthetic {fmt(sv['chosen_spd'])}×). This corrects the earlier characterization that the historic "
             f"pipeline is fundamentally slower than serial — at these batch sizes it is faster, and the fix makes "
             f"auth reopen a fresh committed view and replay the warm pass's in-flight writes at their true version "
             f"instead of trusting warm's stale reads."),
            (f"The limit is real and must not be papered over: on conflict-heavy traffic the depth-1 pipeline still "
             f"livelocks below batch size {h_floor} ({h_fail_txt}), because a hot key written by batch N and read by "
             f"N+2 without an intervening rewrite falls outside the one-boundary coverage. Recommendation: "
             f"<code>Gateway.Pipelined</code> stays <b>off by default</b>; enable it only with bs ≥ {h_floor} (which "
             f"the default max-batch-size = {CHOSEN_BS} already satisfies), and prefer the serial path — the control, "
             f"correct at any batch size — when batch size cannot be guaranteed. Extending the deferral depth would "
             f"lower the floor but yields no throughput gain at production sizes."),
        ],
    }
    print(json.dumps(R, indent=2))


if __name__ == "__main__":
    main()
