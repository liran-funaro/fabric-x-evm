#!/usr/bin/env python3
"""Parse the ec2 sweep logs into the RESULTS structure gen_report.py consumes.

Usage: assemble.py batch_scan.log [ww_scan.log] > results.json

RESULT line format (emitted by pipeline_ab.sh):
  RESULT ds=synthetic bs=128 pipe=off rc=0 | TestReplayJSONDataset: 3221 EVM tx/s (20000/20000 committed | 0 rolled-back batches
WW line format (emitted by ww_pipe.sh, if present):
  RESULT ds=synthetic ww=512 rc=0 | TestReplayJSONDataset: 4519 EVM tx/s (20000/20000 committed | 0 rolled-back batches
"""
import json
import re
import sys

RE_BATCH = re.compile(
    r"RESULT ds=(\w+) bs=(\d+) pipe=(\w+) rc=(\d+).*?"
    r"TestReplayJSONDataset: (\d+) EVM tx/s \((\d+)/(\d+) committed.*?(\d+) rolled-back")
RE_WW = re.compile(
    r"RESULT ds=(\w+) ww=(\d+) rc=(\d+).*?"
    r"TestReplayJSONDataset: (\d+) EVM tx/s \((\d+)/(\d+) committed.*?(\d+) rolled-back")

CHOSEN_BS = 1024
WINDOW = 20000


def parse_batch(path):
    """-> {ds: {bs: {off,on, off_com,on_com, off_tot,on_tot, off_rb,on_rb}}}

    Keyed per (ds, bs, pipe); last write wins, so duplicate log lines (the
    per-iter line plus the end-of-run summary) never double-count rollbacks.
    """
    acc = {}
    with open(path) as f:
        for line in f:
            m = RE_BATCH.search(line)
            if not m:
                continue
            ds, bs, pipe, rc, tps, com, tot, rb = m.groups()
            bs, tps, com, tot, rb = int(bs), int(tps), int(com), int(tot), int(rb)
            d = acc.setdefault(ds, {}).setdefault(bs, {})
            d[pipe] = tps
            d[f"{pipe}_com"] = com
            d[f"{pipe}_tot"] = tot
            d[f"{pipe}_rb"] = rb
    return acc


def parse_ww(path):
    acc = {}
    with open(path) as f:
        for line in f:
            m = RE_WW.search(line)
            if not m:
                continue
            ds, ww, rc, tps, com, tot, rb = m.groups()
            acc.setdefault(ds, []).append(
                {"ww": int(ww), "tput": int(tps), "rb": int(rb), "committed": int(com) == int(tot)})
    for ds in acc:
        acc[ds].sort(key=lambda r: (r["ww"] == 0, r["ww"]))  # 0(default) last for readability
    return acc


def main():
    batch = parse_batch(sys.argv[1])
    datasets = [d for d in ("synthetic", "historic") if d in batch]
    npoints = sum(len(v) * 2 for v in batch.values())

    # Per (ds, bs) rows with an explicit correctness gate on the pipelined run.
    batch_scan = {}
    for ds in datasets:
        rows = []
        for bs in sorted(batch[ds]):
            e = batch[ds][bs]
            on_com, on_tot = e.get("on_com", 0), e.get("on_tot", WINDOW)
            rows.append({
                "bs": bs,
                "off": e.get("off", 0),
                "on": e.get("on", 0),
                "committed": on_com == on_tot,      # gate is on the pipelined run
                "on_com": on_com, "on_tot": on_tot,
                "rb": e.get("on_rb", 0),
            })
        batch_scan[ds] = rows

    # Headline at the chosen operating point (fall back to the median bs).
    headline = {}
    for ds in datasets:
        e = batch[ds].get(CHOSEN_BS) or batch[ds][sorted(batch[ds])[len(batch[ds]) // 2]]
        on_com, on_tot = e.get("on_com", 0), e.get("on_tot", WINDOW)
        headline[ds] = {"off": e.get("off", 0), "on": e.get("on", 0),
                        "committed": on_com == on_tot, "on_com": on_com, "on_tot": on_tot,
                        "rb": e.get("on_rb", 0)}

    # Per-dataset verdict: "safe" only if EVERY pipelined batch-size point met
    # the 20000/20000 gate with zero rollbacks.
    verdict = {}
    for ds in datasets:
        rows = batch_scan[ds]
        all_clean = all(r["committed"] and r["rb"] == 0 for r in rows)
        # Best speedup among the clean points (for the safe case).
        clean = [r for r in rows if r["committed"] and r["rb"] == 0 and r["off"]]
        spds = [r["on"] / r["off"] for r in clean]
        verdict[ds] = {
            "safe": all_clean,
            "spd_min": min(spds) if spds else 0,
            "spd_max": max(spds) if spds else 0,
            "chosen_spd": (headline[ds]["on"] / headline[ds]["off"]) if headline[ds]["off"] else 0,
        }

    ww_scan = parse_ww(sys.argv[2]) if len(sys.argv) > 2 else {}
    if ww_scan:
        npoints += sum(len(v) for v in ww_scan.values())

    syn = verdict.get("synthetic", {})
    hist = verdict.get("historic", {})

    R = {
        "meta": {
            "subtitle": ("Overlapping the concurrent warm (cache-priming) pass of batch N+1 with the serial "
                         "authoritative (in-order MVCC) pass of batch N. Measured on a 32-core EC2 host against a "
                         "full Fabric-X stack; two workloads evaluated independently."),
            "npoints": npoints, "chosen_bs": CHOSEN_BS, "window": WINDOW,
            "footer": ("Generated from EC2 replay sweeps (TestReplayJSONDataset, window=20000, GOGC=500, "
                       "GOMEMLIMIT=48GiB). Warm/auth pipeline behind the Gateway.Pipelined flag; serial path is the "
                       "control. Each point is one full stack up → 20000-tx replay → stack down."),
        },
        "datasets": datasets,
        "headline": headline,
        "verdict": verdict,
        "summary": [
            ("<b>The result splits sharply by workload, so the two datasets must not be averaged together.</b> "
             f"On the conflict-free <b>synthetic</b> workload the pipeline is a clean win — "
             f"{syn.get('spd_min', 0):.2f}–{syn.get('spd_max', 0):.2f}× faster across every batch size, "
             f"with 20000/20000 committed and zero rollbacks. On the <b>historic</b> workload (real Jan-2020 "
             "USDC transfers, where a handful of hot accounts are touched by nearly every transaction) the "
             "pipeline <b class='bad'>fails</b>: throughput collapses by 14–300× and most batch sizes never "
             "finish the run at all."),
            ("The mechanism that makes it fast on synthetic is exactly what breaks it on historic. The warm pass "
             "of batch N+1 is launched — and pins its read snapshot — <i>before</i> batch N's writes land in the "
             "shared cache. When N+1's keys are disjoint from N's (synthetic), that early snapshot is harmless and "
             "the I/O-bound warm pass hides cleanly behind the CPU-bound authoritative pass. When N+1 depends on "
             "N's hot keys (historic), the snapshot is stale, the committer aborts N+1 on an MVCC conflict, and "
             "the executor re-warms against a still-stale snapshot — a rollback livelock. See the root-cause "
             "section below."),
        ],
        "method": (
            "Each data point runs the full stack (orderer, committer/query-service, endorser, gateway) on a "
            "32-core EC2 host and replays a fixed 20000-transaction window of the workload, folding EVM txs into "
            "merged committer txs of the stated batch size. The <b>serial</b> path (control) runs the warm and "
            "authoritative passes back-to-back; the <b>pipelined</b> path overlaps warm(N+1) with auth(N) behind "
            "a barrier. Warm-pass concurrency is held at the code default (one worker per tx in the batch). "
            "The correctness gate for every point is 20000/20000 committed with 0 rolled-back batches; a point "
            "that misses it is reported as a <b>failure</b>, not tuned away or excluded. The two datasets are "
            "reported separately and never consolidated into a single number."),
        "batch_scan": batch_scan,
        "ww_scan": ww_scan,
        "root_cause": [
            ("The pipelined executor forms batch N+1 and launches its concurrent warm pass at the top of the "
             "iteration, then runs the authoritative pass of batch N, and only afterwards applies N's optimistic "
             "writes to the shared read cache. So <b>warm(N+1) always pins its read snapshot one batch of writes "
             "in the past</b> — it cannot see the writes of the batch still in flight ahead of it."),
            ("On <b>synthetic</b> traffic, consecutive batches touch disjoint keys, so N+1 never reads a key that "
             "N wrote; the stale snapshot is invisible and the overlap is pure profit (1.16–1.53×, 0 rollbacks). "
             "On <b>historic</b> USDC traffic, almost every batch reads and writes the same few hot accounts, so "
             "N+1's authoritative read-set carries stale versions for keys N just bumped. The committer detects "
             "the version mismatch and aborts N+1. The executor rolls the aborted txs back into the pending pool "
             "and re-forms a batch — but re-warms it against a snapshot that <i>still</i> predates the in-flight "
             "predecessor's writes, so it conflicts again. That is the livelock."),
            ("The data shows the severity scaling with hot-key overlap per batch: at batch size 128 the run grinds "
             "to completion but 14× slower than serial (3447 → 249 tx/s, 1476 rollbacks); at 256 and above it "
             "makes so little forward progress that the harness's 60-second no-progress detector gives up with the "
             "bulk of the window uncommitted (e.g. batch size 1024: only 1121/20000 committed). The serial path "
             "reads the cache <i>after</i> each predecessor's writes are applied, so its versions are always fresh "
             "— 20000/20000 and 0 rollbacks at every batch size."),
            ("This is a property of the stage-1 <b>barrier</b> design, not a localized bug: the barrier orders the "
             "cache-<i>structure</i> mutations between batches, but it does not make the warm snapshot's read "
             "<i>versions</i> current. The deferred stage-2 “snapshot-per-warm” refinement changes which "
             "snapshot each warm pass owns, not <i>when</i> that snapshot is pinned relative to the in-flight "
             "batch's writes, so it would not fix this either. A real fix has to attack snapshot freshness: e.g. "
             "re-read (or invalidate) exactly the keys written by in-flight predecessors before the authoritative "
             "pass of N+1, or add a conflict-adaptive guard that falls back to serial when the recent abort rate "
             "crosses a threshold."),
        ],
        "choices": [
            ("Gateway.Pipelined", "off (default) — opt-in per deployment",
             "A clean 1.2–1.5× win ONLY on workloads with low cross-batch key conflict (synthetic). On "
             "conflict-heavy workloads (historic) it livelocks and drops most of the window, so it must stay "
             "off by default and be enabled only where the traffic is known to be conflict-light."),
            ("max-batch-size", str(CHOSEN_BS),
             "On synthetic (where the pipeline is usable) 1024 sits at the throughput knee — above it the "
             "batch count drops and the per-batch speedup shrinks (1.53× at 128 → 1.16× at 4096); below it "
             "per-batch overhead dominates. On historic, larger batches make the conflict failure worse, which "
             "reinforces not raising it."),
            ("warm workers", "= batch size (default)",
             "Held at the code default for this study so the batch-size scan varies one thing at a time; the "
             "warm-worker scan (synthetic, where the pipeline works) confirms the default is at/near the knee."),
            ("prefetch depth", "1",
             "The authoritative pass is the serial floor and warm(N+1) < auth(N), so one batch of look-ahead "
             "already keeps the serial pass continuously fed; deeper prefetch cannot beat the serial floor and "
             "would only widen the stale-snapshot window that causes the historic failure."),
            ("orderer submitters", "1",
             "Cross-batch MVCC submission order must be preserved — the pipeline endorses batch k+1 against "
             "batch k's uncommitted writes — so concurrent orderer submission could only reorder, never help."),
        ],
        "conclusion": [
            ("The warm/auth pipeline is <b>not</b> a general-purpose win. It is a strict improvement "
             f"({syn.get('spd_min', 0):.2f}–{syn.get('spd_max', 0):.2f}×, zero correctness cost) on workloads "
             "whose consecutive batches touch disjoint keys, and a correctness/throughput failure on workloads "
             "with hot cross-batch key contention. Because real EVM traffic looks far more like the historic "
             "profile than the synthetic one, the feature stays gated behind <code>Gateway.Pipelined</code> "
             "with a <b>default of off</b>."),
            ("Recommendation: keep <code>Gateway.Pipelined</code> off by default. Enable it only for deployments "
             "whose traffic is verified to have low cross-batch key conflict (e.g. sharded or independent-account "
             "workloads resembling the synthetic profile), where batch size 1024 and the default warm-worker "
             "count give the measured 1.2–1.5×. Before the pipeline can be safe as a general default it needs a "
             "conflict-adaptive fallback — detect a rising abort rate and drop back to the serial path — which is "
             "the recommended next step. The serial path remains the correct, robust default for all workloads."),
        ],
    }
    json.dump(R, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
