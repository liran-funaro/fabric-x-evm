#!/usr/bin/env python3
"""Generate a self-contained HTML performance report for the warm/auth pipeline.

Pure stdlib; charts are hand-rendered inline SVG (no CDN, no matplotlib), so the
HTML opens offline in any browser. Data is supplied by RESULTS at the bottom;
both datasets (synthetic, historic) are always rendered SEPARATELY -- never
consolidated.
"""
import html
import math

# ---------------------------------------------------------------------------
# SVG chart primitives
# ---------------------------------------------------------------------------

FONT = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
COL_OFF = "#9aa5b1"   # serial (control)
COL_ON = "#2f6fed"    # pipelined
COL_HIST = "#e8710a"  # historic accent
COL_GRID = "#e6e8eb"
COL_AXIS = "#5b6570"
COL_TEXT = "#2b3138"


def _nice_ceil(v):
    """Round v up to a visually pleasant axis maximum."""
    if v <= 0:
        return 1.0
    exp = math.floor(math.log10(v))
    base = 10 ** exp
    for m in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if m * base >= v:
            return m * base
    return 10 * base


def _fmt(n):
    return f"{n:,.0f}" if abs(n) >= 100 else f"{n:g}"


def line_chart(series, xlabels, title, ylabel, width=560, height=340,
               xtitle="", legend=True):
    """series: list of (name, color, [y-values aligned to xlabels])."""
    ml, mr, mt, mb = 64, 18, 44, 62
    pw, ph = width - ml - mr, height - mt - mb
    ymax = _nice_ceil(max(y for _, _, ys in series for y in ys if y is not None))
    n = len(xlabels)
    xs = [ml + (pw * i / (n - 1) if n > 1 else pw / 2) for i in range(n)]

    def yc(v):
        return mt + ph - (v / ymax) * ph

    parts = [f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
             f'role="img" font-family="{FONT}">']
    parts.append(f'<text x="{width/2}" y="22" text-anchor="middle" '
                 f'font-size="15" font-weight="600" fill="{COL_TEXT}">{html.escape(title)}</text>')
    # y gridlines + ticks
    ticks = 5
    for t in range(ticks + 1):
        v = ymax * t / ticks
        y = yc(v)
        parts.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{ml+pw}" y2="{y:.1f}" '
                     f'stroke="{COL_GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{ml-8}" y="{y+4:.1f}" text-anchor="end" '
                     f'font-size="11" fill="{COL_AXIS}">{_fmt(v)}</text>')
    parts.append(f'<text x="16" y="{mt+ph/2}" text-anchor="middle" font-size="12" '
                 f'fill="{COL_AXIS}" transform="rotate(-90 16 {mt+ph/2})">{html.escape(ylabel)}</text>')
    # x labels
    for i, lab in enumerate(xlabels):
        parts.append(f'<text x="{xs[i]:.1f}" y="{mt+ph+20}" text-anchor="middle" '
                     f'font-size="11" fill="{COL_AXIS}">{html.escape(str(lab))}</text>')
    if xtitle:
        parts.append(f'<text x="{ml+pw/2}" y="{height-8}" text-anchor="middle" '
                     f'font-size="12" fill="{COL_AXIS}">{html.escape(xtitle)}</text>')
    # series
    for name, color, ys in series:
        pts = [(xs[i], yc(y)) for i, y in enumerate(ys) if y is not None]
        if len(pts) > 1:
            d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            parts.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        for x, y in pts:
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.6" fill="{color}"/>')
    # legend
    if legend:
        lx, ly = ml + 6, mt + 6
        for name, color, _ in series:
            parts.append(f'<rect x="{lx}" y="{ly-9}" width="12" height="12" rx="2" fill="{color}"/>')
            parts.append(f'<text x="{lx+18}" y="{ly+1}" font-size="12" fill="{COL_TEXT}">{html.escape(name)}</text>')
            ly += 18
    parts.append('</svg>')
    return "".join(parts)


def grouped_bar(groups, series, title, ylabel, width=520, height=340, value_fmt=_fmt):
    """groups: x-axis category labels. series: list of (name,color,[values per group])."""
    ml, mr, mt, mb = 64, 18, 44, 52
    pw, ph = width - ml - mr, height - mt - mb
    ymax = _nice_ceil(max(v for _, _, vs in series for v in vs))
    ng, ns = len(groups), len(series)
    gw = pw / ng
    bw = gw * 0.62 / ns

    def yc(v):
        return mt + ph - (v / ymax) * ph

    parts = [f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
             f'role="img" font-family="{FONT}">']
    parts.append(f'<text x="{width/2}" y="22" text-anchor="middle" font-size="15" '
                 f'font-weight="600" fill="{COL_TEXT}">{html.escape(title)}</text>')
    ticks = 5
    for t in range(ticks + 1):
        v = ymax * t / ticks
        y = yc(v)
        parts.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{ml+pw}" y2="{y:.1f}" stroke="{COL_GRID}"/>')
        parts.append(f'<text x="{ml-8}" y="{y+4:.1f}" text-anchor="end" font-size="11" '
                     f'fill="{COL_AXIS}">{_fmt(v)}</text>')
    parts.append(f'<text x="16" y="{mt+ph/2}" text-anchor="middle" font-size="12" '
                 f'fill="{COL_AXIS}" transform="rotate(-90 16 {mt+ph/2})">{html.escape(ylabel)}</text>')
    for gi, g in enumerate(groups):
        gx = ml + gw * gi
        for si, (name, color, vs) in enumerate(series):
            v = vs[gi]
            x = gx + gw * 0.19 + si * bw
            y = yc(v)
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{mt+ph-y:.1f}" '
                         f'fill="{color}" rx="2"/>')
            parts.append(f'<text x="{x+bw/2:.1f}" y="{y-5:.1f}" text-anchor="middle" '
                         f'font-size="10.5" font-weight="600" fill="{COL_TEXT}">{value_fmt(v)}</text>')
        parts.append(f'<text x="{gx+gw/2:.1f}" y="{mt+ph+20}" text-anchor="middle" '
                     f'font-size="12" fill="{COL_AXIS}">{html.escape(str(g))}</text>')
    lx, ly = ml + 6, mt + 6
    for name, color, _ in series:
        parts.append(f'<rect x="{lx}" y="{ly-9}" width="12" height="12" rx="2" fill="{color}"/>')
        parts.append(f'<text x="{lx+18}" y="{ly+1}" font-size="12" fill="{COL_TEXT}">{html.escape(name)}</text>')
        ly += 18
    parts.append('</svg>')
    return "".join(parts)


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------

def build_html(R):
    ds_titles = {"synthetic": "Synthetic (conflict-free ceiling)",
                 "historic": "Historic (real Jan-2020 USDC, high MVCC conflict)"}
    css = """
    body{font-family:%s;color:#2b3138;max-width:960px;margin:0 auto;padding:32px 24px 80px;line-height:1.55}
    h1{font-size:26px;margin:0 0 4px} h2{font-size:20px;margin:40px 0 8px;border-bottom:2px solid #eef0f2;padding-bottom:6px}
    h3{font-size:16px;margin:26px 0 6px;color:#3a4149}
    .sub{color:#6b7580;margin:0 0 24px}
    .kpi{display:flex;gap:16px;flex-wrap:wrap;margin:18px 0}
    .card{border:1px solid #e6e8eb;border-radius:10px;padding:14px 18px;min-width:150px;background:#fbfcfd}
    .card .n{font-size:26px;font-weight:700;color:#2f6fed} .card .l{font-size:12px;color:#6b7580}
    .card.ok{border-color:#bfe6cd;background:#f4fbf6} .card.ok .n{color:#1a9a52}
    .card.fail{border-color:#f0c4bd;background:#fdf5f4} .card.fail .n{color:#c0392b}
    table{border-collapse:collapse;width:100%%;margin:10px 0;font-size:13px}
    th,td{border:1px solid #e6e8eb;padding:6px 10px;text-align:right} th{background:#f5f7f9;text-align:center}
    td.l,th.l{text-align:left}
    tr.failrow td{background:#fdf5f4}
    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:start}
    .charts{display:flex;flex-wrap:wrap;gap:16px;margin:8px 0}
    .cap{font-size:12px;color:#6b7580;margin:2px 0 10px}
    .note{background:#f7f9fc;border-left:3px solid #2f6fed;padding:10px 14px;margin:14px 0;font-size:14px;border-radius:0 6px 6px 0}
    .warn{background:#fdf5f4;border-left:3px solid #c0392b;padding:10px 14px;margin:14px 0;font-size:14px;border-radius:0 6px 6px 0}
    .win{background:#f4fbf6;border-left:3px solid #1a9a52;padding:10px 14px;margin:14px 0;font-size:14px;border-radius:0 6px 6px 0}
    .caution{background:#fff8ec;border-left:3px solid #e8710a;padding:10px 14px;margin:14px 0;font-size:14px;border-radius:0 6px 6px 0}
    .good{color:#1a9a52;font-weight:600} .bad{color:#c0392b;font-weight:600}
    code{background:#f0f2f4;padding:1px 5px;border-radius:4px;font-size:12.5px}
    footer{margin-top:56px;color:#96a0aa;font-size:12px;border-top:1px solid #eef0f2;padding-top:12px}
    """ % FONT

    out = [f"<!doctype html><html><head><meta charset='utf-8'>",
           f"<title>Warm/Auth Pipeline — Performance Report</title><style>{css}</style></head><body>"]
    out.append("<h1>Warm/Auth Execution Pipeline — Performance Evaluation</h1>")
    out.append(f"<p class='sub'>{html.escape(R['meta']['subtitle'])}</p>")

    # KPIs (headline, per dataset, at chosen config) -- verdict-aware.
    verdict = R.get("verdict", {})
    out.append("<div class='kpi'>")
    for ds in R["datasets"]:
        h = R["headline"][ds]
        v = verdict.get(ds, {})
        # The KPI is the headline AT THE CHOSEN operating batch size, so it is
        # gated on headline_safe (committed at chosen bs), not the strict
        # all-batch-size `safe`. A dataset can win cleanly here yet still
        # livelock at other batch sizes -- that caveat lives in the banner and
        # the batch-size scan, not this card.
        if v.get("headline_safe", v.get("safe")):
            spd = h["on"] / h["off"] if h["off"] else 0
            out.append(f"<div class='card ok'><div class='n'>{spd:.2f}×</div>"
                       f"<div class='l'><b>{ds}</b>: pipeline speedup at bs={R['meta']['chosen_bs']}<br>"
                       f"({_fmt(h['off'])} → {_fmt(h['on'])} tx/s) · 20000/20000</div></div>")
        else:
            out.append(f"<div class='card fail'><div class='n'>FAILS</div>"
                       f"<div class='l'><b>{ds}</b>: livelock under MVCC conflict<br>"
                       f"only {_fmt(h['on_com'])}/{_fmt(h['on_tot'])} committed at bs={R['meta']['chosen_bs']} "
                       f"({_fmt(h['on'])} vs {_fmt(h['off'])} tx/s)</div></div>")
    out.append("</div>")

    # Verdict banner -- the one-line takeaway, stated before anything else.
    # Prose comes from R["bottom_line"]; the banner is green when every dataset
    # passes the correctness gate, red otherwise.
    syn_safe = verdict.get("synthetic", {}).get("safe")
    hist_safe = verdict.get("historic", {}).get("safe")
    bl = R.get("bottom_line")
    if bl:
        # Three-way: green when every dataset commits at EVERY batch size;
        # amber ("caution") when every dataset commits at the chosen operating
        # point but at least one livelocks at some other batch size (the
        # threshold case); red when a dataset fails even at the chosen point.
        all_safe = all(verdict.get(ds, {}).get("safe") for ds in R["datasets"])
        head_safe = all(verdict.get(ds, {}).get("headline_safe",
                                                 verdict.get(ds, {}).get("safe"))
                        for ds in R["datasets"])
        cls = "win" if all_safe else "caution" if head_safe else "warn"
        out.append(f"<div class='{cls}'>{bl}</div>")
    elif syn_safe and hist_safe is False:
        out.append("<div class='warn'><b>Bottom line:</b> the pipeline is a clean win on the conflict-free "
                   "synthetic workload but <b>fails</b> on the conflict-heavy historic workload. It stays gated "
                   "behind <code>Gateway.Pipelined</code> with a <b>default of off</b>.</div>")

    # Executive summary
    out.append("<h2>Summary</h2>")
    for p in R["summary"]:
        out.append(f"<p>{p}</p>")

    # Methodology
    out.append("<h2>Methodology</h2>")
    out.append("<div class='note'>" + R["method"] + "</div>")

    # Headline A/B bar (both datasets, grouped but each dataset is its own bar-pair -> not consolidated numerically)
    out.append("<h2>Headline: pipeline on vs off</h2>")
    out.append(f"<p>At the chosen operating point (batch size {R['meta']['chosen_bs']}, "
               f"window {R['meta']['window']}, warm-workers = batch size). Datasets are shown side by side but "
               f"measured and reported independently — the bars are never summed or averaged.</p>")
    groups = [ds for ds in R["datasets"]]
    bar = grouped_bar(
        [ds_titles[d].split(" (")[0] for d in groups],
        [("serial (off)", COL_OFF, [R["headline"][d]["off"] for d in groups]),
         ("pipelined (on)", COL_ON, [R["headline"][d]["on"] for d in groups])],
        "Throughput — serial vs pipelined", "EVM tx/s", width=520, height=340)
    out.append(f"<div class='charts'>{bar}</div>")
    # Caption flags any dataset whose pipelined bar (at the chosen bs) failed
    # the correctness gate. Uses headline_safe: the bars are at the chosen bs,
    # so a dataset that commits there but livelocks at other batch sizes is not
    # flagged here (that caveat is in the batch-size scan below).
    failed = [ds for ds in groups
              if not verdict.get(ds, {}).get("headline_safe", verdict.get(ds, {}).get("safe"))]
    if failed:
        parts = "; ".join(
            f"<b>{ds}</b> pipelined = {_fmt(R['headline'][ds]['on'])} tx/s but only "
            f"{_fmt(R['headline'][ds]['on_com'])}/{_fmt(R['headline'][ds]['on_tot'])} committed "
            f"(<span class='bad'>fails the correctness gate</span>)" for ds in failed)
        out.append(f"<p class='cap'>The near-invisible pipelined bar is not a rounding artifact: {parts}. "
                   f"The serial bars all committed 20000/20000.</p>")

    # Per-dataset batch-size scans (SEPARATE sections)
    out.append("<h2>Parameter scan 1 — batch size</h2>")
    out.append("<p>Why this parameter: batch size sets both the merged committer-tx granularity and "
               "(at the code default) the warm-pass worker count. Too small starves the overlap and pays per-batch "
               "overhead; too large collapses the number of batches so the pipeline can't fill. We scan it per dataset "
               "with the pipeline off and on.</p>")
    for ds in R["datasets"]:
        rows = R["batch_scan"][ds]
        bss = [r["bs"] for r in rows]
        offs = [r["off"] for r in rows]
        ons = [r["on"] for r in rows]
        out.append(f"<h3>{ds_titles[ds]}</h3>")
        lc = line_chart(
            [("serial (off)", COL_OFF, offs), ("pipelined (on)", COL_ON, ons)],
            bss, f"Throughput vs batch size — {ds}", "EVM tx/s",
            xtitle="max batch size (EVM txs per merged committer tx)")
        spd = [(r["on"] / r["off"]) if r["off"] else None for r in rows]
        sc = line_chart(
            [("speedup on/off", COL_ON, spd)],
            bss, f"Pipeline speedup vs batch size — {ds}", "speedup (×)",
            xtitle="max batch size", legend=False)
        out.append(f"<div class='charts'>{lc}{sc}</div>")
        # table
        out.append("<table><tr><th class='l'>batch size</th><th>serial tx/s</th><th>pipelined tx/s</th>"
                   "<th>speedup</th><th>committed</th><th>rolled-back</th></tr>")
        for r in rows:
            sp = f"{r['on']/r['off']:.2f}×" if r["off"] else "—"
            passed = r.get("committed", True)
            com, tot = r.get("on_com", R["meta"]["window"]), r.get("on_tot", R["meta"]["window"])
            ok = f"{_fmt(com)}/{_fmt(tot)}" if passed else f"<span class='bad'>{_fmt(com)}/{_fmt(tot)} FAIL</span>"
            rb = r.get("rb", 0)
            rbc = "0" if rb == 0 else f"<span class='bad'>{_fmt(rb)}</span>"
            cls = "" if passed and rb == 0 else " class='failrow'"
            out.append(f"<tr{cls}><td class='l'>{r['bs']}</td><td>{_fmt(r['off'])}</td><td>{_fmt(r['on'])}</td>"
                       f"<td>{sp}</td><td>{ok}</td><td>{rbc}</td></tr>")
        out.append("</table>")
        # Per-dataset threshold note (e.g. historic livelocks below the floor).
        note = R.get("batch_note", {}).get(ds)
        if note:
            out.append(f"<div class='caution'>{note}</div>")

    # Mechanism -- how the pipeline stays correct AND fast under MVCC conflict
    # (the auth read-layering fix). Falls back to the legacy root_cause key.
    mech = R.get("mechanism") or R.get("root_cause")
    if mech:
        out.append("<h2>Mechanism — how auth stays correct and fast under MVCC conflict</h2>")
        for p in mech:
            out.append(f"<p>{p}</p>")

    # WW scan (optional)
    if R.get("ww_scan"):
        out.append("<h2>Parameter scan 2 — warm-pass concurrency</h2>")
        out.append("<p>Why this parameter: the warm pass primes the per-view read cache concurrently before the "
                   "serial authoritative pass. Too few workers under-overlap I/O; too many add EVM-construction and "
                   "GC churn. We confirm the code default (warm workers = batch size) is at/near the throughput knee "
                   "under the pipeline. This scan is run on the <b>synthetic</b> workload only, where the pipeline "
                   "commits cleanly at every batch size; the warm-worker knee is a conflict-free property the auth "
                   "read-layering fix does not move. The historic operating point is governed instead by the "
                   "batch-size correctness floor (see the batch-size scan), not by warm concurrency.</p>")
        for ds in R["datasets"]:
            if ds not in R["ww_scan"]:
                continue
            rows = R["ww_scan"][ds]
            wws = [("batch" if r["ww"] == 0 else r["ww"]) for r in rows]
            tp = [r["tput"] for r in rows]
            out.append(f"<h3>{ds_titles[ds]}</h3>")
            lc = line_chart([("pipelined tx/s", COL_ON, tp)], wws,
                            f"Throughput vs warm workers — {ds}", "EVM tx/s",
                            xtitle="warm-pass workers (0 = batch size, the default)", legend=False)
            out.append(f"<div class='charts'>{lc}</div>")

    # Parameter choices
    out.append("<h2>Chosen parameters</h2>")
    out.append("<table><tr><th class='l'>parameter</th><th class='l'>value</th><th class='l'>justification</th></tr>")
    for name, val, why in R["choices"]:
        out.append(f"<tr><td class='l'><code>{html.escape(name)}</code></td><td class='l'>{html.escape(val)}</td>"
                   f"<td class='l'>{html.escape(why)}</td></tr>")
    out.append("</table>")

    out.append("<h2>Conclusion</h2>")
    for p in R["conclusion"]:
        out.append(f"<p>{p}</p>")

    out.append(f"<footer>{html.escape(R['meta']['footer'])}</footer>")
    out.append("</body></html>")
    return "".join(out)


if __name__ == "__main__":
    import json
    import sys
    with open(sys.argv[1]) as f:
        R = json.load(f)
    out_path = sys.argv[2] if len(sys.argv) > 2 else "pipeline_report.html"
    with open(out_path, "w") as f:
        f.write(build_html(R))
    print(f"wrote {out_path}")
