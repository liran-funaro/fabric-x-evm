#!/usr/bin/env bash
#
# Render the client demo videos from the metrics already stored in Prometheus.
#
# Nothing is captured live: the run happens unattended, and this replays the
# stored TSDB frame by frame through grafana-image-renderer. That means a bad
# caption or a wrong length costs a re-render, not another overnight run.
#
#   demo-full.mp4       1x REAL TIME, the whole run, low content frame rate
#   demo-highlight.mp4  ~90s cut, ramping from the finest honest step to a sweep
#
# Usage:
#   scripts/demo/render_video.sh                    # both videos, client dashboard
#   scripts/demo/render_video.sh --mode full        # one
#   scripts/demo/render_video.sh --keep-frames      # don't delete PNGs afterwards
#   scripts/demo/render_video.sh --dashboard evm-demo-stack --prefix stack
#                                                   # the committer/orderer view
#
# Env: EVM_PERF_DATA (required), PROM, GRAFANA, FFMPEG_IMAGE, PARALLEL
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${EVM_PERF_DATA:?set EVM_PERF_DATA (e.g. \$HOME/workspace/evm-perf-data)}"
PROM="${PROM:-http://localhost:9090}"
GRAFANA="${GRAFANA:-http://localhost:3000}"
FFMPEG_IMAGE="${FFMPEG_IMAGE:-jrottenberg/ffmpeg:7.1-ubuntu}"
PARALLEL="${PARALLEL:-6}"

# DEMO_LABEL scopes every artifact to one run. Without it a second run would
# silently reuse the first run's frames -- the resume logic skips files that
# already exist, and those frames are of a completely different time window.
DEMO_LABEL="${DEMO_LABEL:-}"
DEMO_DIR="$EVM_PERF_DATA/demo${DEMO_LABEL:+/$DEMO_LABEL}"
OUT_DIR="$DEMO_DIR/video"

# Font path INSIDE the ffmpeg container. The image already ships DejaVu, so the
# font travels with the tool that draws with it: no host install, no download,
# and no network dependency at render time. This matters because the experiment
# host has only variable fonts (google-noto-vf), which drawtext renders badly,
# and no host ffmpeg at all.
FONT="${FONT:-/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf}"

MODES=(full highlight)
KEEP_FRAMES=0
# Which dashboard to film. The client view (evm-demo) is the default; the
# full-stack committer/orderer view (evm-demo-stack) renders from the SAME stored
# run, so covering it costs a re-render rather than another experiment.
DASHBOARD="evm-demo"
PREFIX=""
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODES=("$2"); shift 2 ;;
    --keep-frames) KEEP_FRAMES=1; shift ;;
    --dashboard) DASHBOARD="$2"; shift 2 ;;
    --prefix) PREFIX="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
# Default prefix keeps the original filenames for the client dashboard.
if [ -z "$PREFIX" ]; then
  case "$DASHBOARD" in
    evm-demo) PREFIX=demo ;;
    *) PREFIX="${DASHBOARD#evm-demo-}" ;;
  esac
fi

mkdir -p "$OUT_DIR"
export PROM

# stderr, so a function whose stdout is captured (render_frames returns a count)
# still shows its progress. A 100-minute render with no output looks like a hang.
log() { printf '[render %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# --------------------------------------------------------------------------- #
# ffmpeg / ffprobe run inside a container: ec2 has no ffmpeg, and installing one
# on RHEL means enabling RPM Fusion. The font is mounted in because ec2 ships
# only variable fonts (google-noto-vf), which drawtext renders badly.
# --------------------------------------------------------------------------- #
ff() {
  docker run --rm -u "$(id -u):$(id -g)" \
    -v "$DEMO_DIR":"$DEMO_DIR" -w "$DEMO_DIR" \
    --entrypoint ffmpeg "$FFMPEG_IMAGE" -hide_banner -loglevel error -y "$@"
}
ffprobe_() {
  docker run --rm -u "$(id -u):$(id -g)" \
    -v "$DEMO_DIR":"$DEMO_DIR" -w "$DEMO_DIR" \
    --entrypoint ffprobe "$FFMPEG_IMAGE" -hide_banner -loglevel error "$@"
}

preflight() {
  log "preflight"
  docker image inspect "$FFMPEG_IMAGE" >/dev/null 2>&1 || {
    log "pulling $FFMPEG_IMAGE"
    docker pull -q "$FFMPEG_IMAGE"
  }
  # drawtext is a compile-time option; without it every card and caption fails
  # only at the very end of a long render.
  docker run --rm --entrypoint ffmpeg "$FFMPEG_IMAGE" -hide_banner -filters 2>/dev/null \
    | grep -q ' drawtext ' || {
      echo "error: $FFMPEG_IMAGE has no drawtext filter (needs libfreetype)" >&2; exit 1; }
  docker run --rm --entrypoint sh "$FFMPEG_IMAGE" -c "test -f '$FONT'" || {
    echo "error: no font at $FONT inside $FFMPEG_IMAGE; set FONT= to a path that exists there" >&2
    exit 1; }
  curl -fsS --max-time 10 "$PROM/-/ready" >/dev/null || {
    echo "error: Prometheus not ready at $PROM" >&2; exit 1; }
  curl -fsS --max-time 20 -o /dev/null \
    "$GRAFANA/render/d/$DASHBOARD/x?width=400&height=300&kiosk" || {
      echo "error: Grafana /render failed -- is the renderer container up (DEMO=1)?" >&2; exit 1; }
  log "preflight ok"
}

# --------------------------------------------------------------------------- #
# Frames
# --------------------------------------------------------------------------- #
render_frames() {
  local mode="$1" start="$2" end="$3" dir="$DEMO_DIR/frames/$PREFIX-$mode"
  mkdir -p "$dir"

  python3 "$HERE/demo_lib.py" frames --mode "$mode" --start "$start" --end "$end" \
    > "$dir/schedule.txt"
  local n
  n=$(wc -l < "$dir/schedule.txt" | tr -d ' ')
  log "$PREFIX/$mode: $n frames -> $dir"

  # A 100-minute frame render with no output is indistinguishable from a hang.
  ( while :; do
      sleep 30
      have=$(find "$dir" -name '*.png' 2>/dev/null | wc -l | tr -d ' ')
      log "  $mode: $have/$n frames"
      [ "$have" -ge "$n" ] && break
    done ) & local progress_pid=$!

  # Emit "index from to" so each worker knows its own output name; existing
  # frames are skipped so an interrupted render resumes instead of restarting.
  nl -ba -w1 -s' ' "$dir/schedule.txt" \
    | xargs -P "$PARALLEL" -L1 bash -c '
        idx=$0; from=$1; to=$2
        out=$(printf "%s/%06d.png" "'"$dir"'" "$idx")
        [ -s "$out" ] && exit 0
        url="'"$GRAFANA"'/render/d/'"$DASHBOARD"'/x?orgId=1&from=${from}&to=${to}&width=1920&height=1080&scale=1&kiosk&theme=dark&tz=UTC"
        for attempt in 1 2 3; do
          if curl -fsS --max-time 180 -o "$out" "$url" && [ "$(stat -c%s "$out" 2>/dev/null || stat -f%z "$out")" -gt 5000 ]; then
            exit 0
          fi
          sleep $((attempt * 2))
        done
        echo "FRAME FAILED idx=$idx from=$from to=$to" >&2
        exit 1
      ' || { kill "$progress_pid" 2>/dev/null; echo "error: one or more frames failed to render" >&2; return 1; }
  kill "$progress_pid" 2>/dev/null || true

  # A blank or error PNG must never reach the video, so verify every frame
  # exists and is plausibly an image before spending an hour encoding.
  local missing=0 i
  for i in $(seq 1 "$n"); do
    local f
    f=$(printf "%s/%06d.png" "$dir" "$i")
    if [ ! -s "$f" ] || [ "$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f")" -lt 5000 ]; then
      echo "missing or too small: $f" >&2
      missing=$((missing + 1))
    fi
  done
  [ "$missing" -eq 0 ] || { echo "error: $missing bad frames" >&2; return 1; }
  log "$mode: all $n frames present"
  echo "$n"
}

# --------------------------------------------------------------------------- #
# Cards. Text comes from files rather than inline drawtext arguments so colons,
# commas and em-dashes need no escaping.
# --------------------------------------------------------------------------- #
write_card_text() {
  local dir="$1"; shift
  local i=0
  mkdir -p "$dir"
  for line in "$@"; do
    printf '%s' "$line" > "$dir/line$i.txt"
    i=$((i + 1))
  done
  echo "$i"
}

make_card() {
  # make_card <out.mp4> <fps> <seconds> <text-dir> <count>
  local out="$1" fps="$2" secs="$3" dir="$4" count="$5"
  local filter="" i y size
  for i in $(seq 0 $((count - 1))); do
    case "$i" in
      0) size=64; y=340 ;;
      1) size=40; y=470 ;;
      *) size=34; y=$((540 + (i - 1) * 60)) ;;
    esac
    [ -n "$filter" ] && filter="$filter,"
    filter="${filter}drawtext=fontfile=$FONT:textfile=$dir/line$i.txt:fontcolor=0xffffff:fontsize=$size:x=(w-text_w)/2:y=$y"
  done
  ff -f lavfi -i "color=c=0x0d0d0d:s=1920x1080:d=$secs:r=$fps" \
     -vf "$filter" \
     -c:v libx264 -preset veryfast -crf 20 -pix_fmt yuv420p -r "$fps" "$out"
}

# --------------------------------------------------------------------------- #
# Body encode + captions
# --------------------------------------------------------------------------- #
encode_body() {
  # encode_body <mode> <content_fps> <out_fps> <out.mp4>
  local mode="$1" content_fps="$2" out_fps="$3" out="$4"
  local dir="$DEMO_DIR/frames/$PREFIX-$mode" cdir="$DEMO_DIR/captions"
  mkdir -p "$cdir"

  printf '%s' "Real mainnet transaction trace — not synthetic load" > "$cdir/c0.txt"
  printf '%s' "Every transaction ordered by 4-party BFT consensus"  > "$cdir/c1.txt"
  printf '%s' "Throughput holds flat across the whole run"          > "$cdir/c2.txt"
  printf '%s' "Zero rolled-back batches"                            > "$cdir/c3.txt"

  # Captions land in the opening minutes and are gated on presentation time, so
  # they read the same whether the video is 90s or 8h.
  local f=""
  local starts=(6 20 34 48) ends=(17 31 45 59)
  local i
  for i in 0 1 2 3; do
    [ -n "$f" ] && f="$f,"
    f="${f}drawtext=fontfile=$FONT:textfile=$cdir/c$i.txt:fontcolor=0xffffff:fontsize=40"
    f="${f}:box=1:boxcolor=0x0d0d0d@0.72:boxborderw=18:x=(w-text_w)/2:y=h-140"
    f="${f}:enable='between(t,${starts[$i]},${ends[$i]})'"
  done

  log "$mode: encoding body (content ${content_fps}fps -> output ${out_fps}fps)"
  # Duplicated output frames are near-empty P-frames, so a 10fps container over
  # 0.5fps of content costs almost nothing but avoids low-fps playback quirks.
  ff -framerate "$content_fps" -i "$dir/%06d.png" \
     -vf "$f" \
     -c:v libx264 -preset veryfast -tune stillimage -crf 23 \
     -pix_fmt yuv420p -g 250 -r "$out_fps" -movflags +faststart "$out"
}

concat_parts() {
  # concat_parts <out.mp4> <part...>
  local out="$1"; shift
  local list="$DEMO_DIR/concat_$$.txt"
  : > "$list"
  local p
  for p in "$@"; do printf "file '%s'\n" "$p" >> "$list"; done
  # -c copy keeps the long body untouched; every part is encoded with identical
  # codec settings above so the streams are concatenable as-is.
  if ! ff -f concat -safe 0 -i "$list" -c copy -movflags +faststart "$out" 2>/dev/null; then
    log "stream copy concat failed; re-encoding"
    ff -f concat -safe 0 -i "$list" -c:v libx264 -preset veryfast -crf 23 \
       -pix_fmt yuv420p -movflags +faststart "$out"
  fi
  rm -f "$list"
}

duration_of() {
  ffprobe_ -v error -show_entries format=duration -of csv=p=0 "$1" | tr -d '\r'
}

# Thousands separators for the closing card. bash's printf "%'d" depends on the
# locale and the host runs C.UTF-8, which groups nothing -- "12345678" instead of
# "12,345,678". Python groups regardless of locale.
group() { python3 -c "print(f'{int(\"$1\"):,}')"; }

# Card wording. "0.5-hour continuous run" reads badly, so use minutes below 90
# and hours above. Two forms: adjectival ("30-minute run") and nominal
# ("over 30 minutes").
human_dur() {
  python3 - "$1" "$2" <<'PY'
import sys
s, form = float(sys.argv[1]), sys.argv[2]
if s < 5400:
    n, unit = f"{s/60:.0f}", "minute"
else:
    n, unit = f"{s/3600:.1f}", "hour"
print(f"{n}-{unit}" if form == "adj" else f"{n} {unit}s")
PY
}

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
preflight

read -r START END < <(python3 "$HERE/demo_lib.py" window)
RUN_SECONDS=$(python3 -c "print(f'{$END - $START:.0f}')")
log "run window: $START -> $END (${RUN_SECONDS}s)"
python3 "$HERE/demo_lib.py" plan --start "$START" --end "$END" | sed 's/^/[render] /'

# Totals for the closing card come from the TSDB, never hand-typed.
declare -A T
while IFS='=' read -r k v; do T["$k"]="$v"; done < <(
  python3 "$HERE/demo_lib.py" totals --start "$START" --end "$END")
log "totals: ${T[COMMITTED]} committed, ${T[BATCHES]} batches, ${T[ROLLED_BACK]} rolled back, ${T[TX_PER_SECOND]} tx/s avg"

# Cross-check the closing card's numbers against the replay log, so a video can
# never ship a headline that disagrees with the run's own output -- e.g. if the
# wrong run's window were discovered in the TSDB. A small gap is expected and
# fine: the final commits land after the last scrape, so the counter trails the
# log by a fraction of a percent. Only a gross mismatch is a bug.
REPLAY_LOG_PATH="$DEMO_DIR/replay.log"
if [ -f "$REPLAY_LOG_PATH" ]; then
  logged=$(grep -oE 'Replay complete: [0-9]+' "$REPLAY_LOG_PATH" | tail -1 | grep -oE '[0-9]+$' || true)
  if [ -n "$logged" ]; then
    python3 - "${T[COMMITTED]}" "$logged" <<'PY' || exit 1
import sys
prom, logged = int(sys.argv[1]), int(sys.argv[2])
if logged == 0:
    sys.exit("FAIL: replay log reports 0 committed transactions")
drift = abs(prom - logged) / logged
if drift > 0.01:
    sys.exit(f"FAIL: Prometheus says {prom:,} committed but the replay log says "
             f"{logged:,} ({drift*100:.2f}% apart) -- refusing to ship a video "
             f"whose headline disagrees with the run")
print(f"ok: totals agree with the replay log ({prom:,} vs {logged:,}, "
      f"{drift*100:.3f}% apart -- commits after the last scrape)")
PY
  else
    log "WARNING: no 'Replay complete:' line in $REPLAY_LOG_PATH; skipping the cross-check"
  fi
else
  log "WARNING: no replay log at $REPLAY_LOG_PATH; skipping the totals cross-check"
fi

DUR_ADJ=$(human_dur "$RUN_SECONDS" adj)
DUR_NOUN=$(human_dur "$RUN_SECONDS" noun)

for mode in "${MODES[@]}"; do
  case "$mode" in
    full)      content_fps=0.5; out_fps=10 ;;
    highlight) content_fps=24;  out_fps=30 ;;
    *) echo "unknown mode: $mode" >&2; exit 2 ;;
  esac

  render_frames "$mode" "$START" "$END" >/dev/null   # progress goes to stderr

  body="$DEMO_DIR/body-$PREFIX-$mode.mp4"
  encode_body "$mode" "$content_fps" "$out_fps" "$body"

  # Subtitle follows the dashboard being filmed, so the stack video is not
  # mislabelled with the client view's framing.
  case "$PREFIX" in
    stack) subtitle="Full stack under load — BFT ordering and the committer pipeline" ;;
    *)     subtitle="Sustained throughput on a real Ethereum workload" ;;
  esac
  tdir="$DEMO_DIR/cards/title-$PREFIX-$mode"
  n=$(write_card_text "$tdir" \
    "EVM on Fabric-X" \
    "$subtitle" \
    "Jan-2020 USDC transfer trace — 151,045 transactions replayed continuously" \
    "4-party BFT ordering — 32 vCPU / 61 GB" \
    "${DUR_ADJ} continuous run$([ "$mode" = highlight ] && echo ' — highlights' || echo ' — real time, unedited')")
  make_card "$DEMO_DIR/title-$PREFIX-$mode.mp4" "$out_fps" 4 "$tdir" "$n"

  cdir="$DEMO_DIR/cards/close-$PREFIX-$mode"
  n=$(write_card_text "$cdir" \
    "$(group "${T[COMMITTED]}") transactions" \
    "committed over ${DUR_NOUN}" \
    "$(group "${T[TX_PER_SECOND]}") EVM transactions / second sustained" \
    "$(group "${T[BATCHES]}") BFT-ordered committer transactions" \
    "${T[ROLLED_BACK]} rolled-back batches — ${T[ABORTED]} aborted transactions")
  make_card "$DEMO_DIR/close-$PREFIX-$mode.mp4" "$out_fps" 6 "$cdir" "$n"

  final="$OUT_DIR/$PREFIX-$mode.mp4"
  concat_parts "$final" "$DEMO_DIR/title-$PREFIX-$mode.mp4" "$body" "$DEMO_DIR/close-$PREFIX-$mode.mp4"

  dur=$(duration_of "$final")
  size=$(du -h "$final" | cut -f1)
  log "$mode: $final (${dur}s, $size)"

  # The real-time invariant. A sped-up "real time" video is invisible to the eye
  # on a slow-moving dashboard, so assert it rather than trusting the pipeline.
  if [ "$mode" = "full" ]; then
    python3 - "$dur" "$RUN_SECONDS" <<'PY'
import sys
video, run = float(sys.argv[1]), float(sys.argv[2])
cards = 10.0  # title + closing card seconds, which are not part of the run
body = video - cards
drift = abs(body - run) / run
if drift > 0.01:
    sys.exit(f"FAIL: body is {body:.0f}s for a {run:.0f}s run "
             f"({drift*100:.1f}% off) -- not 1x real time")
print(f"ok: body {body:.0f}s == run {run:.0f}s (1x real time)")
PY
  fi

  if [ "$KEEP_FRAMES" -eq 0 ]; then
    rm -rf "$DEMO_DIR/frames/$PREFIX-$mode"
    log "$mode: frames deleted (--keep-frames to retain)"
  fi
  rm -f "$body" "$DEMO_DIR/title-$PREFIX-$mode.mp4" "$DEMO_DIR/close-$PREFIX-$mode.mp4"
done

log "done: $OUT_DIR"
ls -lh "$OUT_DIR"
