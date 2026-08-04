#!/usr/bin/env bash
#
# Assert every PromQL expression in a dashboard actually returns data.
#
# The point is to catch a typo'd or renamed metric BEFORE an overnight run,
# rather than discovering an empty panel in the finished video. Run it against a
# live stack WITH a replay in progress: on an idle stack the loadgen job has no
# series, so "NO DATA" then is expected and means nothing.
#
# Usage: scripts/demo/check_dashboard_queries.sh [dashboard.json]
#        PROM=http://host:9090 scripts/demo/check_dashboard_queries.sh
set -euo pipefail

DASHBOARD="${1:-config/monitoring/grafana/evm-demo.json}"
PROM="${PROM:-http://localhost:9090}"

if [ ! -f "$DASHBOARD" ]; then
  echo "error: dashboard not found: $DASHBOARD" >&2
  exit 2
fi

mapfile -t exprs < <(python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
seen = set()
for p in d["panels"]:
    for t in p.get("targets") or []:
        e = t.get("expr")
        if e and e not in seen:
            seen.add(e)
            print(e)
' "$DASHBOARD")

if [ "${#exprs[@]}" -eq 0 ]; then
  echo "error: no PromQL expressions found in $DASHBOARD" >&2
  exit 2
fi

echo "Checking ${#exprs[@]} expressions against $PROM"
fail=0
for e in "${exprs[@]}"; do
  # $__rate_interval is a Grafana macro Prometheus does not understand; substitute
  # a concrete window so the expression is checkable as written otherwise.
  probe=${e//\$__rate_interval/1m}
  n=$(curl -sG --max-time 20 "$PROM/api/v1/query" --data-urlencode "query=$probe" \
      | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)
except Exception:
    print(-1); raise SystemExit
print(len(r.get("data", {}).get("result", [])) if r.get("status") == "success" else -1)
')
  case "$n" in
    -1) printf 'QUERY ERROR  %s\n' "$e"; fail=1 ;;
     0) printf 'NO DATA      %s\n' "$e"; fail=1 ;;
     *) printf 'ok (%s series) %s\n' "$n" "$e" ;;
  esac
done

if [ "$fail" -ne 0 ]; then
  echo
  echo "FAIL: at least one panel would render empty in the video." >&2
  exit 1
fi
echo
echo "PASS: every panel has data."
