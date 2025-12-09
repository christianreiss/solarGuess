#!/usr/bin/env bash
# Run daily prediction and write the latest JSON output.
# Usage: cron.sh [YYYY-MM-DD] [output.json] [debug.jsonl]

set -euo pipefail

# Always run from repo root
cd "$(dirname "$0")"

DATE=${1:-$(date +%F)}
CFG=etc/config.yaml

# Read defaults from config; fall back if missing
RUN_TIMESTEP=$(python - <<'PY'
import yaml, pathlib, sys
cfg = pathlib.Path('etc/config.yaml')
try:
    data = yaml.safe_load(cfg.read_text()) or {}
    run = data.get('run', {})
    print(run.get('timestep', '15m'))
    print(run.get('format', 'json'))
    print(run.get('output', 'live_results.json'))
    print(run.get('debug', 'live_debug.jsonl'))
except FileNotFoundError:
    print('15m')
    print('json')
    print('live_results.json')
    print('live_debug.jsonl')
PY
)
TIMESTEP=$(echo "$RUN_TIMESTEP" | sed -n '1p')
FORMAT=$(echo "$RUN_TIMESTEP" | sed -n '2p')
OUT=$(echo "$RUN_TIMESTEP" | sed -n '3p')
DEBUG=$(echo "$RUN_TIMESTEP" | sed -n '4p')

# Use local venv when present
if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

PYTHONPATH=src python -m solarpredict.cli run \
  --config "$CFG" \
  --date "$DATE" \
  --timestep "$TIMESTEP" \
  --format "$FORMAT" \
  --output "$OUT" \
  --debug "$DEBUG"

# Wrap results with metadata for downstream consumers.
python - <<'PY' "$DATE" "$OUT"
import json, sys, datetime as dt, pathlib
from collections import defaultdict
from datetime import timezone
date = sys.argv[1]
out_path = pathlib.Path(sys.argv[2])
data = json.loads(out_path.read_text())

# Round energy counters to 1 decimal for readability/consistency.
for rec in data:
    if "energy_kwh" in rec:
        rec["energy_kwh"] = round(rec["energy_kwh"], 1)
    if "peak_kw" in rec:
        rec["peak_kw"] = round(rec["peak_kw"], 1)
    if "poa_kwh_m2" in rec:
        rec["poa_kwh_m2"] = round(rec["poa_kwh_m2"], 1)
    if "temp_cell_max" in rec:
        rec["temp_cell_max"] = round(rec["temp_cell_max"], 1)

# Group arrays by site to build a hierarchical payload.
by_site = defaultdict(list)
for rec in data:
    by_site[rec.get("site")].append(rec)

sites = []
for site_id, recs in by_site.items():
    recs_sorted = []
    for rec in sorted(recs, key=lambda r: r.get("array", "")):
        clean = dict(rec)
        # site/date are redundant once nested under site/meta; strip to keep payload lean.
        clean.pop("site", None)
        clean.pop("date", None)
        recs_sorted.append(clean)

    site_total = round(sum(r.get("energy_kwh", 0) for r in recs_sorted), 1)
    sites.append(
        {
            "id": site_id,
            "total_energy_kwh": site_total,
            "arrays": recs_sorted,
        }
    )

sites = sorted(sites, key=lambda s: s["id"] or "")

meta = {
    "generated_at": dt.datetime.now(timezone.utc).isoformat(),
    "date": date,
    "timestep": "15m",
    "provider": "open-meteo",
    "total_energy_kwh": round(sum(s["total_energy_kwh"] for s in sites), 1),
    "site_count": len(sites),
    "array_count": len(data),
}

payload = {"meta": meta, "sites": sites}
out_path.write_text(json.dumps(payload, indent=2))
PY
