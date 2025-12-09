#!/usr/bin/env bash
# Run daily prediction and write the latest JSON output.
# Usage: cron.sh [YYYY-MM-DD] [output.json] [debug.jsonl]

set -euo pipefail

# Always run from repo root
cd "$(dirname "$0")"

DATE=${1:-$(date +%F)}
OUT=${2:-live_results.json}
DEBUG=${3:-live_debug.jsonl}

# Use local venv when present
if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

PYTHONPATH=src python -m solarpredict.cli run \
  --config scenario.yaml \
  --date "$DATE" \
  --timestep 1h \
  --format json \
  --output "$OUT" \
  --debug "$DEBUG"

# Wrap results with metadata for downstream consumers.
python - <<'PY' "$DATE" "$OUT"
import json, sys, datetime as dt, pathlib
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

payload = {
    "generated_at": dt.datetime.now(timezone.utc).isoformat(),
    "date": date,
    "timestep": "1h",
    "provider": "open-meteo",
    "total_energy_kwh": round(sum(r.get("energy_kwh", 0) for r in data), 1),
    "results": data,
}
out_path.write_text(json.dumps(payload, indent=2))
PY
