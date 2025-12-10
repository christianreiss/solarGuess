#!/usr/bin/env bash
# Minimal one-shot run + MQTT publish using config defaults.

set -euo pipefail

cd "$(dirname "$0")"

DATE=${1:-$(date +%F)}

# pick interpreter: prefer venv python, else system python3, else python
if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

PY_BIN=${PY_BIN:-}
if [ -z "$PY_BIN" ]; then
  for cand in python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then
      PY_BIN="$cand"
      break
    fi
  done
fi

if [ -z "$PY_BIN" ]; then
  echo "ERROR: no python interpreter found (looked for python3/python)." >&2
  exit 127
fi

# derive output path: prefer run.output; else derive from first site id; else fallback
OUT=$($PY_BIN - <<'PY'
import yaml, pathlib
cfg = pathlib.Path("etc/config.yaml")
if cfg.exists():
    data = yaml.safe_load(cfg.read_text()) or {}
    run = data.get("run") or {}
    if run.get("output"):
        print(run["output"])
        raise SystemExit
    sites = data.get("sites") or []
    if sites and isinstance(sites, list) and sites[0].get("id"):
        print(f"{sites[0]['id']}_live.json")
        raise SystemExit
print("live_results.json")
PY
)

PYTHONPATH=src $PY_BIN -m solarpredict.cli run \
  --config etc/config.yaml \
  --date "$DATE" \
  --output "$OUT"

# Wrap flat records into hierarchical payload with meta (matches MQTT expectations)
"$PY_BIN" - <<'PY' "$DATE" "$OUT"
import json, sys, datetime as dt, pathlib
from collections import defaultdict
date = sys.argv[1]
out_path = pathlib.Path(sys.argv[2])
data = json.loads(out_path.read_text())
if isinstance(data, dict) and "meta" in data and "sites" in data:
    raise SystemExit  # already hierarchical

# Rounded output (preserve meaningful peaks): energy 1dp, power/irradiance 3dp, temp 1dp
def r_energy(x):
    try:
        return round(float(x), 1)
    except Exception:
        return x

def r_power(x):
    try:
        return round(float(x), 3)
    except Exception:
        return x

def r_poa(x):
    try:
        return round(float(x), 3)
    except Exception:
        return x

def r_temp(x):
    try:
        return round(float(x), 1)
    except Exception:
        return x

by_site = defaultdict(list)
for rec in data:
    by_site[rec.get("site")].append(rec)

sites = []
for site_id, recs in by_site.items():
    recs_sorted = []
    for rec in sorted(recs, key=lambda r: r.get("array", "")):
        clean = dict(rec)
        clean.pop("site", None)
        clean.pop("date", None)
        if "energy_kwh" in clean: clean["energy_kwh"] = r_energy(clean["energy_kwh"])
        if "peak_kw" in clean: clean["peak_kw"] = r_power(clean["peak_kw"])
        if "poa_kwh_m2" in clean: clean["poa_kwh_m2"] = r_poa(clean["poa_kwh_m2"])
        if "temp_cell_max" in clean: clean["temp_cell_max"] = r_temp(clean["temp_cell_max"])
        recs_sorted.append(clean)
    site_total = r_energy(sum(r.get("energy_kwh", 0) or 0 for r in recs_sorted))
    sites.append({"id": site_id, "total_energy_kwh": site_total, "arrays": recs_sorted})

sites = sorted(sites, key=lambda s: s["id"] or "")
meta = {
    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    "date": date,
    "timestep": "15m",
    "provider": "open-meteo",
    "total_energy_kwh": r_energy(sum(s["total_energy_kwh"] for s in sites)),
    "site_count": len(sites),
    "array_count": sum(len(s["arrays"]) for s in sites),
}
payload = {"meta": meta, "sites": sites}
out_path.write_text(json.dumps(payload, indent=2))
PY

PYTHONPATH=src $PY_BIN -m solarpredict.cli publish-mqtt \
  --config etc/config.yaml \
  --input "$OUT"
