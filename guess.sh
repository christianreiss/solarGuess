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

PYTHONPATH=src $PY_BIN -m solarpredict.cli publish-mqtt \
  --config etc/config.yaml \
  --input "$OUT"
