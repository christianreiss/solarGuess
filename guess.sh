#!/usr/bin/env bash
# Minimal one-shot run + MQTT publish using config defaults.

set -euo pipefail

cd "$(dirname "$0")"

DATE=${1:-$(date +%F)}

# Basic log framing for cron/appended logs.
echo "=== solarguess $(date -Is) date=${DATE} ==="

# Default to tuned config when present (keeps etc/config.yaml as the "base" file).
CONFIG=${CONFIG:-}
if [ -z "${CONFIG}" ]; then
  if [ -f etc/config.tuned.yaml ]; then
    CONFIG=etc/config.tuned.yaml
  else
    CONFIG=etc/config.yaml
  fi
fi

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

# derive output path: prefer run.output from config, but allow strftime-style
# tokens so we can keep per-day history (json/YYYY-MM-DD.json).
OUT=$($PY_BIN - <<PY
import datetime as dt
import pathlib
import yaml

date = dt.date.fromisoformat("${DATE}")
cfg = pathlib.Path("${CONFIG}")

out = None
if cfg.exists():
    data = yaml.safe_load(cfg.read_text()) or {}
    run = data.get("run") or {}
    out = run.get("output")

if not out:
    out = "json/%F.json"

out = str(out)
out = out.replace("%F", date.isoformat())
print(out)
PY
)

mkdir -p "$(dirname "$OUT")"

echo "[guess.sh] config=${CONFIG} output=${OUT}"

RUN_ARGS=()
if [ "${RUN_FORCE:-0}" = "1" ]; then
  RUN_ARGS+=(--force)
fi

PYTHONPATH=src $PY_BIN -m solarpredict.cli run \
  --config "$CONFIG" \
  --date "$DATE" \
  --pvgis-cache-dir .cache/pvgis \
  --output "$OUT" \
  "${RUN_ARGS[@]}"

MQTT_ARGS=(
  --publish-retries "${MQTT_PUBLISH_RETRIES:-3}"
  --retry-delay "${MQTT_RETRY_DELAY:-2}"
)
if [ "${MQTT_VERIFY:-1}" = "1" ]; then
  MQTT_ARGS+=(--verify)
fi
if [ "${MQTT_FORCE:-0}" = "1" ]; then
  MQTT_ARGS+=(--force)
fi

PYTHONPATH=src $PY_BIN -m solarpredict.cli publish-mqtt \
  --config "$CONFIG" \
  --input "$OUT" \
  "${MQTT_ARGS[@]}"
