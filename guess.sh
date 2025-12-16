#!/usr/bin/env bash
# Legacy wrapper that now delegates to the Python CLI `solarguess go`.

set -euo pipefail

cd "$(dirname "$0")"

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

ARGS=()

DATE=${1:-}
if [ -n "$DATE" ]; then
  ARGS+=(--date "$DATE")
fi

if [ -n "${CONFIG:-}" ]; then
  ARGS+=(--config "$CONFIG")
fi

if [ "${RUN_FORCE:-}" = "1" ]; then
  ARGS+=(--force)
elif [ "${RUN_FORCE:-}" = "0" ]; then
  ARGS+=(--no-force)
fi

if [ "${MQTT_PUBLISH:-}" = "0" ]; then
  ARGS+=(--no-publish)
elif [ "${MQTT_PUBLISH:-}" = "1" ]; then
  ARGS+=(--publish)
fi

if [ "${MQTT_FORCE:-}" = "1" ]; then
  ARGS+=(--mqtt-force)
elif [ "${MQTT_FORCE:-}" = "0" ]; then
  ARGS+=(--no-mqtt-force)
fi

if [ -n "${MQTT_VERIFY:-}" ]; then
  if [ "${MQTT_VERIFY}" = "1" ]; then
    ARGS+=(--verify)
  else
    ARGS+=(--no-verify)
  fi
fi

if [ -n "${MQTT_PUBLISH_RETRIES:-}" ]; then
  ARGS+=(--publish-retries "${MQTT_PUBLISH_RETRIES}")
fi

if [ -n "${MQTT_RETRY_DELAY:-}" ]; then
  ARGS+=(--retry-delay "${MQTT_RETRY_DELAY}")
fi

if [ -n "${MQTT_SKIP_IF_FRESH:-}" ]; then
  if [ "${MQTT_SKIP_IF_FRESH}" = "1" ]; then
    ARGS+=(--skip-if-fresh)
  else
    ARGS+=(--no-skip-if-fresh)
  fi
fi

if [ "${#ARGS[@]}" -eq 0 ]; then
  echo "=== solarguess $(date -Is) args: <defaults> ==="
else
  echo "=== solarguess $(date -Is) args: ${ARGS[*]} ==="
fi

PYTHONPATH=src $PY_BIN -m solarpredict.cli go "${ARGS[@]}"
