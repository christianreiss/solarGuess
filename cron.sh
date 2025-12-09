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

