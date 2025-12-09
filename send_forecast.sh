#!/usr/bin/env bash
# Publish latest forecast JSON to Home Assistant via MQTT.
# Reads config defaults from etc/config.yaml (or .example.yaml if missing),
# with CLI overrides available in the Python module.
# Flags:
#   --debug  : enable verbose MQTT logging (maps to --verbose)
#   --force  : publish even if unchanged/older (maps to --force)
#   remaining args are passed through to the Python module.

set -euo pipefail

cd "$(dirname "$0")"

DEBUG=0
FORCE=0
NO_STATE=0
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug) DEBUG=1 ;;
    --force) FORCE=1 ;;
    --no-state) NO_STATE=1 ;;
    *) EXTRA+=("$1") ;;
  esac
  shift
done

CONFIG=etc/config.yaml
[ -f "$CONFIG" ] || CONFIG=etc/config.example.yaml

PYTHONPATH=src python -m solarpredict.integrations.ha_mqtt \
  --config "$CONFIG" \
  ${DEBUG:+--verbose} \
  ${FORCE:+--force} \
  ${NO_STATE:+--no-state} \
  "${EXTRA[@]}"
