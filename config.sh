#!/usr/bin/env bash
# Thin wrapper to launch the solarGuess config TUI.

set -euo pipefail

CONFIG_PATH="${1:-etc/config.yaml}"
shift 2>/dev/null || true

if ! command -v solarpredict >/dev/null 2>&1; then
  echo "solarpredict CLI not found; install with 'pip install -e .'" >&2
  exit 1
fi

exec solarpredict config "${CONFIG_PATH}" "$@"
