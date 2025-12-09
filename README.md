# solarGuess

Day-scale solar generation predictor with auditable debug output. Forecast weather, compute sun position, POA irradiance, cell temperature, DC/AC power, and aggregate daily energy for multiple sites and arrays.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Dependencies: pvlib, pandas, requests, PyYAML, typer. Default weather provider (Open-Meteo) hits the public API; tests can run offline with fixtures but are allowed to use the network when needed.

## Quick start

1) Create a scenario file `scenario.yaml`:

```yaml
sites:
  - id: site1
    location:
      id: loc1
      lat: 40.0
      lon: -105.0
      tz: America/Denver
    arrays:
      - id: roof
        tilt_deg: 30
        azimuth_deg: 180
        pdc0_w: 5000
        gamma_pdc: -0.004
        dc_ac_ratio: 1.2
        eta_inv_nom: 0.96
        losses_percent: 7
        temp_model: close_mount_glass_glass
```

2) Run a daily simulation:

```bash
PYTHONPATH=src \
python -m solarpredict.cli run --config scenario.yaml --date 2025-06-01 --timestep 1h --format json --output results.json
```

Add `--debug debug.jsonl` to emit deterministic JSONL events for every stage.

## CLI summary

- `run`: execute simulation for a date, write summary as JSON/CSV, optional debug JSONL.
- `config <path>`: interactive builder/editor for scenario files (YAML/JSON).

## Project structure

- `src/solarpredict/core`: domain models, config loader, debug collectors
- `src/solarpredict/weather`: provider interface + Open-Meteo client
- `src/solarpredict/solar`: position, POA irradiance, cell temperature
- `src/solarpredict/pv`: PVWatts DC/AC wrappers and losses
- `src/solarpredict/engine`: end-to-end simulation orchestrator
- `src/solarpredict/cli.py`: Typer-based entrypoint

## Tests

```bash
PYTHONPATH=src python -m compileall src/solarpredict
PYTHONPATH=src pytest -q
```

Tests are fixture-driven where possible; network calls are acceptable when required by a provider.

## Debugging and auditing

All modules emit structured debug events. Use `--debug <file>` on the CLI or pass a `JsonlDebugWriter` into the engine to capture per-stage payloads for diffing and audits.

