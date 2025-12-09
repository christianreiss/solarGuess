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

1) Create a combined config `etc/config.yaml` (includes scenario + MQTT + run defaults):

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
        pdc0_w: 5000            # array STC DC watts
        gamma_pdc: -0.004       # power temp coefficient 1/°C (typical crystalline Si)
        dc_ac_ratio: 1.2        # DC nameplate / AC nameplate
        eta_inv_nom: 0.96       # nominal inverter efficiency
        losses_percent: 7       # lump-sum losses (%)
        temp_model: close_mount_glass_glass
```

2) Run a daily simulation:

```bash
PYTHONPATH=src \
python -m solarpredict.cli run --config etc/config.yaml --date 2025-06-01
# If your weather API uses backward-averaged timestamps (Open-Meteo), the default `--weather-label end`
# keeps solar geometry aligned. Use `--weather-label start` if your data is forward-averaged.
```

Add `--debug debug.jsonl` to emit deterministic JSONL events for every stage.

### Picking reasonable numbers (rules of thumb)

- `gamma_pdc`: -0.003 to -0.005 /°C for most mono/mono-PERC modules; -0.004 is a safe default.
- `dc_ac_ratio`: 1.1–1.3 is common for rooftop; lower if grid export limits are tight.
- `eta_inv_nom`: 0.95–0.97 for modern string inverters.
- `losses_percent`: 5–14% lumping wiring, soiling, mismatch; start at 7–10%.
- `tilt_deg`/`azimuth_deg`: use roof tilt and azimuth (south = 180, north = 0 in this convention).

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

## Publish to Home Assistant via MQTT

After `cron.sh` writes `live_results.json`, publish to Home Assistant MQTT with change/freshness guards:

```bash
PYTHONPATH=src python -m solarpredict.integrations.ha_mqtt \
  --config etc/config.yaml \
  --verbose --force

# Skip publishing the retained state blob (only scalars)
PYTHONPATH=src python -m solarpredict.integrations.ha_mqtt \
  --config etc/config.yaml \
  --publish-topics --no-state --force
```

Behavior:
- Publishes only if the local `generated_at` is newer **and** the content changed (ignores timestamp-only churn).
- Publishes HA discovery for `sensor.solarguess_forecast`; state is `total_energy_kwh`, attributes mirror `results`.
- Uses retained messages on `solarguess/forecast` and availability at `solarguess/availability`.
- New hierarchical payload (`meta` + `sites[].arrays[]`) is normalized before publish; totals are auto-filled if missing.
- To recover from a stuck/unavailable entity, clear retained once then republish:
  ```bash
  mosquitto_pub -h <broker> -p 1883 -u <user> -P '<pass>' -r -n -t solarguess/forecast
  mosquitto_pub -h <broker> -p 1883 -u <user> -P '<pass>' -r -n -t solarguess/availability
  PYTHONPATH=src python -m solarpredict.integrations.ha_mqtt --config etc/config.yaml --verbose --force
  ```

## Debugging and auditing

All modules emit structured debug events. Use `--debug <file>` on the CLI or pass a `JsonlDebugWriter` into the engine to capture per-stage payloads for diffing and audits.
