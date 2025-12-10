# solarGuess

Predict day-scale solar production with auditable, structured debug output. solarGuess ingests weather forecasts, runs a rigorous irradiance → DC → AC power chain for every array, and ships explainable results (timeseries + daily rollups) per site.

---

## Table of contents

1. [Why solarGuess exists](#why-solarguess-exists)
2. [High-level workflow](#high-level-workflow)
3. [Install & environment](#install--environment)
4. [Your first run (tutorial)](#your-first-run-tutorial)
5. [Configuration guide](#configuration-guide)
6. [Calculation pipeline (deep dive)](#calculation-pipeline-deep-dive)
7. [Debugging & audits](#debugging--audits)
8. [CLI reference](#cli-reference)
9. [Project layout](#project-layout)
10. [Testing](#testing)
11. [Home Assistant / MQTT integration](#home-assistant--mqtt-integration)
12. [Roadmap ideas](#roadmap-ideas)

---

## Why solarGuess exists

Utilities publish yesterday's production; asset owners need tomorrow's. solarGuess focuses on three promises:

- **End-to-end transparency.** Every transformation emits JSONL breadcrumbs (weather request, POA energy, inverter clipping, etc.). You can diff two runs and see *why* numbers moved.
- **Multi-site readiness.** A single scenario handles N sites × N arrays, including inverter grouping and shared losses.
- **Operator-friendly defaults.** sane time-label handling (Open-Meteo backward-averaged samples), deterministic tests, and CLI ergonomics that play well in cron.

---

## High-level workflow

> Think "weather forecast ➜ irradiance ➜ panel temperature ➜ DC ➜ AC ➜ energy".

```
Forecast (GHI/DNI/DHI, Tair, wind)
        │
    Step detection ──┐
        │            ▼
  Solar geometry  (pvlib)
        │            │
        ├──► Plane-of-array irradiance (Perez)
        │            │
        ├──► Cell temperature (SAPM)
        │            │
        ├──► DC power (PVWatts)
        │            │
        └──► AC power + losses → energy per array/site
```

---

## Install & environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

- Depends on `pvlib`, `pandas`, `requests`, `typer`, `PyYAML`.
- Weather data defaults to Open-Meteo public API; tests are allowed to call the network but fixtures keep them deterministic when possible.

---

## Your first run (tutorial)

1. **Copy the sample config.** Create `etc/config.yaml`:

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
            azimuth_deg: 180      # south-facing
            pdc0_w: 5000          # DC STC watts
            gamma_pdc: -0.004     # temp coefficient (1/°C)
            dc_ac_ratio: 1.2
            eta_inv_nom: 0.96
            losses_percent: 7
            temp_model: close_mount_glass_glass
    ```

2. **Run a forecast.**

    ```bash
    PYTHONPATH=src \
    python -m solarpredict.cli run \
      --config etc/config.yaml \
      --date 2025-06-01 \
      --weather-label end \
      --debug debug.jsonl
    ```

    - `--weather-label end` matches Open-Meteo's backward-averaged timestamps. Switch to `start` if your provider reports forward averages.
    - Add `--format csv --output results.csv` if you need a spreadsheet-friendly dump.

3. **Inspect results.** The command prints JSON summary (sites/arrays with `energy_kwh`, `peak_kw`, etc.) and the optional `debug.jsonl` captures every stage for audits.

---

## Configuration guide

Each scenario describes **sites**, **locations**, and **arrays**. Minimal schema:

```yaml
sites:
  - id: <site-id>
    location:
      id: <location-id>
      lat: <deg>
      lon: <deg>
      tz: <IANA tz>
      elevation_m: <optional>
    arrays:
      - id: <array-id>
        tilt_deg: 0..90
        azimuth_deg: -180..180   # 180 = south, 0 = north
        pdc0_w: STC DC watts
        gamma_pdc: -0.003..-0.005  # °C⁻¹
        dc_ac_ratio: >0
        eta_inv_nom: 0..1
        losses_percent: 0..100
        temp_model: pvlib SAPM key (e.g., close_mount_glass_glass)
        inverter_group_id: optional string to share an inverter
        inverter_pdc0_w: optional explicit DC input limit
```

Rules of thumb:

- `gamma_pdc`: -0.004 for mono-PERC owners unless you have factory curves.
- `dc_ac_ratio`: 1.1–1.3 for rooftops, ~1.1 for export-limited feeders.
- `eta_inv_nom`: 0.96 is realistic for modern string inverters; raise if you have datasheet evidence.
- `losses_percent`: 5–14% lumps wiring, mismatch, soiling. Start at 7–10%.
- `temp_model`: choose from pvlib SAPM table (`open_rack_glass_polymer`, `close_mount_glass_glass`, ...). Config must match exactly.
- `inverter_group_id`: arrays sharing the same physical inverter go into the same group so clipping is modeled correctly.

---

## Calculation pipeline (deep dive)

> Everything below lives in `src/solarpredict/**`. Matching function names are referenced so you can trace execution quickly.

### 1. Weather ingest (`weather.open_meteo.OpenMeteoWeatherProvider`)

- Calls Open-Meteo with `timestep ∈ {1h, 15m}` and auto timezones per site.
- Normalizes hourly/minutely blocks into a tz-aware `DataFrame` with `ghi_wm2`, `dni_wm2`, `dhi_wm2`, `temp_air_c`, `wind_ms`.
- Emits `weather.request`, `weather.response_meta`, and `weather.summary` debug events so you can replay API parameters and min/max irradiance.

### 2. Step detection (`engine.simulate._infer_step_seconds`)

- Median delta of the timestamp index (robust to DST jumps) becomes the canonical step width.
- When medians fail (single sample, missing data), falls back to declared CLI timestep.

### 3. Time-label alignment (`engine.simulate._apply_time_label`)

- Weather providers disagree about whether a value stamped `12:00` represents `[11:00,12:00]` or `[12:00,13:00]`.
- We shift to interval midpoints: `label=end` subtracts half a step, `label=start` adds half, `label=center` no-op.

### 4. Solar geometry (`solar.position.solar_position`)

- pvlib computes sun zenith/elevation/azimuth for the midpoint timestamps using site latitude, longitude, and elevation.
- Requires tz-aware index (we honor the provider timezone). Emits `solar_position.summary` with min/max elevation for quick sanity checks.

### 5. Plane-of-array irradiance (`solar.irradiance.poa_irradiance`)

- Perez transposition with `dni_extra` precomputed. Negative irradiance caused by API noise is clipped to zero for deterministic behavior.
- Returns columns `poa_global`, `poa_direct`, `poa_diffuse`, `poa_ground_diffuse` per array. Summary debug reports energy (`poa_wh_m2`) and peak POA.

### 6. Cell temperature (`solar.temperature.cell_temperature`)

- SAPM model keyed by `temp_model` (mounting configuration). Pulls coefficients from pvlib's parameter tables.
- Emits parameter dumps (`temp_model.params`) plus min/max cell temperature summary.

### 7. DC power (`pv.power.pvwatts_dc`)

- PVWatts DC using array-specific `pdc0_w`, `gamma_pdc`. Produces name-aligned `pdc_w` series.
- Debug payload shows min/max DC to catch wrong irradiance or shading assumptions.

### 8. Inverters & clipping (`engine.simulate` + `pv.power.pvwatts_ac`)

- Arrays join inverter groups via `inverter_group_id`. If you provide `inverter_pdc0_w`, it's honored verbatim; otherwise we derive it from `dc_ac_ratio` and `eta_inv_nom`.
- PVWatts inverter model returns clipped `pac_w`. We apportion group AC back to arrays using their instantaneous DC share to avoid energy creation.

### 9. System losses (`pv.power.apply_losses`)

- Lumped `losses_percent` applied to `pac_w` to produce `pac_net_w`. Debug log captures the factor and resulting min/max.

### 10. Energy integration & aggregation (`engine.simulate`)

- Per-array timeseries stored under `(site_id, array_id)` with POA, cell temp, DC, AC raw, and AC net columns.
- Daily rollups integrate `pac_net_w` × timestep hours (handles DST and irregular grids) into `energy_kwh`, `peak_kw`, `poa_kwh_m2`, `temp_cell_max`.
- Rollups are aggregated per site and globally in the CLI output / MQTT payloads.

---

## Debugging & audits

- Pass `--debug debug.jsonl` to any CLI command; internally this instantiates `JsonlDebugWriter` which produces deterministic JSON lines (`{"stage":"pv.dc.summary",...}`) for every site/array/stage.
- Each module accepts a `DebugCollector`, so custom tooling (e.g., send events to Loki) is trivial: implement collector → pass into `simulate_day`.
- Typical workflow to trace differences:

  ```bash
  diff <(jq -S . debug_run_a.jsonl) <(jq -S . debug_run_b.jsonl)
  # or
  rg --json "stage.dc" debug.jsonl
  ```

---

## CLI reference

| Command | Description |
| --- | --- |
| `run` | Execute a simulation for a date. Flags: `--config`, `--date`, `--timestep` (`1h`/`15m`), `--weather-label`, `--debug`, `--format {json,csv}`, `--output`. |
| `config` | Interactive YAML/JSON scenario builder/editor. Guides you through locations and arrays with validation using the same models as the engine. |

All CLI code lives in `src/solarpredict/cli.py` (Typer-based). Weather provider defaults to Open-Meteo but the CLI factory (`default_weather_provider`) makes dependency injection easy for tests.

---

## Project layout

```
src/solarpredict/
  cli.py                 # Typer CLI (run/config)
  core/
    config.py            # load_scenario, ConfigError, Scenario models
    debug.py             # DebugCollector implementations (JsonlDebugWriter, Scoped collectors)
    models.py            # Location, PVArray, Site, Scenario validation
  engine/
    simulate.py          # orchestrates weather → solar → PV → aggregation (simulate_day)
  weather/
    base.py              # WeatherProvider protocol
    open_meteo.py        # Open-Meteo implementation (default)
  solar/
    position.py          # pvlib solar geometry wrappers
    irradiance.py        # Perez POA helpers
    temperature.py       # SAPM cell temp wrappers
  pv/
    power.py             # PVWatts DC/AC, losses, inverter sizing
  integrations/
    ha_mqtt.py           # Home Assistant MQTT publisher
```

Tests mirror the package layout under `tests/` with fixtures in `tests/fixtures/`.

---

## Testing

```bash
PYTHONPATH=src python -m compileall src/solarpredict
PYTHONPATH=src pytest -q
```

- Syntax check ensures modules import cleanly (mandatory per AGENTS.md).
- Pytest covers config parsing, engine happy-paths, PV/solar math, CLI wiring, and integrations. Network hits are acceptable when fixture coverage is insufficient, but most tests rely on recorded data.

---

## Home Assistant / MQTT integration

After a daily run (write `live_results.json` with your scheduler), publish to Home Assistant using the main CLI:

```bash
# simulate + publish (topics only by default)
PYTHONPATH=src python -m solarpredict.cli run \
  --config etc/config.yaml \
  --date 2025-12-10 \
  --timestep 15m \
  --format json \
  --output live_results.json

PYTHONPATH=src python -m solarpredict.cli publish-mqtt \
  --config etc/config.yaml \
  --verify --publish-retries 3 --retry-delay 2 --skip-if-fresh
```

- Publishes to `solarguess/forecast` and `solarguess/availability` with retained messages.
- Discovery payload registers `sensor.solarguess_forecast` whose state is `total_energy_kwh`; attributes mirror the per-site/array breakdown.
- Publishing only proceeds when the local `generated_at` is newer *and* the payload changed (prevents timestamp churn).
- Use `--publish-topics --no-state` to emit only scalar topics (skip retained blob).
- Recovery procedure for stuck entities:

  ```bash
  mosquitto_pub -h <broker> -p 1883 -u <user> -P '<pass>' -r -n -t solarguess/forecast
  mosquitto_pub -h <broker> -p 1883 -u <user> -P '<pass>' -r -n -t solarguess/availability
  PYTHONPATH=src python -m solarpredict.cli publish-mqtt --config etc/config.yaml --verbose --force
  ```

Health/verification:
- `publish-mqtt` supports `--verify` to read back retained state and confirm the payload matches the just-published forecast (hash compare).
- Use `--publish-retries N` to retry publish+verify if the broker is briefly unavailable; `--retry-delay` controls spacing.
- Discovery publish can be disabled with `--no-discovery` or `mqtt.publish_discovery: false`.

---

## Roadmap ideas

- Alternative weather providers (NOAA NBM, Meteomatics) with the same DebugCollector plumbing.
- Probabilistic ensembles: run the pipeline across multiple weather scenarios and emit P10/P50/P90 energy bands.
- FastAPI wrapper for on-demand forecasts with per-request debug streaming.
- Built-in visual diff tooling for JSONL debug streams.

---

Need help or want to contribute a module? Open an issue or PR; the repo prefers one-task-per-PR with tests + debug coverage per AGENTS.md.
