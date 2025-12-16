# solarGuess

Predict day-scale solar production with auditable, structured debug output. solarGuess ingests weather forecasts, runs a rigorous irradiance → DC → AC power chain for every array, and ships explainable results (timeseries + daily rollups) per site. Weather inputs come from Open-Meteo (live) and the EU JRC **PVGIS** (Photovoltaic Geographical Information System) TMY dataset for baselines and QA.

---

## Table of contents

1. [Why solarGuess exists](#why-solarguess-exists)
2. [High-level workflow](#high-level-workflow)
3. [Install & environment](#install--environment)
4. [Your first run (tutorial)](#your-first-run-tutorial)
5. [Configuration guide](#configuration-guide)
6. [Calculation pipeline (deep dive)](#calculation-pipeline-deep-dive)
7. [New modeling features](#new-modeling-features)
8. [Debugging & audits](#debugging--audits)
9. [CLI reference](#cli-reference)
10. [Project layout](#project-layout)
11. [Testing](#testing)
12. [Home Assistant / MQTT integration](#home-assistant--mqtt-integration)
13. [Roadmap ideas](#roadmap-ideas)

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
Forecast inputs (Open-Meteo live + cloud cover, PVGIS TMY baseline/cache)
        │
        ├──► Composite / cloud-scaled merge (optional)
        │
    Step detection + time-label alignment
        │
  Solar geometry (pvlib)
        │
        ├──► Plane-of-array irradiance (Perez + horizon mask + IAM)
        │            │
        ├──► Morning/evening damping          │
        │            │                        │
        ├──► Cell temperature (SAPM)          │
        │            │                        │
        ├──► DC power (PVWatts)               │
        │            │                        │
        ├──► Inverter grouping + clipping     │
        │            │                        │
        ├──► Losses → AC net                  │
        │            │                        │
        └──► Aggregation → QC (PVGIS compare + clear-sky ceiling) → load windows / actual scaling
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

2. **Run a forecast (and optionally publish).**

    ```bash
    PYTHONPATH=src solarguess go --date 2025-06-01 --no-publish
    ```

    - `solarguess go` wraps the old `run` + `publish-mqtt` steps in one command. It reads defaults from your config's `run:` and `mqtt:` sections, so `--config`, `--output`, `--debug`, `--input`, etc. no longer need to be repeated.
    - Use `--publish/--no-publish` to override `mqtt.enable`. When you're ready to wire up MQTT just drop the `--no-publish`.
    - Need just the simulator? `solarguess run` still exists with the same flags as before, and it now infers `--config`, `--date`, `--output`, `--debug`, `--intervals`, and friends from the config when not provided.
    - `--weather-label end` matches Open-Meteo's backward-averaged timestamps. Switch to `start` if your provider reports forward averages; set it once under `run.weather_label` to make it the default.
    - Want climatology instead of a live forecast? Set `run.weather_source: pvgis-tmy` or pass `--weather-source pvgis-tmy` (optionally with `--pvgis-cache-dir .cache/pvgis`) to fetch PVGIS (EU JRC Photovoltaic Geographical Information System) typical-year irradiance for your coordinates.
    - To sanity-check live forecasts against PVGIS, add `--qc-pvgis` or set `run.qc_pvgis: true`; the CLI will emit `qc.pvgis_compare` debug events so you can diff live vs. typical energy/POA.

3. **Inspect results.** The command prints JSON summary (sites/arrays with `energy_kwh`, `peak_kw`, etc.) and the optional `debug.jsonl` captures every stage for audits.

### Interactive config editor (TUI)

Temporarily disabled while we redo the config UX. Run `solarguess --help` for current commands.

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

### Config-driven CLI defaults

`solarguess run`, `go`, and `publish-mqtt` read their defaults from the scenario file so you only need to declare paths and switches once.

- Under `run:` you can set `date`, `timestep`, `format`, `output`, `debug`, `intervals`, `weather_label`, `weather_source`, `weather_mode`, `scale_factor`, `force`, `pvgis_cache_dir`, `qc_pvgis`, and load-window knobs. Paths accept `strftime` tokens such as `%F` so `json/%F.json` turns into `json/2025-12-16.json`.
- Under `mqtt:` configure `enable`, `input`, `verify`, `force`, `publish_retries`, `retry_delay`, `skip_if_fresh`, `publish_topics`, `publish_discovery`, and broker credentials. `mqtt.enable: true` makes `solarguess go` publish by default; `--publish/--no-publish` overrides it per run.

Example:

```yaml
run:
  date: 2025-12-01
  output: json/%F.json
  debug: debug/%F.jsonl
  intervals: intervals/%F.csv
  weather_label: end
  weather_source: open-meteo

mqtt:
  enable: true
  input: json/%F.json
  verify: true
  publish_retries: 3
  retry_delay: 2
  skip_if_fresh: true
```

With that in place `solarguess go --date 2025-12-16` will run + publish using those defaults; leaving `--date` out reverts to `run.date` or today's date.

---

## Calculation pipeline (deep dive)

> Everything below lives in `src/solarpredict/**`. Matching function names are referenced so you can trace execution quickly.

### 1. Weather ingest (`weather.open_meteo.OpenMeteoWeatherProvider`, `weather.pvgis.PVGISWeatherProvider`, `weather.composite.CompositeWeatherProvider`)

- Open-Meteo: live forecast (1h or 15m) with tz autodetect per site.
- PVGIS TMY: fetches the EU JRC PVGIS typical meteorological year JSON, re-stamps timestamps to the requested year, and optionally caches responses per lat/lon on disk. Perfect for baseline sanity checks or offline runs.
- Composite: always runs the primary provider (default Open-Meteo) and fills NaNs or negative irradiance values from PVGIS before handing data to the rest of the pipeline.
- All providers emit `weather.request`, `weather.response_meta`, `weather.summary`, plus `weather.merge` for composite stats.

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
- Ground-reflected diffuse uses per-array `albedo` (default `0.2`), so snow/bright ground can be modeled explicitly when needed.
- Returns columns `poa_global`, `poa_direct`, `poa_diffuse`, `poa_ground_diffuse` per array. Summary debug reports energy (`poa_wh_m2`) and peak POA.

### 6. Cell temperature (`solar.temperature.cell_temperature`)

- SAPM model keyed by `temp_model` (mounting configuration). Pulls coefficients from pvlib's parameter tables.
- Emits parameter dumps (`temp_model.params`) plus min/max cell temperature summary.

### 7. DC power (`pv.power.pvwatts_dc`)

- PVWatts DC using array-specific `pdc0_w`, `gamma_pdc`. Produces name-aligned `pdc_w` series.
- Debug payload shows min/max DC to catch wrong irradiance or shading assumptions.

### 8. Damping & horizon masking (`engine.simulate._damping_factor`, `solar.irradiance.poa_irradiance`)

- Optional `damping` (single value) or `damping_morning` / `damping_evening` attenuate POA near sunrise/sunset with a cosine window (~1.5 h). Perfect for east-west row shading or mandated curtailment.
- `horizon_deg` (≥12 azimuth bins) blanks the direct beam when local terrain/structures exceed the sun elevation. Diffuse + ground components remain so twilight production stays realistic.

### 9. IAM (incidence angle modifiers, `solar.incidence.apply_iam`)

- Arrays can specify `iam_model: ashrae` plus `iam_coefficient` (`b0`) or use CLI overrides (`--iam-model`, `--iam-coefficient`).
- IAM derates only the direct component using pvlib, then recomposes `poa_global = direct + diffuse + ground` so downstream energy sums are consistent.

### 10. Inverters & clipping (`engine.simulate` + `pv.power.pvwatts_ac`)

- Arrays join inverter groups via `inverter_group_id`. If you provide `inverter_pdc0_w`, it's honored verbatim; otherwise we derive it from `dc_ac_ratio` and `eta_inv_nom`.
- PVWatts inverter model returns clipped `pac_w`. We apportion group AC back to arrays using their instantaneous DC share to avoid energy creation.

### 11. System losses (`pv.power.apply_losses`)

- Lumped `losses_percent` applied to `pac_w` to produce `pac_net_w`.
- Important: this is applied *after* the inverter model, so treat `losses_percent` as “everything except inverter conversion efficiency” (wiring, mismatch, soiling, availability, etc.) to avoid double-counting inverter losses.

### 12. Energy integration & aggregation (`engine.simulate`)

- Per-array timeseries stored under `(site_id, array_id)` with POA, cell temp, DC, AC raw, and AC net columns.
- Daily rollups integrate `pac_net_w` × timestep hours (handles DST and irregular grids) into `energy_kwh`, `peak_kw`, `poa_kwh_m2`, `temp_cell_max`.
- Rollups are aggregated per site and globally in the CLI output / MQTT payloads.

---

## New modeling features

### Cloud-scaled weather mode (`weather.cloud_scaled.CloudScaledWeatherProvider`)

- Use `--weather-mode cloud-scaled` (or `run.weather_mode: cloud-scaled`) to bypass noisy irradiance feeds and instead scale pvlib clear-sky by Open-Meteo cloud cover.
- Converts cloud % → clearness index via `k_t = 1 - 0.75 * C**3.4`, clamps [0,1], scales clear-sky **GHI**, then derives DNI/DHI via decomposition (so cloudier conditions shift energy toward diffuse rather than scaling beam/diffuse equally). Preserves Open-Meteo temp/wind for SAPM temperature.
- Emits `cloudscaled.summary` debug with clearness min/mean/max plus resulting `ghi_max` so you can diff runs quickly.

### Incidence angle modifiers (IAM)

- Arrays accept `iam_model: ashrae` and optional `iam_coefficient` (ASHRAE `b0`) to derate low-angle direct beam.
- CLI overrides (`--iam-model/--iam-coefficient`) let you A/B coatings without touching configs.
- IAM only touches `poa_direct`; we recompute `poa_global = direct + diffuse + ground` so energy accounting stays additive.

### Morning/evening damping & horizon masks

- `damping` (single) or `damping_morning`/`damping_evening` attenuate POA near sunrise/sunset using a cosine ramp (~1.5 h window). Ideal for self-consumption caps or shading heuristics.
- `horizon_deg` (≥12 evenly spaced azimuth samples) blanks the direct beam when local terrain exceeds sun elevation. We interpolate circularly and leave diffuse terms untouched.

### PVGIS QC + clipping

- `--qc-pvgis` spins a parallel PVGIS run and compares POA energy vs typical-year climatology. We *warn* when the ratio is outside the heuristic band (~0.6–1.6×, wider when cloudy) but avoid hard clamping for plausible weather extremes.
- We only clamp when the forecast exceeds a **clear-sky POA ceiling** (physical guardrail). Daily rows carry `pvgis_poa_kwh_m2`; if clamping triggers we set `qc_clipped`, `qc_clip_reason=clearsky_ceiling`, plus `qc_clearsky_*` fields for auditability.
- When clipping fires we rescale POA/DC/AC timeseries so debug output, MQTT payloads, and load windows align with the adjusted totals.

### Load window detection (`engine.load_window`)

- Provide `base_load_w`, `min_duration_min`, and optional `required_wh` (config `run.*` or CLI flags) to scan per-site `pac_net_w` for dispatchable windows.
- Output JSON embeds `load_windows.best|earliest|latest` with ISO stamps, kWh, duration, and peak/avg watts. Perfect for EV charging or resistive loads.

### Actuals-based adjustment

- `--actual-kwh-today` (or `run.actual_kwh_today`) scales only *future* intervals so cumulative energy up to “now” equals telemetry. Use `--actual-as-of` to pin the split point; we clamp to the simulated date bounds otherwise.
- Debug emits `actual.adjust.*` events with predicted vs. actual kWh, sample counts, and applied scale for audit trails.

### Empirical scale factor

- `--scale-factor` (or `run.scale_factor`) applies a constant multiplier to DC/AC outputs (`pdc_w`, `pac_w`, `pac_net_w`), plus `energy_kwh` and `peak_kw`. This is a pragmatic calibration knob for systematic bias (weather model, configuration drift, soiling season, etc.).
- `solarguess ha-compare` prints a suggested multiplier (median actual/pred) and can write a tuned config via `--write-config`.

### Per-array scale factors (auto-tuned from HA)

- `run.array_scale_factors` lets you apply *different* multipliers per array (or per `site/array` pair). This is useful when one subsystem is consistently biased (e.g., a separate inverter, persistent shading, snow, or just wrong nameplate).
- `solarguess ha-tune` auto-detects groups from your HA export (e.g., `house_north`, `house_south`, `solarfarm_*`, `playhouse_phase_*`), runs historical sims, and writes `run.array_scale_factors` into a tuned config.

### Open-Meteo composite fallback

- `weather_source: composite` (or `--weather-source composite`) runs Open-Meteo live plus PVGIS TMY. Any NaN or negative irradiance is backfilled from PVGIS, logged via `weather.merge` and `weather.merge_detail` so you can see how often climatology stepped in.

### Incidence-aware load forecasting

- All per-site load windows reuse the same `pac_net_w` and interval widths used for energy integration, so dispatch planning aligns exactly with the published forecast.
- Optional `required_wh` ensures windows supply enough energy for target loads (e.g., “need ≥6 kWh for the battery heater”).

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
| `run` | Execute a simulation for a date. Flags: `--config`, `--date`, `--timestep` (`1h`/`15m`), `--weather-label`, `--weather-source {open-meteo,pvgis-tmy,composite}`, `--weather-mode {standard,cloud-scaled}`, `--scale-factor`, `--pvgis-cache-dir`, `--qc-pvgis`, `--debug`, `--format {json,csv}`, `--output`. |
| `ha-compare` | Compare predicted daily totals against HA history for a single `*_energy_today` sensor. Prints a suggested `run.scale_factor` and optionally writes a tuned config. |
| `ha-tune` | Auto-train per-array scale factors from an HA export (subsystem sensors). Writes `run.array_scale_factors` into a tuned config. |
| `publish-mqtt` | Publish a forecast JSON to MQTT/Home Assistant (supports retained state, discovery, verify). |
| `config` | Disabled (legacy). |

All CLI code lives in `src/solarpredict/cli.py` (Typer-based). Weather provider defaults to Open-Meteo but the CLI factory (`default_weather_provider`) makes dependency injection easy for tests. Install exposes `solarguess` (preferred) and `solarpredict` console scripts.

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
    pvgis.py             # PVGIS TMY climatology provider (cacheable)
    composite.py         # Merge live+climatology with deterministic backfill
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

## Comparing Against Home Assistant History

If you have a Home Assistant export of daily max values for `*_energy_today` sensors
(example shape: `pv_data_2025.json`), you can compare predicted daily totals against
actual production:

```bash
PYTHONPATH=src solarguess ha-compare \
  --config etc/config.yaml \
  --ha pv_data_2025.json \
  --entity sensor.total_pv_energy_today \
  --start 2025-09-15 \
  --end 2025-12-12 \
  --weather-source open-meteo \
  --output /tmp/ha_compare.csv
```

To write a tuned config that applies the suggested scale factor:

```bash
PYTHONPATH=src solarguess ha-compare \
  --config etc/config.yaml \
  --ha pv_data_2025.json \
  --entity sensor.total_pv_energy_today \
  --start 2025-09-15 \
  --end 2025-12-12 \
  --weather-source open-meteo \
  --write-config etc/config.tuned.yaml
```

To auto-train per-array scale factors (uses subsystem sensors when available):

```bash
PYTHONPATH=src solarguess ha-tune \
  --config etc/config.yaml \
  --ha pv_data_2025.json \
  --start 2025-09-15 \
  --end 2025-12-12 \
  --weather-source open-meteo \
  --write-config etc/config.tuned.yaml
```

If you already have a debug JSONL containing `stage=weather.raw` (e.g., from a prior run),
you can train without network by passing:

```bash
PYTHONPATH=src solarguess ha-tune \
  --config etc/config.yaml \
  --ha pv_data_2025.json \
  --start 2025-09-15 \
  --end 2025-12-12 \
  --weather-debug tmp_ha_compare_openmeteo.debug.jsonl \
  --write-config etc/config.tuned.yaml
```

---

## Home Assistant / MQTT integration

`solarguess go` covers the full “simulate + publish” story and is safe for cron jobs because it respects the freshness guard. When you need manual control or want to republish an existing file, run the two commands explicitly:

```bash
PYTHONPATH=src solarguess run \
  --config etc/config.yaml \
  --date 2025-12-10 \
  --timestep 15m \
  --format json \
  --output json/2025-12-10.json

PYTHONPATH=src solarguess publish-mqtt \
  --config etc/config.yaml \
  --input json/2025-12-10.json \
  --verify --publish-retries 3 --retry-delay 2 --skip-if-fresh
```

### Cron-friendly wrapper

`solarguess go` is cron-friendly on its own: it expands `run.output`, honors the skip-if-fresh guard, and publishes based on `mqtt.enable`. The legacy `./guess.sh` shim now just forwards to `go` while keeping the familiar env overrides.

```bash
./guess.sh              # uses today's date
./guess.sh 2025-12-12   # explicit date (YYYY-MM-DD)
```

- If `etc/config.tuned.yaml` exists, the CLI (and wrapper) uses it by default; override via `--config` or `CONFIG=etc/config.yaml ./guess.sh`.
- Publishes to `solarguess/forecast` and `solarguess/availability` with retained messages.
- Discovery payload registers `sensor.solarguess_forecast` whose state is `meta.total_energy_kwh`; attributes include the full payload (`meta` + `sites`) so you can see `meta.date` and `meta.generated_at` in HA.
- Publishing only proceeds when the local `generated_at` is newer *and* the payload changed (prevents timestamp churn).
- `mqtt.verify`, `mqtt.publish_retries`, and `mqtt.skip_if_fresh` now live in the config. Override per invocation with `--verify/--no-verify`, `--publish-retries`, `--skip-if-fresh`, or env vars (`MQTT_VERIFY`, `MQTT_PUBLISH_RETRIES`, `MQTT_SKIP_IF_FRESH`) if you're calling `guess.sh`.
- Safety guard: discovery/state must travel together. If you set `publish_state: false`, also set `publish_discovery: false`; otherwise the CLI will refuse `--publish-topics` because HA would reference an unwritten state topic.
- Recovery procedure for stuck entities:

  ```bash
  mosquitto_pub -h <broker> -p 1883 -u <user> -P '<pass>' -r -n -t solarguess/forecast
  mosquitto_pub -h <broker> -p 1883 -u <user> -P '<pass>' -r -n -t solarguess/availability
  PYTHONPATH=src solarguess publish-mqtt --config etc/config.yaml --verbose --force
  ```

Health/verification:
- `publish-mqtt` supports `--verify` to read back retained state and confirm the payload matches the just-published forecast (hash compare).
- Use `--publish-retries N` to retry publish+verify if the broker is briefly unavailable; `--retry-delay` controls spacing.
- Discovery publish can be disabled with `--no-discovery` or `mqtt.publish_discovery: false`.

### Topic layout & dynamic allocation

- Base topic defaults to `solarguess` (override with `--base-topic foo/bar`); trailing slashes are stripped.
- Two retained JSON blobs when `publish_state` is true:
  - `solarguess/forecast` — full forecast (sites, arrays, meta).
  - `solarguess/availability` — simple online/offline heartbeat.
- Optional scalar fan-out when `--publish-topics` or `mqtt.publish_topics: true` is set:
  - Meta metrics: `solarguess/forecast/meta/<metric>` (e.g., `total_energy_kwh`, `peak_kw`).
  - Per-site metrics: `solarguess/<site_id>/energy_kwh`, `.../peak_kw`, `.../peak_time`.
  - Per-array metrics: `solarguess/<site_id>/<array_id>/energy_kwh`, `.../peak_kw`, `.../poa_wh_m2`.
- Topic set is generated dynamically from the forecast payload at publish time; new sites/arrays appear automatically without config changes.
- Verification (`--verify`) spot-checks a subset of scalar topics to ensure retained values match the freshly published forecast.

Safety rules:

- Discovery + state must travel together. If `publish_state=false`, also set `publish_discovery=false`; otherwise publish-mqtt aborts to avoid dangling HA entities.
- Scalar topics obey the same freshness gate as the state blob; `--force` bypasses it if you need to republish identical payloads.

---

Need help or want to contribute a module? Open an issue or PR; the repo prefers one-task-per-PR with tests + debug coverage per AGENTS.md.
