## Proposed repo shape (Python)

* `src/solarpredict/`

  * `core/` (types, config, debug plumbing)
  * `weather/` (Open‑Meteo client + provider interface)
  * `solar/` (solar position, POA irradiance, temperature)
  * `pv/` (DC + inverter + losses)
  * `engine/` (multi-location/multi-array orchestration + aggregation)
  * `cli.py` (command line entry)
* `tests/` (unit + small integration tests; network calls are acceptable when needed)

**Debug format:** JSONL events like:

```json
{"ts":"2025-12-09T11:00:00+01:00","site":"solingen","array":"roof_south","stage":"poa","data":{"dni":320,"dhi":90,"ghi":220,"poa_global":410}}
```

So you can diff runs, or feed it to another AI and ask “does this math look sane?” 🧪

---

## Codex coding tasks (each = one module, from scratch to tested)

### Task 1: `core` module (models + config + debug)

**Goal:** Define all domain models + a universal debug mechanism.

**Implement**

* `core/models.py`

  * `Location(id, lat, lon, tz="auto", elevation_m|None)`
  * `PVArray(id, tilt_deg, azimuth_deg, pdc0_w, gamma_pdc, dc_ac_ratio, eta_inv_nom, losses_percent, temp_model)`
  * `Site(id, location, arrays[])`
  * `Scenario(sites[])`
  * Validation: tilt 0..90, azimuth -180..180, non-negative powers, etc.
* `core/config.py`

  * Load scenario from YAML/JSON into `Scenario`
* `core/debug.py`

  * `DebugCollector` interface: `emit(stage:str, payload:dict, *, ts, site, array)`
  * `NullDebugCollector`, `ListDebugCollector`, `JsonlDebugWriter(path)`
  * All collectors must be deterministic (sorted keys) for diffability.

**Tests**

* `tests/core/test_models_validation.py`: rejects invalid tilt/azimuth, missing ids, etc.
* `tests/core/test_debug_collectors.py`: JSONL writer produces valid JSON per line; list collector captures events.
* `tests/core/test_compileall.py`: `compileall` on `src/solarpredict` (syntax check).

**Done when**

* `pytest` passes
* minimal example YAML loads into `Scenario` without warnings

---

### Task 2: `weather` module (Open‑Meteo forecast provider)

**Goal:** Fetch weather + solar radiation forecast for **1..N locations** and return normalized time series.

Open‑Meteo supports multi-location by comma-separating coordinates; response becomes a list of structures ([Open Meteo][1]). It also exposes **solar radiation variables** (GHI/DNI/DHI/GTI etc.) and notes they’re backward-averaged for energy use ([Open Meteo][1]).

**Implement**

* `weather/base.py`

  * `WeatherProvider` protocol: `get_forecast(locations, start, end, timestep) -> dict[location_id, pandas.DataFrame]`
* `weather/open_meteo.py`

  * Build request to `/v1/forecast` with:

    * `temperature_2m`, `wind_speed_10m`
    * `shortwave_radiation` (GHI), `diffuse_radiation` (DHI), `direct_normal_irradiance` (DNI) ([Open Meteo][1])
    * `timezone=auto`
  * Supports `timestep="1h"` and optionally `"15m"` (use 15-minutely if requested; doc notes regional availability) ([Open Meteo][1])
  * Normalize columns to: `temp_air_c`, `wind_ms`, `ghi_wm2`, `dhi_wm2`, `dni_wm2`
  * Debug events:

    * `weather.request` (URL + params minus secrets)
    * `weather.response_meta` (model name if present, timezone, etc.)
    * `weather.summary` per location (min/max radiation/temp)

**Tests**

* Use `tests/fixtures/open_meteo_multi_location.json` (saved response) and parse it.
* `tests/weather/test_open_meteo_build_url.py`: correct query params for N locations.
* `tests/weather/test_open_meteo_parse.py`: index tz-aware, expected columns present, no NaNs explosion.

**Done when**

* Network access in tests is acceptable; prefer fixtures when they keep tests deterministic.
* Works for 2+ locations in one call.

---

### Task 3: `solar.position` module (sun position)

**Goal:** Solar zenith/azimuth per timestamp and location.

Use pvlib’s `Location.get_solarposition()` (wraps `pvlib.solarposition.get_solarposition`) ([pvlib-python.readthedocs.io][2]).

**Implement**

* `solar/position.py`

  * `solar_position(location: Location, times: DatetimeIndex, debug: DebugCollector|None) -> DataFrame`
  * Output columns at least: `zenith`, `azimuth`, `elevation` (whatever pvlib returns, standardized)
  * Debug:

    * `solar_position.summary` (min/max elevation, any NaNs)

**Tests**

* `tests/solar/test_position_basic_invariants.py`

  * Nighttime yields negative elevation for a known location/date.
  * Values are within sensible bounds (zenith 0..180).
* `tests/solar/test_position_timezone.py`

  * Requires tz-aware times (pvlib assumes UTC if not localized) ([pvlib-python.readthedocs.io][2]).

---

### Task 4: `solar.irradiance` module (GHI/DNI/DHI → POA)

**Goal:** Compute plane-of-array irradiance for arbitrary tilt/azimuth.

Use `pvlib.irradiance.get_total_irradiance()` which returns total in-plane irradiance and components ([pvlib-python.readthedocs.io][3]).

**Implement**

* `solar/irradiance.py`

  * `poa_irradiance(surface_tilt, surface_azimuth, dni, ghi, dhi, solar_zenith, solar_azimuth, albedo=0.2, model="perez", debug=None) -> DataFrame`
  * Return columns: `poa_global`, `poa_direct`, `poa_diffuse`, `poa_ground_diffuse`
  * Debug:

    * `poa.summary` per array (daily sum Wh/m², max POA)

**Tests**

* `tests/solar/test_poa_horizontal_matches_ghi.py`: if `tilt=0`, `poa_global ≈ ghi` (within tolerance).
* `tests/solar/test_poa_nonnegative.py`: no negative POA (clip small negatives to 0 with a documented epsilon rule).

---

### Task 5: `solar.temperature` module (POA + meteo → cell temperature)

**Goal:** Compute cell temperature, defaulting to SAPM.

`pvlib.temperature.sapm_cell()` takes POA irradiance, air temp, wind at 10m and model params ([pvlib-python.readthedocs.io][4]).

**Implement**

* `solar/temperature.py`

  * `cell_temperature(poa_global, temp_air_c, wind_ms, model="sapm", mounting="close_mount_glass_polymer", debug=None) -> Series`
  * Pull parameters from pvlib’s `TEMPERATURE_MODEL_PARAMETERS` (store chosen set in debug)
  * Debug:

    * `temp_model.params`
    * `temp_cell.summary` (min/max)

**Tests**

* Reproduce pvlib’s documented example: `sapm_cell(1000, 10, 0, params) ≈ 44.117` ([pvlib-python.readthedocs.io][4]).
* `tests/solar/test_temp_monotonic.py`: higher POA should not reduce cell temp all else equal.

---

### Task 6: `pv.power` module (PVWatts DC + inverter AC + losses)

**Goal:** Turn effective irradiance + cell temp into DC, then AC.

PVWatts DC model formula is explicitly documented in pvlib ([pvlib-python.readthedocs.io][5]), and inverter model too ([pvlib-python.readthedocs.io][6]).

**Implement**

* `pv/power.py`

  * `pvwatts_dc(effective_irradiance, temp_cell, pdc0_w, gamma_pdc) -> Series` (use pvlib function, but wrap + normalize)
  * `pvwatts_ac(pdc_w, pdc0_inv_w, eta_inv_nom=0.96) -> Series` (pvlib inverter)
  * `apply_losses(p_ac_w, losses_percent) -> Series`
  * Helper: compute `pdc0_inv_w` from `pdc0_w`, `dc_ac_ratio`, `eta_inv_nom` (document assumptions)
  * Debug:

    * `pv.dc.summary`, `pv.ac.summary`, `pv.losses.summary`

**Tests**

* `tests/pv/test_pvwatts_dc_matches_formula.py`: check against the documented PVWatts DC equation ([pvlib-python.readthedocs.io][5]).
* `tests/pv/test_inverter_clipping.py`: AC never exceeds nameplate implied by pvlib’s model (using their definition) ([pvlib-python.readthedocs.io][6]).

---

### Task 7: `engine` module (multi-location, multi-array simulation + daily aggregation)

**Goal:** Glue everything together and output daily kWh.

**Implement**

* `engine/simulate.py`

  * `simulate_day(scenario: Scenario, date, timestep="1h", weather_provider=OpenMeteoProvider(), debug=None) -> SimulationResult`
  * Handles **1..N sites** and **1..N arrays per site**
  * Output:

    * `results.daily` DataFrame: `site,array,date,energy_kwh,peak_kw,poa_kwh_m2,temp_cell_max`
    * optionally `results.timeseries` dict keyed by (site,array)
  * Uses Open‑Meteo weather + solar radiation variables (averaged over past hour) for energy integration ([Open Meteo][1]).
  * Debug:

    * emits stage events in order: `weather` → `solarpos` → `poa` → `temp` → `dc` → `ac` → `aggregate`

**Tests**

* `tests/engine/test_two_sites_two_arrays.py`: synthetic weather (no HTTP) with known constant irradiance window; energy matches analytic integral.
* `tests/engine/test_debug_event_order.py`: ensures events emitted per stage.

---

### Task 8: `cli` module (split into 3 sub-tasks)

**Goal:** Human-usable entrypoint.

**Task 8.1: CLI scaffold**

* add `cli.py` with Typer/argparse wiring, version/--help, and placeholder commands
* baseline test: CLI imports and `--help` exits 0

**Task 8.2: `run` command**

* implement `run --config scenario.yaml --date YYYY-MM-DD --timestep 1h --debug out.jsonl --format json|csv`
* prints summary + writes output file
* debug optional: if set, write JSONL via `JsonlDebugWriter`
* smoke test: fixture config + fixture weather provider (dependency injected), exits 0, produces output files

**Task 8.3: `config` command (interactive setup)**

* interactive add/edit/delete of scenario parts (sites, arrays, location fields) and save to YAML/JSON
* validation reuse from `core.config`
* tests to cover add/edit/delete flows without actual tty (e.g., simulate input)
