# Implementation Plan

Format: `X.Y.Z` where **X** is one self-contained module (doable in a single task), **Y** the required sub-feature, **Z** the detail to implement/test.

## Checklist

- [x] 1. Horizon Masking  
  - [x] 1.1. Config & parsing  
    - [x] 1.1.1. Accept per-array `horizon_deg` list (CSV/string or list) with validation (≥12 values, 0–90°).  
    - [x] 1.1.2. Add docs and CLI schema wiring; emit parse errors via `ConfigError`.  
  - [x] 1.2. Geometry application  
    - [x] 1.2.1. Compute azimuth-binned horizon; interpolate to sun azimuth each timestep.  
    - [x] 1.2.2. Zero direct-beam component in POA when solar elevation < horizon; keep diffuse/ground intact.  
    - [x] 1.2.3. Debug events: count masked samples, example max blocked DNI.  
  - [x] 1.3. Tests  
    - [x] 1.3.1. Unit test horizon interp/masking with synthetic sun path.  
    - [x] 1.3.2. Engine integration test: horizon cuts morning/evening energy; debug includes mask stats.  

- [x] 2. Damping (morning/evening attenuation)  
  - [x] 2.1. Config  
    - [x] 2.1.1. Per-array `damping_morning`/`damping_evening` (0–1) with optional single `damping` tuple.  
    - [x] 2.1.2. Validation + docs; defaults to 1.0 (no effect).  
  - [x] 2.2. Application  
    - [x] 2.2.1. Apply smooth (e.g., cosine taper) attenuation around civil sunrise/sunset or configurable window.  
    - [x] 2.2.2. Hook into engine after POA or at pac_net; ensure multiplicative and deterministic.  
    - [x] 2.2.3. Debug: emit attenuation window and min/max applied factor.  
  - [x] 2.3. Tests  
    - [x] 2.3.1. Deterministic unit test on synthetic day: verify AM/PM watts reduced by expected factor; midday untouched.  

- [x] 3. Interval Export  
  - [x] 3.1. CLI/format  
    - [x] 3.1.1. Add `--intervals` flag (json or csv) to write per-interval watts/Wh/Wh_cum plus existing daily.  
    - [x] 3.1.2. Preserve current defaults; ensure backward compatibility on `--format`.  
  - [x] 3.2. Serialization  
    - [x] 3.2.1. JSON: records with site/array timestamps, pac_net_w, poa_global, interval_h, wh_period, wh_cum.  
    - [x] 3.2.2. CSV: tidy long-format with columns site,array,ts,metric,value.  
    - [x] 3.2.3. Debug: note path and row count.  
  - [x] 3.3. Tests  
    - [x] 3.3.1. CLI test generates intervals file; validate columns and first/last cumulative values.  
    - [x] 3.3.2. Compileall/pytest coverage updates.  

- [x] 4. Actual Production Adjustment  
  - [x] 4.1. Config/CLI  
    - [x] 4.1.1. Add `actual_kwh_today` (float) and optional `limit=0`-style suppress flag.  
    - [x] 4.1.2. Validation and docs.  
  - [x] 4.2. Algorithm  
    - [x] 4.2.1. Compare predicted cumulative (up to current time) vs provided actual; scale remaining intervals proportionally.  
    - [x] 4.2.2. Idempotent reset when actual=0; clamp to non-negative; avoid double-application.  
    - [x] 4.2.3. Debug: emit before/after energy and scale factor.  
  - [x] 4.3. Tests  
    - [x] 4.3.1. Synthetic timeseries: ensure scaling only future intervals; zero resets forecast.  

 - [x] 5. Load Window Finder  
   - [x] 5.1. Inputs  
     - [x] 5.1.1. Config/CLI accepts `base_load_w`, `min_duration_min`, optional `required_wh`.  
     - [x] 5.1.2. Validate positive durations/loads.  
   - [x] 5.2. Computation  
     - [x] 5.2.1. From `pac_net_w` & `interval_h`, find intervals exceeding base load; compute earliest/best/latest windows, optional energy satisfaction.  
     - [x] 5.2.2. Return in output payload; debug window stats.  
   - [x] 5.3. Tests  
     - [x] 5.3.1. Unit tests on synthetic profiles (single window, multiple windows, none).  
     - [x] 5.3.2. CLI integration to ensure serialization.

6. Cloud-Cover Scaling Path
6.1. Weather ingest  
6.1.1. Extend Open-Meteo fetch to pull cloudcover (%).  
6.1.2. Add new `weather_mode=cloud-scaled` to engine.  
6.2. Clear-sky base  
6.2.1. New `solar/clear_sky.py` using pvlib clearsky (Ineichen) with Linke/altitude; reuse horizon mask when present.  
6.3. Clearness mapping  
6.3.1. Implement pluggable cloud→clearness (default `1 - 0.75*C^3.4`), clamp to [0,1].  
6.3.2. Compute GHI/DNI/DHI from clear-sky × clearness; feed rest of pipeline.  
6.3.3. Debug: emit mean clearness, min/max scaled irradiance.
6.4. Tests  
6.4.1. Unit tests for mapping edge cases (0/50/100% cloud).  
6.4.2. Engine test comparing clear-sky vs cloud-scaled energy reduction.  
6.4.3. Compileall/pytest updates.
