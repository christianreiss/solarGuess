Your current calculation chain is structurally sound (weather → sun position → POA irradiance → cell temp → PVWatts DC → PVWatts inverter AC → losses → integrate), and you’ve got the important unit/timezone guardrails in the right places. The remaining “gotchas” are mostly about time alignment, integration, and shared-inverter realism.

## What the engine is doing (and it’s the right order)

For each site, it:

1. Pulls weather time series (GHI/DHI/DNI, air temp, wind) with timezone-aware timestamps (Open-Meteo `timezone=auto` parsing is handled explicitly). 
2. Infers timestep seconds from the index (median delta) as a fallback when the provider doesn’t guarantee a step. 
3. Computes solar position via pvlib, and enforces tz-aware inputs. 
4. Computes POA irradiance using Perez (dni_extra included, negatives clipped). 
5. Computes cell temperature with SAPM, which *depends on wind speed* (in m/s). 
6. Computes DC power with PVWatts DC, then AC with PVWatts inverter, then applies a lumped loss factor. 
7. Integrates energy via `sum(P_ac_net * step_hours)`. 

So the physics pipeline is reasonable and “audit-friendly”.

## The biggest logic risk: your midpoint time shift direction

You currently do:

```python
solar_times = times + step/2
```

with the comment that Open-Meteo irradiance is “backward-averaged”. 

If the data point at `t` represents the average over `(t-step, t]` (backward averaged), the midpoint is **t - step/2**, not plus. Getting this wrong can noticeably skew POA for tilted arrays in winter (low sun angles amplify geometry errors).

Practical fix: make this explicit and configurable:

* `label="start"` → use `times + step/2`
* `label="end"` → use `times - step/2`

Bonus: add a tiny heuristic test using sunrise (pvlib) vs first nonzero GHI to infer “start vs end” for a given provider.

## Integration: constant step is ok… until DST or missing samples

You infer a single `step_seconds` (median delta) and multiply every sample by that.  

This is fine for clean hourly/15-min series, but on DST days (23h/25h) or if the API has gaps, you’ll under/over-count energy. The robust approach is per-interval integration:

* compute `dt_hours[i] = (t[i+1]-t[i]) / 3600`
* energy = `sum(p[i] * dt_hours[i])`

It’s a small change and makes energy accounting bulletproof.

## Wind units: yes, it matters (and you handled it correctly)

Wind is only there to estimate **cell temperature** via SAPM (`wind_speed=wind_ms`). 
Feeding km/h instead of m/s “overcools” the panel by 3.6x wind, which increases predicted power (sometimes by a lot on sunny/cold/windy days). You even request m/s from Open-Meteo and still convert if it comes back as km/h.  

So: wind units are not about wind generation, they’re about *temperature derating*.

## Inverter modeling: per-array is ok, but shared inverters need grouping

Right now you compute AC per-array, either from an explicit inverter size or derived from dc/ac ratio.  

This is fine if each array has its own inverter. If multiple arrays share a single inverter, the correct clipping behavior depends on **sum of DC across arrays**. Splitting into independent inverters will overpredict on high-irradiance days (because each “virtual inverter” clips independently instead of one shared clip ceiling).

If you want realism later: add `inverter_group_id` and do:

* sum DC for arrays in group
* apply one PVWatts inverter
* allocate clipped AC back to arrays proportionally (by DC power)

## Debug/auditability

You’re in a good spot:

* Each stage emits summaries and stage markers, and the engine scopes debug by site/array. 
  Only nit: make sure “site” identifiers are consistent everywhere (site.id vs location.id), otherwise your debug JSON becomes a Where’s-Waldo of IDs.

## Why your forecast can be lower than “already produced”

If your live screenshot shows more kWh than the model’s full-day total, it’s usually one of:

* forecast irradiance was too pessimistic (most common)
* time-label semantics mismatch (start vs end) causing POA underestimation on tilted planes (your midpoint sign is a prime suspect)
* energy integration undercount (DST/gaps)

If you want, I can point to the exact next 2-3 PR-sized tasks to fix the midpoint semantics + per-interval integration + optional inverter grouping.

