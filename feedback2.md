Alright, I dug through `solarguess.txt` and I’m gonna be blunt: this is **way** more “real PV modeling” than most homebrew solar forecasters. You’re using the right building blocks (pvlib + sensible bookkeeping), and the overall pipeline is coherent.

**My rating (as-is): 8.4 / 10** ✅
**What keeps it from 9+ right now:** one *actual* integration bug (IAM never really applies), and a couple “good intention, blunt instrument” heuristics (cloud-scaled scaling and PVGIS QC clamping) that can quietly bias results.

Below is the deep dive, step-by-step, with what’s solid, what’s sketchy, and what I’d improve first. 😄🔧

---

## What your calculation pipeline is doing (and why it’s mostly solid)

### 1) Timestep + timestamp semantics (this is a big deal, and you nailed it)

You do two things that many PV calculators mess up:

**A) You infer timestep robustly** using the median of index deltas and fall back to the declared timestep if inference fails. 
That’s exactly what you want for DST weirdness and imperfect data.

**B) You correct time-label bias** by shifting to interval midpoints depending on whether timestamps represent interval start/end/center. 
Then you compute solar position at those midpoint timestamps and re-align the index so joins still work. 

That’s… chef’s kiss. 🤌
Hourly irradiance is usually an **average over an hour**, and computing solar geometry at the endpoint *does* bias results.

**Energy integration also respects labeling** (interval widths depend on start/end semantics). 
And daily energy is integrated correctly as `pac_net_w * interval_h / 1000`. 

✅ **Verdict:** this whole timestamp/interval layer is *excellent* and will save you from silent +5–10% errors.

---

### 2) DNI/DHI handling (good, but there’s a gotcha)

You use an Erbs decomposition if either DNI or DHI is missing. 
That’s a reasonable default, and your tests even check closure on a sample. 

**The gotcha:** your logic is basically “if one is missing, recompute both from GHI.” 
That means if a provider gives you *good* DNI but not DHI (or vice versa), you throw away the good one.

**Suggested improvement (small, high ROI):**

* If **DNI exists but DHI missing**: compute DHI from closure: `DHI = GHI - DNI*cos(zenith)` (clipped ≥0).
* If **DHI exists but DNI missing**: compute DNI similarly.
* Only do Erbs when **both** are missing (or both are garbage).

Also: treat “present but all NaNs” like missing. Right now, a Series full of NaNs still counts as “have_dni/have_dhi”.

✅ **Verdict:** good approach, but you can make it smarter with a tiny change.

---

### 3) POA irradiance: Perez + sane clipping + horizon mask (strong)

You’re doing Perez transposition and clipping negative irradiance to zero for deterministic behavior. 
The docs match that intent too. 

**Horizon masking** is also implemented in a physically sane way:

* You interpolate a horizon profile by solar azimuth 
* You blank *direct* when the sun is below the horizon obstacle, but keep diffuse/ground 
* You validate horizon inputs are ≥12 bins, values 0–90° 

✅ **Verdict:** this part is very solid.

---

### 4) Morning/evening damping (good as a pragmatic knob)

Your `_damping_factor()` applies a cosine taper around sunrise and sunset over a fixed window. 
And you apply it multiplicatively to POA when computing effective irradiance. 

This is a good “real life is messy” knob for:

* row-to-row shading,
* curtailment ramps,
* local obstruction weirdness you don’t want to model geometrically.

✅ **Verdict:** good pragmatic feature.

---

## The biggest real problem: IAM is basically not active right now 😬

You *did* implement IAM correctly in isolation:

* only derate direct beam,
* recompute `poa_global = direct + diffuse + ground`,
* use pvlib’s ASHRAE IAM. 

…but in `poa_irradiance()` you set:

```python
aoi = poa.get("aoi") if isinstance(poa, dict) else None
```

and because `poa` is a DataFrame, that makes `aoi = None`. 

Then `apply_iam()` sees `aoi is None` and **skips**. 

### Fix (high priority)

Compute AOI explicitly and pass it:

* Use pvlib’s AOI calculation (it’s standard), and keep it aligned to your index.
* Then IAM will actually do something.

Also add one integration test: “same scenario + same weather + IAM enabled => energy drops a bit.” Right now you only test `apply_iam()` in isolation. 

**Impact:** depending on array tilt + low sun angles (winter, mornings/evenings), this can be a non-trivial % difference. So yeah, this is worth fixing first.

---

## Power modeling: PVWatts DC + inverter + grouping (really good)

### DC

PVWatts DC is clean and you’re wrapping it in a consistent way. 
Your deep-dive doc reflects that too. 

### Cell temperature

You’re using SAPM temperature with mounting configs pulled from pvlib tables. 

This is a solid middle-ground model.

### Inverter & clipping

You:

* group arrays by `inverter_group_id`,
* sum DC,
* run a single inverter model,
* allocate AC back to arrays by instantaneous DC share. 

That’s the correct “don’t create energy / don’t double-clip” behavior.

Your inverter sizing helper matches PVWatts’ dc/ac framing. 

✅ **Verdict:** this is one of the strongest parts of the whole project.

### A design improvement I’d still make

Right now, group-level inverter params are implicitly derived from per-array params (max dc_ac_ratio, max eta). 
That works, but it’s kinda conceptually backwards: an inverter is a shared object.

**Suggestion:** define an explicit “inverter group” config with:

* pac0 (or pdc0_inv),
* eta curve assumptions,
* maybe MPPT count / limits if you ever get fancy.

It’ll make the mental model cleaner and reduce config footguns.

---

## Losses: fine, but be clear what you’re counting

You apply lumped losses percent to AC output. 

That’s fine **as long as** you treat it as “everything except inverter conversion efficiency,” because your inverter model already covers conversion behavior. (In practice people often double-count here.)

✅ **Verdict:** acceptable, but I’d document “losses_percent excludes inverter conversion” super explicitly.

---

## Cloud-scaled weather mode: clever, but physically biased (fixable)

Your cloud-scaled mode:

* converts cloudcover% → clearness-ish factor `k_t = 1 - 0.75*C**3.4`,
* clamps 0..1,
* multiplies clear-sky GHI/DNI/DHI by the same factor. 

This is **simple and stable**, but here’s the issue:

### Why this is biased

Clouds don’t “turn down” DNI and DHI equally.

* DNI usually collapses fast with clouds.
* DHI often *increases* (more scattering), at least up to a point.

Scaling both by the same factor tends to:

* **overestimate direct beam** under partial cloud,
* **underestimate diffuse**,
* which then biases POA for tilted arrays (and anything IAM-related too, once IAM works).

### What I’d do instead (still simple)

Use cloud cover to scale **GHI only**, then derive DNI/DHI using a decomposition (Erbs, DISC, etc.) based on solar zenith. That gives you the “clouds → more diffuse fraction” behavior for free.

✅ **Verdict:** good idea, but right now it’s “good-looking wrong” in certain conditions. This is #2 on my fix list after IAM.

---

## PVGIS QC clamping: useful training wheels, but dangerous if you trust it too much

You compute a ratio vs PVGIS baseline and clamp when outside thresholds, then you rescale the entire timeseries. 

This is *great* for catching:

* insane API irradiance spikes,
* broken config (wrong tilt/azimuth),
* unit mistakes.

But it can also:

* suppress legit “perfect bluebird day” highs,
* suppress legit “apocalypse cloud soup” lows,
* and mask problems you’d rather surface and fix.

**My strong opinion:**
👉 Keep the PVGIS comparison, but make “hard clamping” **off by default**, and treat it as a **warning / anomaly flag** unless the forecast violates a physical ceiling (e.g., exceeds clear-sky by a big margin).

✅ **Verdict:** QC is smart; clamping is a blunt hammer.

---

## Bonus: your “actual energy so far today” correction is genuinely smart

Your `apply_actual_adjustment()`:

* compares predicted cumulative to “now” vs actual_kwh_today,
* scales **future** intervals only,
* and explicitly avoids time-label misclassification by using a half-step bias correction for end-labeled series. fileciteturn18file10L9-L16

That is *exactly* the kind of pragmatic correction that makes automation feel “sticky accurate” by noon. 😄

---

## My prioritized improvement list (do these in order)

1. **Fix IAM actually applying** (compute AOI and pass it). fileciteturn4file2L41-L44
   Add one integration test for it.

2. **Fix cloud-scaled mode**: scale GHI then decompose to DNI/DHI (don’t scale all three equally). 

3. **Improve fill_dni_dhi**: don’t throw away a good component when only one is missing; treat all-NaN as missing. 

4. **Re-think PVGIS QC clamping**: warning-first, clamp only on physically impossible values. 

5. **Make inverter group a first-class config object** (less “max() of array params”). 

6. (Forward-thinking fun) **Produce an uncertainty band / ensemble**: run open-meteo + cloud-scaled + PVGIS baseline and publish P10/P50/P90. That’s gold for “should I run the dishwasher + charge the EV at 11:00?” automation.

---

## Final score breakdown 🧮

* **Math/physics chain:** 8/10 (Perez + SAPM + PVWatts is solid; IAM bug and cloud scaling hold it back)
* **Time semantics + integration correctness:** 9.5/10 (seriously good) fileciteturn18file0L13-L31
* **Robustness / defensive coding:** 9/10 (validation + schema checks + debug staging is strong) fileciteturn18file11L16-L30
* **Heuristic risk (QC clamp, cloud scaling):** 6.5/10 (helpful, but can bias silently) fileciteturn16file11L7-L55

**Overall:** **8.4/10** ✅
Fix IAM + cloud-scaled DNI/DHI handling and you’re basically at **9.2+**.

---

One non-math thing I can’t not mention: your config file contains **plain-text MQTT creds**. Rotate them + move to a secrets/env mechanism before that bites you. (Ask me how many times I’ve seen this turn into “why is my broker mining crypto” 🫠)

If you want, I can also call out any *parameter* choices (losses%, dc/ac ratio, temp model selection) that look inconsistent with real-world behavior — but the big wins are the four items above.

