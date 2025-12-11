"""End-to-end daily simulation engine."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector, ScopedDebugCollector
from solarpredict.core.models import Scenario
from solarpredict.pv.power import apply_losses, inverter_pdc0_from_dc_ac_ratio, pvwatts_ac, pvwatts_dc
from solarpredict.solar.irradiance import poa_irradiance
from solarpredict.solar.position import solar_position
from solarpredict.solar.temperature import cell_temperature
from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider
from solarpredict.weather.cloud_scaled import CloudScaledWeatherProvider


def _clip_to_pvgis(daily_rows):
    """Clamp daily POA/energy using PVGIS climatology guidance.

    - If POA ratio vs PVGIS is outside [0.6, 1.6], scale POA and energy to the nearest bound.
    - Applies per array, preserves peak_kw proportionally.
    """

    for row in daily_rows:
        pvgis = row.get("pvgis_poa_kwh_m2")
        poa = row.get("poa_kwh_m2")
        if pvgis is None or poa is None:
            continue
        if pvgis <= 0:
            continue

        ratio = poa / pvgis if pvgis else None
        if ratio is None:
            continue
        if 0.6 <= ratio <= 1.6:
            continue

        # clamp
        capped_ratio = 1.6 if ratio > 1.6 else 0.6
        scale = capped_ratio / ratio if ratio != 0 else 1.0

        row["poa_kwh_m2"] = poa * scale
        row["energy_kwh"] = row["energy_kwh"] * scale
        row["peak_kw"] = row["peak_kw"] * scale

        row["qc_clipped"] = True
        row["qc_ratio"] = ratio

    return daily_rows


@dataclass(frozen=True)
class SimulationResult:
    daily: pd.DataFrame
    timeseries: Dict[Tuple[str, str], pd.DataFrame]
    weather_raw: Dict[str, pd.DataFrame] | None = None
    meta: Dict[str, any] | None = None


def _now_like(index: pd.DatetimeIndex) -> pd.Timestamp:
    """Return a timezone-aware 'now' matching the provided index tz.

    Tests can monkeypatch this function to control the reference time without
    touching datetime globally.
    """

    tz = index.tz if hasattr(index, "tz") else None
    return pd.Timestamp.now(tz=tz)


def apply_actual_adjustment(
    result: SimulationResult,
    actual_kwh_today: float,
    debug: DebugCollector,
    now_ts: pd.Timestamp | None = None,
) -> SimulationResult:
    """Scale only future intervals so cumulative matches provided actual.

    - Compares predicted cumulative energy up to ``now_ts`` against
      ``actual_kwh_today``.
    - Multiplies future PAC/DC by a constant factor to make the remaining
      forecast align with the delta.
    - Leaves POA/temperature untouched (these are weather-driven inputs).
    - Returns a new SimulationResult; original inputs are not mutated.
    """

    try:
        actual = float(actual_kwh_today)
    except Exception as exc:
        raise ValueError("actual_kwh_today must be numeric") from exc

    if actual < 0:
        raise ValueError("actual_kwh_today must be non-negative")

    if actual == 0:
        debug.emit("actual.adjust.skip", {"reason": "reset", "actual_kwh_today": actual}, ts=now_ts)
        return result

    if not isinstance(result, SimulationResult):
        # Defensive: allow duck-typed in tests
        result = SimulationResult(daily=getattr(result, "daily", pd.DataFrame()), timeseries=getattr(result, "timeseries", {}))

    if not result.timeseries:
        debug.emit("actual.adjust.skip", {"reason": "empty_timeseries"}, ts=now_ts)
        return result

    # Pick a representative index to derive timezone and window.
    sample_df = next(iter(result.timeseries.values()))
    if sample_df is None or sample_df.empty:
        debug.emit("actual.adjust.skip", {"reason": "empty_timeseries"}, ts=now_ts)
        return result

    ts_index = sample_df.index
    now_ts = now_ts or _now_like(ts_index)
    # Clamp to simulation window to keep backfills/future runs deterministic.
    if now_ts < ts_index.min():
        now_ts = ts_index.min()
    if now_ts > ts_index.max():
        now_ts = ts_index.max()

    # Compute cumulative energy up to now and remaining energy.
    cumulative_kwh = 0.0
    future_energy_kwh = 0.0
    future_sample_count = 0
    date_val = None

    for df in result.timeseries.values():
        if df is None or df.empty:
            continue
        df_sorted = df.sort_index()
        energy_period = (df_sorted["pac_net_w"] * df_sorted["interval_h"]).astype(float) / 1000.0
        past_mask = df_sorted.index <= now_ts
        future_mask = df_sorted.index > now_ts
        cumulative_kwh += float(energy_period[past_mask].sum())
        future_energy_kwh += float(energy_period[future_mask].sum())
        future_sample_count += int(future_mask.sum())
        if date_val is None:
            date_val = df_sorted.index[0].date().isoformat()

    if cumulative_kwh <= 0:
        debug.emit(
            "actual.adjust.skip",
            {"reason": "zero_predicted", "actual_kwh_today": actual, "now": str(now_ts)},
            ts=now_ts,
        )
        return result

    if future_energy_kwh == 0:
        debug.emit(
            "actual.adjust.skip",
            {"reason": "no_future_samples", "actual_kwh_today": actual, "cumulative_kwh": cumulative_kwh},
            ts=now_ts,
        )
        return result

    # Bias-correct remaining intervals by ratio of observed vs predicted so far.
    scale = max(0.0, actual / cumulative_kwh)
    adjusted_timeseries: Dict[Tuple[str, str], pd.DataFrame] = {}

    for key, df in result.timeseries.items():
        if df is None or df.empty:
            adjusted_timeseries[key] = df
            continue
        df_sorted = df.sort_index()
        future_mask = df_sorted.index > now_ts
        df_scaled = df_sorted.copy()
        for col in ("pdc_w", "pac_w", "pac_net_w"):
            if col in df_scaled.columns:
                df_scaled.loc[future_mask, col] = df_scaled.loc[future_mask, col] * scale
        adjusted_timeseries[key] = df_scaled

    # Recompute daily aggregates from adjusted timeseries.
    daily_rows = []
    for key, df in adjusted_timeseries.items():
        if df is None or df.empty:
            continue
        site_id, array_id = key
        energy_kwh = float(((df["pac_net_w"] * df["interval_h"]) / 1000.0).sum())
        peak_kw = float(df["pac_net_w"].max() / 1000.0)
        # Retain POA and temp maxima from existing result; fallback to computed if missing.
        poa_kwh_m2 = None
        temp_cell_max = None
        if key in result.timeseries:
            original_df = result.timeseries[key]
            poa_kwh_m2 = float(((original_df["poa_global"] * original_df["interval_h"]) / 1000.0).sum())
            temp_cell_max = float(original_df["temp_cell_c"].max())

        daily_rows.append(
            {
                "site": site_id,
                "array": array_id,
                "date": result.daily.iloc[0]["date"],
                "energy_kwh": energy_kwh,
                "peak_kw": peak_kw,
                "poa_kwh_m2": poa_kwh_m2,
                "temp_cell_max": temp_cell_max,
            }
        )

    adjusted_daily = pd.DataFrame(daily_rows)
    if date_val and "date" in result.daily.columns:
        adjusted_daily["date"] = date_val

    debug.emit(
        "actual.adjust.applied",
        {
            "actual_kwh_today": actual,
            "predicted_to_now_kwh": cumulative_kwh,
            "future_energy_kwh": future_energy_kwh,
            "scale_future": scale,
            "now": str(now_ts),
            "future_samples": future_sample_count,
        },
        ts=now_ts,
    )

    return SimulationResult(daily=adjusted_daily, timeseries=adjusted_timeseries)


def _daterange_bounds(date: dt.date) -> tuple[str, str]:
    return date.isoformat(), (date + dt.timedelta(days=1)).isoformat()


def _infer_step_seconds(index: pd.DatetimeIndex, declared_timestep: str) -> float:
    """Best-effort timestep inference that stays sane on DST gaps/duplication and sparse data."""
    if len(index) > 1:
        deltas = index.to_series().diff().dt.total_seconds().dropna()
        median = float(deltas.median()) if not deltas.empty else float("nan")
        if median > 0:
            return median

    # Fallback to declared timestep (e.g., "1h", "15m") if median is NaN/0 or only one sample.
    try:
        td = pd.to_timedelta(declared_timestep)
        if pd.notna(td) and td.total_seconds() > 0:
            return float(td.total_seconds())
    except Exception:
        pass
    return 0.0


def _emit_df(debug: DebugCollector, stage: str, df: pd.DataFrame, *, ts, site=None, array=None) -> None:
    """Emit entire dataframe as records for auditability."""
    try:
        payload = {
            "rows": len(df),
            "data": df.reset_index().rename(columns={"index": "ts"}).to_dict(orient="records"),
        }
        debug.emit(stage, payload, ts=ts, site=site, array=array)
    except Exception as exc:  # pragma: no cover - best effort
        debug.emit(f"{stage}.error", {"error": str(exc)}, ts=ts, site=site, array=array)


def _emit_series(debug: DebugCollector, stage: str, series: pd.Series, *, ts, site=None, array=None) -> None:
    """Emit series with index for full traceability."""
    try:
        payload = {
            "rows": len(series),
            "data": series.reset_index().rename(columns={"index": "ts", series.name or "value": "value"}).to_dict(orient="records"),
        }
        debug.emit(stage, payload, ts=ts, site=site, array=array)
    except Exception as exc:  # pragma: no cover
        debug.emit(f"{stage}.error", {"error": str(exc)}, ts=ts, site=site, array=array)


def _apply_time_label(times: pd.DatetimeIndex, step_seconds: float, label: str) -> pd.DatetimeIndex:
    """Shift timestamps to the interval midpoint based on label semantics.

    - label=="end": samples represent (t-step, t]; midpoint at t - step/2
    - label=="start": samples represent [t, t+step); midpoint at t + step/2
    - label=="center": already centered; no shift
    """
    if step_seconds <= 0:
        return times

    label = (label or "end").lower()
    delta = pd.to_timedelta(step_seconds / 2.0, unit="s")
    if label == "end":
        return times - delta
    if label == "start":
        return times + delta
    if label == "center":
        return times
    raise ValueError(f"Unsupported time label: {label}")


def _interval_hours(index: pd.DatetimeIndex, step_seconds: float, label: str) -> pd.Series:
    """Interval widths (hours) consistent with timestamp labeling.

    - label=="start": sample marks interval start → use forward delta (t[i+1]-t[i]).
    - label=="end": sample marks interval end   → use backward delta (t[i]-t[i-1]).
    - label=="center": treat like start; symmetric choice is acceptable.

    First/last interval gets the declared step when provided, otherwise the nearest non-null
    delta so gappy series still integrate reasonably.
    """

    if len(index) == 0:
        return pd.Series([], index=index, dtype=float)

    label = (label or "end").lower()
    deltas_fwd = index.to_series().diff().shift(-1).dt.total_seconds()

    if len(index) == 1:
        fill = step_seconds if step_seconds > 0 else float("nan")
        hours = pd.Series([fill / 3600.0], index=index, dtype=float)
        if hours.isna().any():
            raise ValueError("Unable to infer interval hours from single sample; provide timestep")
        return hours

    widths = deltas_fwd.copy()
    fallback = widths.dropna()
    filler = step_seconds if step_seconds > 0 else (fallback.iloc[-1] if len(fallback) else float("nan"))
    widths.iloc[-1] = filler

    if label not in {"start", "end", "center"}:
        raise ValueError(f"Unsupported time label: {label}")

    if label == "end":
        widths = widths.shift(1)
        first_fallback = step_seconds if step_seconds > 0 else (fallback.iloc[0] if len(fallback) else float("nan"))
        widths.iloc[0] = first_fallback
        if len(widths) > 1 and pd.isna(widths.iloc[-1]):
            widths.iloc[-1] = widths.dropna().iloc[-1]
    # center: keep forward widths

    hours = widths / 3600.0
    if hours.isna().any():
        raise ValueError(
            "Unable to infer interval hours; provide a valid timestep or weather_label-compatible timestamps"
        )
    return hours


def _damping_factor(
    times: pd.DatetimeIndex,
    solar_elevation: pd.Series,
    solar_noon_ts,
    damping_morning: float,
    damping_evening: float,
    window_hours: float = 1.5,
) -> pd.Series:
    """Smooth attenuation factor around sunrise/sunset using cosine taper over a fixed window."""
    if solar_noon_ts is None or solar_elevation.empty or len(times) == 0:
        return pd.Series([1.0] * len(times), index=times, dtype=float)

    daylight = solar_elevation[solar_elevation > 0]
    if daylight.empty:
        return pd.Series([1.0] * len(times), index=times, dtype=float)

    sunrise = daylight.index[0]
    sunset = daylight.index[-1]
    window = pd.to_timedelta(window_hours, unit="h")

    factors = pd.Series(1.0, index=times, dtype=float)

    # Morning ramp-up: start at damping_morning at sunrise, reach 1.0 after window.
    morning_mask = (times >= sunrise) & (times <= sunrise + window)
    if morning_mask.any():
        frac = ((times[morning_mask] - sunrise) / window)
        frac = pd.Series(frac, index=times[morning_mask]).clip(lower=0, upper=1).astype(float)
        blend = 0.5 - 0.5 * np.cos(np.pi * frac)
        factors.loc[morning_mask] = damping_morning + (1.0 - damping_morning) * blend

    # Evening ramp-down: start at 1.0 at sunset-window, taper to damping_evening at sunset.
    evening_mask = (times >= sunset - window) & (times <= sunset)
    if evening_mask.any():
        frac = ((sunset - times[evening_mask]) / window)
        frac = pd.Series(frac, index=times[evening_mask]).clip(lower=0, upper=1).astype(float)
        blend = 0.5 - 0.5 * np.cos(np.pi * frac)
        factors.loc[evening_mask] = damping_evening + (1.0 - damping_evening) * blend

    factors = factors.clip(lower=min(damping_morning, damping_evening), upper=1.0)
    return factors


def simulate_day(
    scenario: Scenario,
    date: dt.date,
    timestep: str = "1h",
    weather_provider=None,
    debug: DebugCollector | None = None,
    weather_label: str = "end",
    weather_mode: str | None = None,
) -> SimulationResult:
    """Run full-day simulation for all sites/arrays in scenario."""

    debug = debug or NullDebugCollector()
    # Allow explicit weather provider override, otherwise pick based on weather_mode
    if weather_provider is None:
        if (weather_mode or "").lower() == "cloud-scaled":
            weather_provider = CloudScaledWeatherProvider(debug=debug)
        else:
            weather_provider = OpenMeteoWeatherProvider(debug=debug)

    if (weather_mode or "standard").lower() not in {"standard", "cloud-scaled"}:
        raise ValueError("weather_mode must be 'standard' or 'cloud-scaled'")

    start, end = _daterange_bounds(date)
    locations = [{"id": site.id, "lat": site.location.lat, "lon": site.location.lon} for site in scenario.sites]
    debug.emit(
        "weather.request",
        {"timestep": timestep, "locations": [loc["id"] for loc in locations]},
        ts=start,
        site=None,
    )
    weather = weather_provider.get_forecast(locations, start=start, end=end, timestep=timestep)
    # emit raw weather snapshot for audit (per site to keep site ids consistent)
    for loc_id, df in weather.items():
        try:
            serial = df.reset_index().rename(columns={"index": "ts"})
            debug.emit(
                "weather.raw",
                {"data": serial.to_dict(orient="records")},
                ts=start,
                site=loc_id,
            )
        except Exception as exc:  # pragma: no cover - best effort; don't fail pipeline
            debug.emit("weather.raw_error", {"error": str(exc)}, ts=start, site=loc_id)

    daily_rows = []
    timeseries: Dict[Tuple[str, str], pd.DataFrame] = {}

    required_cols = {"ghi_wm2", "dni_wm2", "dhi_wm2", "temp_air_c", "wind_ms"}

    for site in scenario.sites:
        wx = weather[str(site.id)]
        site_debug = ScopedDebugCollector(debug, site=site.id)

        missing = required_cols.difference(wx.columns)
        if missing:
            site_debug.emit(
                "weather.schema_error",
                {"missing_columns": sorted(missing), "available": sorted(wx.columns)},
                ts=wx.index[0] if len(wx.index) else None,
            )
            raise ValueError(f"Weather data for site {site.id} missing required columns: {sorted(missing)}")

        # Enforce exact [date, date+1) window regardless of provider inclusivity semantics.
        if hasattr(wx.index, "tz") and wx.index.tz is not None:
            start_ts = pd.Timestamp(date, tz=wx.index.tz)
        else:
            start_ts = pd.Timestamp(date)
        end_ts = start_ts + pd.Timedelta(days=1)
        wx = wx.loc[(wx.index >= start_ts) & (wx.index < end_ts)]

        times = wx.index
        step_seconds = _infer_step_seconds(times, timestep)

        # Emit minimal weather meta/summary even if provider didn't
        site_debug.emit(
            "weather.response_meta",
            {"timezone": str(times.tz), "timestep_seconds": step_seconds},
            ts=times[0] if len(times) else None,
        )
        site_debug.emit(
            "weather.summary",
            {
                "ghi_min": float(wx["ghi_wm2"].min()) if not wx.empty else None,
                "ghi_max": float(wx["ghi_wm2"].max()) if not wx.empty else None,
                "temp_min": float(wx["temp_air_c"].min()) if not wx.empty else None,
                "temp_max": float(wx["temp_air_c"].max()) if not wx.empty else None,
            },
            ts=times[0] if len(times) else None,
        )

        # Validate weather_label vs provider defaults when possible.
        if hasattr(weather_provider, "__class__"):
            provider_name = weather_provider.__class__.__name__.lower()
            if provider_name == "openmeteoweatherprovider" and weather_label.lower() == "start":
                site_debug.emit(
                    "weather.label_warning",
                    {"expected": "end", "received": weather_label},
                    ts=times[0] if len(times) else None,
                )

        # Use interval midpoints when we have a valid step to reduce bias from averaged irradiance.
        solar_times = _apply_time_label(times, step_seconds, weather_label)

        solar_pos = solar_position(site.location, solar_times, debug=debug, site_id=site.id)
        # Align back to original weather timestamps so downstream joins stay aligned.
        solar_pos.index = times
        site_debug.emit("stage.solarpos", {"rows": len(solar_pos)}, ts=times[0])
        _emit_df(site_debug, "solar.position", solar_pos, ts=times[0], site=site.id)

        solar_noon_ts = solar_pos["elevation"].idxmax() if not solar_pos.empty else None

        # Precompute per-array POA/temp/DC (independent of inverter grouping)
        array_data = {}
        for array in site.arrays:
            arr_debug = ScopedDebugCollector(site_debug, array=array.id)
            poa = poa_irradiance(
                surface_tilt=array.tilt_deg,
                surface_azimuth=array.azimuth_deg,
                dni=wx["dni_wm2"],
                ghi=wx["ghi_wm2"],
                dhi=wx["dhi_wm2"],
                solar_zenith=solar_pos["zenith"],
                solar_azimuth=solar_pos["azimuth"],
                horizon_deg=array.horizon_deg,
                debug=arr_debug,
            )
            arr_debug.emit("stage.poa", {"rows": len(poa)}, ts=times[0])
            _emit_df(arr_debug, "poa.detail", poa, ts=times[0], site=site.id, array=array.id)

            temps = cell_temperature(
                poa_global=poa["poa_global"],
                temp_air_c=wx["temp_air_c"],
                wind_ms=wx["wind_ms"],
                mounting=array.temp_model,
                debug=arr_debug,
            )
            arr_debug.emit("stage.temp", {"rows": len(temps)}, ts=times[0])
            _emit_series(arr_debug, "temp.detail", temps.rename("temp_cell_c"), ts=times[0], site=site.id, array=array.id)

            damping = _damping_factor(
                times=times,
                solar_elevation=solar_pos["elevation"],
                solar_noon_ts=solar_noon_ts,
                damping_morning=array.damping_morning,
                damping_evening=array.damping_evening,
            )
            arr_debug.emit(
                "damping.summary",
                {
                    "morning": array.damping_morning,
                    "evening": array.damping_evening,
                    "min_factor": float(damping.min()) if len(damping) else None,
                    "max_factor": float(damping.max()) if len(damping) else None,
                },
                ts=times[0] if len(times) else None,
            )

            pdc = pvwatts_dc(
                effective_irradiance=poa["poa_global"] * damping,
                temp_cell=temps,
                pdc0_w=array.pdc0_w,
                gamma_pdc=array.gamma_pdc,
                debug=arr_debug,
            )
            arr_debug.emit("stage.dc", {"rows": len(pdc)}, ts=times[0])
            _emit_series(arr_debug, "dc.detail", pdc.rename("pdc_w"), ts=times[0], site=site.id, array=array.id)

            array_data[array.id] = {
                "debug": arr_debug,
                "poa": poa,
                "temps": temps,
                "pdc": pdc,
                "array": array,
            }

        # Group arrays by inverter_group_id (None -> its own group)
        groups: Dict[str, list[str]] = {}
        for arr_id, data in array_data.items():
            group_id = data["array"].inverter_group_id or arr_id
            groups.setdefault(group_id, []).append(arr_id)

        # Compute group AC and allocate back to arrays
        for group_id, arr_ids in groups.items():
            pdc_sum = sum(array_data[a]["pdc"] for a in arr_ids)

            explicit_sizes = {
                array_data[a]["array"].inverter_pdc0_w for a in arr_ids if array_data[a]["array"].inverter_pdc0_w is not None
            }

            if explicit_sizes:
                if len(explicit_sizes) > 1:
                    raise ValueError(
                        f"Arrays in inverter group '{group_id}' specify conflicting inverter_pdc0_w values: {sorted(explicit_sizes)}"
                    )
                pdc0_inv = explicit_sizes.pop()
                # If user gave an AC nameplate (rare), we still respect eta_inv_nom to keep pac0=eta_inv_nom*pdc0_inv
                eta_inv_nom = max(array_data[a]["array"].eta_inv_nom for a in arr_ids)
            else:
                pdc0_group = sum(array_data[a]["array"].pdc0_w for a in arr_ids)
                dc_ac_ratio = max(array_data[a]["array"].dc_ac_ratio for a in arr_ids)
                eta_inv_nom = max(array_data[a]["array"].eta_inv_nom for a in arr_ids)
                pdc0_inv = inverter_pdc0_from_dc_ac_ratio(pdc0_group, dc_ac_ratio, eta_inv_nom)

            pac_group = pvwatts_ac(pdc_sum, pdc0_inv_w=pdc0_inv, eta_inv_nom=eta_inv_nom, debug=site_debug)
            _emit_series(site_debug, "ac.group", pac_group.rename(f"pac_group_{group_id}"), ts=times[0], site=site.id)

            # allocate by DC share per timestep; handle zeros
            pdc_sum_safe = pdc_sum.replace(0, pd.NA).infer_objects(copy=False)
            share = {
                a: (array_data[a]["pdc"].infer_objects(copy=False) / pdc_sum_safe).infer_objects(copy=False)
                for a in arr_ids
            }
            for a in arr_ids:
                pac_arr = pac_group * share[a].fillna(0)
                array_data[a]["pac"] = pac_arr

        # Fallback for any array missing pac (shouldn't happen)
        for arr_id, data in array_data.items():
            if "pac" not in data:
                array = data["array"]
                pdc0_inv = array.inverter_pdc0_w or inverter_pdc0_from_dc_ac_ratio(array.pdc0_w, array.dc_ac_ratio, array.eta_inv_nom)
                data["pac"] = pvwatts_ac(data["pdc"], pdc0_inv_w=pdc0_inv, eta_inv_nom=array.eta_inv_nom, debug=data["debug"])

        # Apply losses and aggregate per array
        for arr_id, data in array_data.items():
            array = data["array"]
            arr_debug = data["debug"]
            poa = data["poa"]
            temps = data["temps"]
            pac = data["pac"]

            arr_debug.emit("stage.ac", {"rows": len(pac)}, ts=times[0])
            _emit_series(arr_debug, "ac.detail", pac.rename("pac_w"), ts=times[0], site=site.id, array=array.id)

            pac_net = apply_losses(pac, array.losses_percent, debug=arr_debug)
            interval_h = _interval_hours(pac_net.index, step_seconds=step_seconds, label=weather_label)
            arr_debug.emit(
                "stage.aggregate",
                {"rows": len(pac_net), "interval_h_mean": float(interval_h.mean()) if len(interval_h) else None},
                ts=times[0],
            )
            _emit_series(arr_debug, "ac.net", pac_net.rename("pac_net_w"), ts=times[0], site=site.id, array=array.id)
            _emit_series(arr_debug, "intervals", interval_h.rename("interval_h"), ts=times[0], site=site.id, array=array.id)

            energy_kwh = float(((pac_net / 1000.0) * interval_h).sum())
            peak_kw = float(pac_net.max() / 1000)
            poa_kwh_m2 = float(((poa["poa_global"] / 1000.0) * interval_h).sum())
            temp_cell_max = float(temps.max())

            daily_rows.append(
                {
                    "site": site.id,
                    "array": array.id,
                    "date": date.isoformat(),
                    "energy_kwh": energy_kwh,
                    "peak_kw": peak_kw,
                    "poa_kwh_m2": poa_kwh_m2,
                    "temp_cell_max": temp_cell_max,
                }
            )

            ts_df = pd.DataFrame(
                {
                    "poa_global": poa["poa_global"],
                    "temp_cell_c": temps,
                    "pdc_w": data["pdc"],
                    "pac_w": pac,
                    "pac_net_w": pac_net,
                    "interval_h": interval_h,
                }
            )
            _emit_df(arr_debug, "timeseries", ts_df, ts=times[0], site=site.id, array=array.id)
            timeseries[(site.id, array.id)] = ts_df

    # Clamp implausible daily POA/energy vs PVGIS climatology (guideline: 0.6x–1.6x).
    if daily_rows:
        capped_rows = []
        for row in daily_rows:
            pvgis = row.get("pvgis_poa_kwh_m2")
            poa = row.get("poa_kwh_m2")
            if pvgis is None or poa is None or pvgis <= 0:
                capped_rows.append(row)
                continue
            ratio = poa / pvgis
            if 0.6 <= ratio <= 1.6:
                capped_rows.append(row)
                continue
            cap = 1.6 if ratio > 1.6 else 0.6
            scale = cap / ratio if ratio != 0 else 1.0
            row = row.copy()
            row["poa_kwh_m2"] = poa * scale
            row["energy_kwh"] = row["energy_kwh"] * scale
            row["peak_kw"] = row["peak_kw"] * scale
            row["qc_clipped"] = True
            row["qc_ratio"] = ratio
            capped_rows.append(row)
        daily_rows = capped_rows

    daily_df = pd.DataFrame(daily_rows)
    return SimulationResult(daily=daily_df, timeseries=timeseries)


__all__ = ["simulate_day", "SimulationResult"]
