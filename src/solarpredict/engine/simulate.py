"""End-to-end daily simulation engine."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Tuple

import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector, ScopedDebugCollector
from solarpredict.core.models import Scenario
from solarpredict.pv.power import apply_losses, inverter_pdc0_from_dc_ac_ratio, pvwatts_ac, pvwatts_dc
from solarpredict.solar.irradiance import poa_irradiance
from solarpredict.solar.position import solar_position
from solarpredict.solar.temperature import cell_temperature
from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider


@dataclass(frozen=True)
class SimulationResult:
    daily: pd.DataFrame
    timeseries: Dict[Tuple[str, str], pd.DataFrame]


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
        return pd.Series([fill / 3600.0], index=index, dtype=float)

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

    return widths / 3600.0


def simulate_day(
    scenario: Scenario,
    date: dt.date,
    timestep: str = "1h",
    weather_provider=None,
    debug: DebugCollector | None = None,
    weather_label: str = "end",
) -> SimulationResult:
    """Run full-day simulation for all sites/arrays in scenario."""

    debug = debug or NullDebugCollector()
    weather_provider = weather_provider or OpenMeteoWeatherProvider(debug=debug)

    start, end = _daterange_bounds(date)
    locations = [{"id": site.id, "lat": site.location.lat, "lon": site.location.lon} for site in scenario.sites]
    debug.emit(
        "weather.request",
        {"timestep": timestep, "locations": [loc["id"] for loc in locations]},
        ts=start,
        site=None,
    )
    weather = weather_provider.get_forecast(locations, start=start, end=end, timestep=timestep)

    daily_rows = []
    timeseries: Dict[Tuple[str, str], pd.DataFrame] = {}

    for site in scenario.sites:
        wx = weather[str(site.id)]
        site_debug = ScopedDebugCollector(debug, site=site.id)

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

        # Use interval midpoints when we have a valid step to reduce bias from averaged irradiance.
        solar_times = _apply_time_label(times, step_seconds, weather_label)

        solar_pos = solar_position(site.location, solar_times, debug=debug, site_id=site.id)
        # Align back to original weather timestamps so downstream joins stay aligned.
        solar_pos.index = times
        site_debug.emit("stage.solarpos", {"rows": len(solar_pos)}, ts=times[0])

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
                debug=arr_debug,
            )
            arr_debug.emit("stage.poa", {"rows": len(poa)}, ts=times[0])

            temps = cell_temperature(
                poa_global=poa["poa_global"],
                temp_air_c=wx["temp_air_c"],
                wind_ms=wx["wind_ms"],
                mounting=array.temp_model,
                debug=arr_debug,
            )
            arr_debug.emit("stage.temp", {"rows": len(temps)}, ts=times[0])

            pdc = pvwatts_dc(
                effective_irradiance=poa["poa_global"],
                temp_cell=temps,
                pdc0_w=array.pdc0_w,
                gamma_pdc=array.gamma_pdc,
                debug=arr_debug,
            )
            arr_debug.emit("stage.dc", {"rows": len(pdc)}, ts=times[0])

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

            # allocate by DC share per timestep; handle zeros
            share = {a: array_data[a]["pdc"] / pdc_sum.replace(0, pd.NA) for a in arr_ids}
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

            pac_net = apply_losses(pac, array.losses_percent, debug=arr_debug)
            interval_h = _interval_hours(pac_net.index, step_seconds=step_seconds, label=weather_label)
            arr_debug.emit(
                "stage.aggregate",
                {"rows": len(pac_net), "interval_h_mean": float(interval_h.mean()) if len(interval_h) else None},
                ts=times[0],
            )

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
            timeseries[(site.id, array.id)] = ts_df

    daily_df = pd.DataFrame(daily_rows)
    return SimulationResult(daily=daily_df, timeseries=timeseries)


__all__ = ["simulate_day", "SimulationResult"]
