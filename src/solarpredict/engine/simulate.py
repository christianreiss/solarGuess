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


def simulate_day(
    scenario: Scenario,
    date: dt.date,
    timestep: str = "1h",
    weather_provider=None,
    debug: DebugCollector | None = None,
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

        # Use interval midpoints when we have a valid step to reduce bias from backward-averaged irradiance (Open‑Meteo behavior).
        solar_times = times
        if step_seconds > 0:
            solar_times = times + pd.to_timedelta(step_seconds / 2.0, unit="s")

        solar_pos = solar_position(site.location, solar_times, debug=debug)
        # Align back to original weather timestamps so downstream joins stay aligned.
        solar_pos.index = times
        site_debug.emit("stage.solarpos", {"rows": len(solar_pos)}, ts=times[0])

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

            pdc0_inv = inverter_pdc0_from_dc_ac_ratio(array.pdc0_w, array.dc_ac_ratio, array.eta_inv_nom)
            pac = pvwatts_ac(pdc, pdc0_inv_w=pdc0_inv, eta_inv_nom=array.eta_inv_nom, debug=arr_debug)
            arr_debug.emit("stage.ac", {"rows": len(pac)}, ts=times[0])

            pac_net = apply_losses(pac, array.losses_percent, debug=arr_debug)
            arr_debug.emit("stage.aggregate", {"rows": len(pac_net)}, ts=times[0])

            # Aggregate daily metrics
            step_hours = step_seconds / 3600.0
            energy_kwh = float((pac_net * step_hours / 1000).sum()) if step_hours > 0 else 0.0
            peak_kw = float(pac_net.max() / 1000)
            poa_kwh_m2 = float((poa["poa_global"] * step_hours / 1000).sum()) if step_hours > 0 else 0.0
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
                    "pdc_w": pdc,
                    "pac_w": pac,
                    "pac_net_w": pac_net,
                }
            )
            timeseries[(site.id, array.id)] = ts_df

    daily_df = pd.DataFrame(daily_rows)
    return SimulationResult(daily=daily_df, timeseries=timeseries)


__all__ = ["simulate_day", "SimulationResult"]
