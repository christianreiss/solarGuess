"""End-to-end daily simulation engine."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Tuple

import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector
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
        times = wx.index

        # Emit minimal weather meta/summary even if provider didn't
        debug.emit(
            "weather.response_meta",
            {"timezone": str(times.tz), "timestep_seconds": float(times.to_series().diff().dt.total_seconds().median() or 0)},
            ts=times[0] if len(times) else None,
            site=site.id,
        )
        debug.emit(
            "weather.summary",
            {
                "ghi_min": float(wx["ghi_wm2"].min()) if not wx.empty else None,
                "ghi_max": float(wx["ghi_wm2"].max()) if not wx.empty else None,
                "temp_min": float(wx["temp_air_c"].min()) if not wx.empty else None,
                "temp_max": float(wx["temp_air_c"].max()) if not wx.empty else None,
            },
            ts=times[0] if len(times) else None,
            site=site.id,
        )

        solar_pos = solar_position(site.location, times, debug=debug)
        debug.emit("stage.solarpos", {"rows": len(solar_pos)}, ts=times[0], site=site.id)

        for array in site.arrays:
            poa = poa_irradiance(
                surface_tilt=array.tilt_deg,
                surface_azimuth=array.azimuth_deg,
                dni=wx["dni_wm2"],
                ghi=wx["ghi_wm2"],
                dhi=wx["dhi_wm2"],
                solar_zenith=solar_pos["zenith"],
                solar_azimuth=solar_pos["azimuth"],
                debug=debug,
            )
            debug.emit("stage.poa", {"rows": len(poa)}, ts=times[0], site=site.id, array=array.id)

            temps = cell_temperature(
                poa_global=poa["poa_global"],
                temp_air_c=wx["temp_air_c"],
                wind_ms=wx["wind_ms"],
                mounting=array.temp_model,
                debug=debug,
            )
            debug.emit("stage.temp", {"rows": len(temps)}, ts=times[0], site=site.id, array=array.id)

            pdc = pvwatts_dc(
                effective_irradiance=poa["poa_global"],
                temp_cell=temps,
                pdc0_w=array.pdc0_w,
                gamma_pdc=array.gamma_pdc,
                debug=debug,
            )
            debug.emit("stage.dc", {"rows": len(pdc)}, ts=times[0], site=site.id, array=array.id)

            pdc0_inv = inverter_pdc0_from_dc_ac_ratio(array.pdc0_w, array.dc_ac_ratio, array.eta_inv_nom)
            pac = pvwatts_ac(pdc, pdc0_inv_w=pdc0_inv, eta_inv_nom=array.eta_inv_nom, debug=debug)
            debug.emit("stage.ac", {"rows": len(pac)}, ts=times[0], site=site.id, array=array.id)

            pac_net = apply_losses(pac, array.losses_percent, debug=debug)
            debug.emit("stage.aggregate", {"rows": len(pac_net)}, ts=times[0], site=site.id, array=array.id)

            # Aggregate daily metrics
            step_hours = (pac_net.index.to_series().diff().dt.total_seconds().median() or 3600) / 3600
            energy_kwh = float((pac_net * step_hours / 1000).sum())
            peak_kw = float(pac_net.max() / 1000)
            poa_kwh_m2 = float((poa["poa_global"] * step_hours / 1000).sum())
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
