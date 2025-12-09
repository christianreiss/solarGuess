"""Open-Meteo weather provider."""

from __future__ import annotations

import datetime as dt
from typing import Dict, Iterable, List

import pandas as pd
import requests

from solarpredict.core.debug import DebugCollector, NullDebugCollector
from .base import WeatherProvider

_VAR_MAP = {
    "temperature_2m": "temp_air_c",
    "wind_speed_10m": "wind_ms",
    "shortwave_radiation": "ghi_wm2",
    "diffuse_radiation": "dhi_wm2",
    "direct_normal_irradiance": "dni_wm2",
}


class OpenMeteoWeatherProvider(WeatherProvider):
    def __init__(
        self,
        base_url: str = "https://api.open-meteo.com/v1/forecast",
        debug: DebugCollector | None = None,
        session: requests.Session | None = None,
    ):
        self.base_url = base_url
        self.debug = debug or NullDebugCollector()
        self.session = session or requests.Session()

    def _build_params(
        self, locations: Iterable[Dict[str, str | float]], start: str, end: str, timestep: str
    ) -> Dict[str, str]:
        lats, lons, ids = [], [], []
        for loc in locations:
            lats.append(str(loc["lat"]))
            lons.append(str(loc["lon"]))
            ids.append(str(loc["id"]))

        hourly_vars = ",".join(_VAR_MAP.keys())
        params = {
            "latitude": ",".join(lats),
            "longitude": ",".join(lons),
            "timezone": "auto",
            "start_date": start,
            "end_date": end,
        }
        if timestep == "15m":
            params["minutely_15"] = hourly_vars
        else:
            params["hourly"] = hourly_vars
        # echo ids back to parse by position; Open-Meteo doesn't support this yet but we keep for traceability
        params["location_ids"] = ",".join(ids)
        return params

    def _emit_summary(self, loc_id: str, df: pd.DataFrame) -> None:
        payload = {
            "ghi_min": float(df["ghi_wm2"].min()) if not df.empty else None,
            "ghi_max": float(df["ghi_wm2"].max()) if not df.empty else None,
            "temp_min": float(df["temp_air_c"].min()) if not df.empty else None,
            "temp_max": float(df["temp_air_c"].max()) if not df.empty else None,
        }
        ts = df.index[0] if not df.empty else None
        self.debug.emit("weather.summary", payload, ts=ts, site=loc_id)

    def _parse_single(self, payload: Dict[str, any]) -> pd.DataFrame:
        time_block = payload.get("hourly") or payload.get("minutely_15")
        if time_block is None:
            raise ValueError("Open-Meteo response missing time series block")
        time_values = time_block["time"]
        timezone = payload.get("timezone")
        index = pd.to_datetime(time_values, utc=True)
        if timezone:
            index = index.tz_convert(timezone)

        data: Dict[str, List] = {}
        for api_key, col in _VAR_MAP.items():
            data[col] = time_block.get(api_key, [])
        df = pd.DataFrame(data, index=index)
        df.index.name = "ts"
        return df

    def get_forecast(
        self,
        locations: Iterable[Dict[str, str | float]],
        start: str,
        end: str,
        timestep: str = "1h",
    ) -> Dict[str, pd.DataFrame]:
        if timestep not in {"1h", "15m"}:
            raise ValueError("timestep must be '1h' or '15m'")

        params = self._build_params(locations, start, end, timestep)
        self.debug.emit("weather.request", {"url": self.base_url, "params": params}, ts=start)
        resp = self.session.get(self.base_url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):  # Open-Meteo returns list for multiple coordinates
            data = [data]

        results: Dict[str, pd.DataFrame] = {}
        for idx, loc_payload in enumerate(data):
            loc_id = params["location_ids"].split(",")[idx] if params.get("location_ids") else str(idx)
            df = self._parse_single(loc_payload)
            self.debug.emit(
                "weather.response_meta",
                {
                    "model": loc_payload.get("model"),
                    "timezone": loc_payload.get("timezone"),
                    "time_key": "minutely_15" if "minutely_15" in loc_payload else "hourly",
                },
                ts=df.index[0] if not df.empty else None,
                site=loc_id,
            )
            self._emit_summary(loc_id, df)
            results[loc_id] = df
        return results


__all__ = ["OpenMeteoWeatherProvider"]
