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
    "cloudcover": "cloudcover",
}


class OpenMeteoWeatherProvider(WeatherProvider):
    _COORD_TOLERANCE_DEG = 0.01  # ~1.1 km; generous enough for rounding but avoids cross-city swaps

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

        hourly_vars = ",".join(k for k in _VAR_MAP.keys() if k != "cloudcover")
        cloud_param = "cloudcover"
        params = {
            "latitude": ",".join(lats),
            "longitude": ",".join(lons),
            "timezone": "auto",
            "start_date": start,
            "end_date": end,
        }
        if timestep == "15m":
            params["minutely_15"] = ",".join([hourly_vars, cloud_param])
        else:
            params["hourly"] = ",".join([hourly_vars, cloud_param])
        # Request wind in m/s to match downstream expectations (temp model assumes m/s).
        params["wind_speed_unit"] = "ms"
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

        # Open‑Meteo returns local times when `timezone=auto`. Parsing as UTC and then converting
        # would shift the series by the offset (e.g., +1h for Europe/Berlin). Instead:
        #   - parse strings as naive
        #   - if API supplies a timezone, localize (not convert) the naive timestamps
        #   - if timestamps are already tz-aware (e.g., when requesting `timezone=UTC`), honor them
        index = pd.to_datetime(time_values)
        if index.tz is None:
            if timezone:
                index = index.tz_localize(timezone)
            else:
                index = index.tz_localize("UTC")
        elif timezone:
            index = index.tz_convert(timezone)

        data: Dict[str, List] = {}
        for api_key, col in _VAR_MAP.items():
            series = time_block.get(api_key, [])
            if not series:
                series = [None] * len(index)
            if api_key == "wind_speed_10m":
                # API may return km/h if caller didn't request m/s; convert just in case.
                units = payload.get("hourly_units", {}).get("wind_speed_10m")
                if units == "km/h":
                    series = [v / 3.6 if v is not None else None for v in series]
            data[col] = series
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
        params["wind_speed_unit"] = "ms"
        self.debug.emit("weather.request", {"url": self.base_url, "params": params}, ts=start)
        for attempt in range(1, 4):
            try:
                resp = self.session.get(self.base_url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as exc:
                if attempt == 3:
                    raise
                backoff = 0.5 * attempt
                self.debug.emit("weather.retry", {"attempt": attempt, "error": str(exc)}, ts=start)
                import time as _time

                _time.sleep(backoff)
        if not isinstance(data, list):  # Open-Meteo returns list for multiple coordinates
            data = [data]

        # Build an index of requested locations to reconcile responses even if Open-Meteo reorders/omits entries.
        requested = [
            {
                "id": str(loc["id"]),
                "lat": float(loc["lat"]),
                "lon": float(loc["lon"]),
            }
            for loc in locations
        ]
        unmatched_idxs = set(range(len(requested)))

        def _match_location_id(lat: float, lon: float) -> str:
            for idx in list(unmatched_idxs):
                req = requested[idx]
                if abs(lat - req["lat"]) <= self._COORD_TOLERANCE_DEG and abs(lon - req["lon"]) <= self._COORD_TOLERANCE_DEG:
                    unmatched_idxs.remove(idx)
                    return req["id"]
            raise ValueError(
                f"Open-Meteo returned unexpected coordinate ({lat}, {lon}); no requested location within tolerance"
            )

        results: Dict[str, pd.DataFrame] = {}
        for idx, loc_payload in enumerate(data):
            lat = loc_payload.get("latitude")
            lon = loc_payload.get("longitude")
            if lat is None or lon is None:
                raise ValueError("Open-Meteo response missing latitude/longitude for a location entry")
            loc_id = _match_location_id(float(lat), float(lon))
            df = self._parse_single(loc_payload)
            self.debug.emit(
                "weather.response_meta",
                {
                    "model": loc_payload.get("model"),
                    "timezone": loc_payload.get("timezone"),
                    "time_key": "minutely_15" if "minutely_15" in loc_payload else "hourly",
                    "lat": lat,
                    "lon": lon,
                    "matched_id": loc_id,
                },
                ts=df.index[0] if not df.empty else None,
                site=loc_id,
            )
            self._emit_summary(loc_id, df)
            results[loc_id] = df

        if unmatched_idxs:
            missing = [requested[i]["id"] for i in sorted(unmatched_idxs)]
            raise ValueError(
                f"Open-Meteo response missing {len(missing)} requested locations; unmatched ids: {missing}"
            )
        return results


__all__ = ["OpenMeteoWeatherProvider"]
