"""PVGIS weather provider (typical meteorological year JSON service)."""

from __future__ import annotations

import datetime as dt
import json
from typing import Dict, Iterable

from pathlib import Path

import pandas as pd
import requests

from solarpredict.core.debug import DebugCollector, NullDebugCollector
from .base import WeatherProvider


class PVGISWeatherProvider(WeatherProvider):
    """Fetch irradiance/meteorology from the PVGIS TMY endpoint.

    Notes
    -----
    * PVGIS TMY returns a single "typical" year; we re-stamp the returned timestamps
      to the requested start year so downstream day slicing works.
    * Timestamps are reported at HH:00 but irradiance represents the SARAH/ERA5
      sample at HH:MM (see PVGIS docs). We leave the offset untouched; the caller
      can adjust via `weather_label`.
    """

    def __init__(
        self,
        base_url: str = "https://re.jrc.ec.europa.eu/api/v5_3/tmy",
        debug: DebugCollector | None = None,
        session: requests.Session | None = None,
        cache_dir: str | Path | None = None,
    ):
        self.base_url = base_url
        self.debug = debug or NullDebugCollector()
        self.session = session or requests.Session()
        self.cache_dir = Path(cache_dir) if cache_dir else None

    def _build_params(self, loc: Dict[str, str | float]) -> Dict[str, str]:
        return {
            "lat": str(loc["lat"]),
            "lon": str(loc["lon"]),
            "outputformat": "json",
            "browser": "0",
        }

    @staticmethod
    def _parse_time(values: list[str], target_year: int) -> pd.DatetimeIndex:
        idx = pd.to_datetime(values, format="%Y%m%d:%H%M", utc=True)
        # Re-stamp year to match caller's requested year so the engine's date window works.
        idx = idx.map(lambda ts: ts.replace(year=target_year))
        return idx

    def _parse_single(self, payload: Dict[str, any], target_year: int) -> pd.DataFrame:
        tmy = payload.get("outputs", {}).get("tmy_hourly")
        if not tmy:
            raise ValueError("PVGIS response missing tmy_hourly block")

        time_key = "time" if "time" in tmy[0] else "time(UTC)"
        time_values = [row[time_key] for row in tmy]
        index = self._parse_time(time_values, target_year)

        def col(name: str, default: float | None = None) -> list:
            return [row.get(name, default) for row in tmy]

        data = {
            "temp_air_c": col("T2m"),
            "wind_ms": col("WS10m"),
            "ghi_wm2": col("G(h)"),
            "dni_wm2": col("Gb(n)"),
            "dhi_wm2": col("Gd(h)"),
        }
        df = pd.DataFrame(data, index=index)
        df.index.name = "ts"
        return df

    def _emit_summary(self, loc_id: str, df: pd.DataFrame) -> None:
        payload = {
            "ghi_min": float(df["ghi_wm2"].min()) if not df.empty else None,
            "ghi_max": float(df["ghi_wm2"].max()) if not df.empty else None,
            "temp_min": float(df["temp_air_c"].min()) if not df.empty else None,
            "temp_max": float(df["temp_air_c"].max()) if not df.empty else None,
        }
        ts = df.index[0] if not df.empty else None
        self.debug.emit("weather.summary", payload, ts=ts, site=loc_id)

    def get_forecast(
        self,
        locations: Iterable[Dict[str, str | float]],
        start: str,
        end: str,
        timestep: str = "1h",
    ) -> Dict[str, pd.DataFrame]:
        if timestep != "1h":
            raise ValueError("PVGIS TMY only supports 1h timestep")

        target_year = int(dt.date.fromisoformat(start).year)

        results: Dict[str, pd.DataFrame] = {}
        for loc in locations:
            params = self._build_params(loc)
            cache_hit = False
            if self.cache_dir:
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                cache_path = self.cache_dir / f"pvgis_tmy_{loc['lat']}_{loc['lon']}.json"
                if cache_path.exists():
                    data = json.loads(cache_path.read_text())
                    cache_hit = True
            if not cache_hit:
                self.debug.emit("weather.request", {"url": self.base_url, "params": params}, ts=start, site=loc["id"])
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
                        self.debug.emit("weather.retry", {"attempt": attempt, "error": str(exc)}, ts=start, site=loc["id"])
                        import time as _time

                        _time.sleep(backoff)
                if self.cache_dir:
                    cache_path.write_text(json.dumps(data))
            df = self._parse_single(data, target_year=target_year)
            self.debug.emit(
                "weather.response_meta",
                {
                    "timezone": "UTC",
                    "source": data.get("inputs", {}).get("meteo_data", {}).get("radiation_db"),
                    "cache": cache_hit,
                },
                ts=df.index[0] if not df.empty else None,
                site=loc["id"],
            )
            self._emit_summary(str(loc["id"]), df)
            results[str(loc["id"])] = df
        return results


__all__ = ["PVGISWeatherProvider"]
