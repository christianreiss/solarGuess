"""Weather provider wrapper backed by pre-fetched data.

Used to avoid N network calls when evaluating many historical days.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Iterable

import pandas as pd

from solarpredict.weather.base import WeatherProvider


@dataclass(frozen=True)
class PrefetchedWeatherProvider(WeatherProvider):
    """Serve weather slices from an in-memory dict of dataframes.

    The stored dataframes must be indexed by timestamps and include the columns
    expected by the engine (ghi_wm2/dni_wm2/dhi_wm2/temp_air_c/wind_ms).
    """

    data: Dict[str, pd.DataFrame]

    def get_forecast(
        self,
        locations: Iterable[Dict[str, str | float]],
        start: str,
        end: str,
        timestep: str = "1h",
    ) -> Dict[str, pd.DataFrame]:
        try:
            start_d = dt.date.fromisoformat(start)
            end_d = dt.date.fromisoformat(end)
        except ValueError:
            # Best effort: if not ISO dates, return full data to let callers fail elsewhere.
            start_d = None  # type: ignore[assignment]
            end_d = None  # type: ignore[assignment]

        out: Dict[str, pd.DataFrame] = {}
        for loc in locations:
            loc_id = str(loc["id"])
            if loc_id not in self.data:
                raise ValueError(f"Prefetched weather missing location id {loc_id}")
            df = self.data[loc_id]
            if df is None or df.empty:
                out[loc_id] = df
                continue

            # Slice by date window if possible. We honor the df timezone if present.
            if start_d is not None and end_d is not None:
                tz = df.index.tz
                start_ts = pd.Timestamp(start_d, tz=tz) if tz is not None else pd.Timestamp(start_d)
                end_ts = pd.Timestamp(end_d, tz=tz) if tz is not None else pd.Timestamp(end_d)
                out[loc_id] = df.loc[(df.index >= start_ts) & (df.index < end_ts)]
            else:
                out[loc_id] = df
        return out


__all__ = ["PrefetchedWeatherProvider"]

