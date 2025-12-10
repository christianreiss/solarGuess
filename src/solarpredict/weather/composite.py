"""Composite weather provider: primary forecast + secondary climatology fallback."""

from __future__ import annotations

from typing import Dict, Iterable

import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector, ScopedDebugCollector
from .base import WeatherProvider

_REQ_COLS = ["temp_air_c", "wind_ms", "ghi_wm2", "dhi_wm2", "dni_wm2"]


class CompositeWeatherProvider(WeatherProvider):
    """Use a primary provider, filling gaps from a secondary provider (e.g., PVGIS TMY).

    Both providers are always called; secondary is used to fill NaNs or negative irradiance
    in the primary output. Indexes are aligned to the primary index via reindex/ffill.
    """

    def __init__(self, primary: WeatherProvider, secondary: WeatherProvider, debug: DebugCollector | None = None):
        self.primary = primary
        self.secondary = secondary
        self.debug = debug or NullDebugCollector()

    def _align_secondary(self, sec: pd.DataFrame, target_index: pd.DatetimeIndex) -> pd.DataFrame:
        """Reindex secondary data to primary timestamps; forward-fill then back-fill."""
        sec_aligned = sec.reindex(target_index, method="pad")
        sec_aligned = sec_aligned.fillna(method="bfill")
        return sec_aligned

    def get_forecast(
        self,
        locations: Iterable[Dict[str, str | float]],
        start: str,
        end: str,
        timestep: str = "1h",
    ) -> Dict[str, pd.DataFrame]:
        primary = self.primary.get_forecast(locations, start=start, end=end, timestep=timestep)
        secondary = self.secondary.get_forecast(locations, start=start, end=end, timestep="1h")

        merged: Dict[str, pd.DataFrame] = {}
        for loc in locations:
            loc_id = str(loc["id"])
            prim_df = primary[loc_id]
            sec_df = self._align_secondary(secondary[loc_id], prim_df.index)

            filled = prim_df.copy()
            fill_counts = {}
            for col in _REQ_COLS:
                if col not in filled.columns or col not in sec_df.columns:
                    continue
                before_na = filled[col].isna() | (filled[col] < 0)
                filled.loc[before_na, col] = sec_df.loc[before_na, col]
                after_na = filled[col].isna()
                fill_counts[col] = int(before_na.sum())
                # Clip negative irradiance just in case
                if "wm2" in col:
                    filled[col] = filled[col].clip(lower=0)
            # emit merge stats
            ScopedDebugCollector(self.debug, site=loc_id).emit(
                "weather.merge",
                {"filled_points": fill_counts},
                ts=filled.index[0] if len(filled.index) else None,
            )
            merged[loc_id] = filled
        return merged


__all__ = ["CompositeWeatherProvider"]
