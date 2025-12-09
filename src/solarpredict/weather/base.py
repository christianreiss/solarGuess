"""Abstract weather provider protocol."""

from __future__ import annotations

from typing import Dict, Iterable, Protocol

import pandas as pd


class WeatherProvider(Protocol):
    """Interface for fetching weather forecasts."""

    def get_forecast(
        self,
        locations: Iterable[Dict[str, str | float]],
        start: str,
        end: str,
        timestep: str = "1h",
    ) -> Dict[str, pd.DataFrame]:
        """Return forecast per location id keyed by `id` from `locations`.

        Each DataFrame is indexed by tz-aware timestamps and contains normalized columns:
        - temp_air_c
        - wind_ms
        - ghi_wm2
        - dhi_wm2
        - dni_wm2
        """
        ...


__all__ = ["WeatherProvider"]
