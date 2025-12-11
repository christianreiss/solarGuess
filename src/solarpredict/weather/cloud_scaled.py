"""Weather provider that scales clear-sky irradiance by cloud cover.

Implements PLAN 6.x.x "Cloud-Cover Scaling Path":
- 6.1.1: pull cloudcover (%) from Open-Meteo
- 6.1.2: expose `weather_mode=cloud-scaled`
- 6.2.x: compute clear-sky baseline (Ineichen)
- 6.3.x: map cloud→clearness, scale GHI/DNI/DHI, emit debug
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Callable

import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector, ScopedDebugCollector
from solarpredict.solar.clear_sky import clear_sky_irradiance
from solarpredict.weather.base import WeatherProvider
from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider


def default_cloud_to_clearness(cloud_fraction: pd.Series) -> pd.Series:
    """Empirical mapping from fractional cloud cover → clearness index.

    Formula per howto.md & PLAN: k_t = 1 - 0.75 * C**3.4 where C in [0,1].
    Clamped to [0,1] for stability.
    """

    c = cloud_fraction.clip(lower=0.0, upper=1.0).astype(float)
    k_t = 1.0 - 0.75 * (c ** 3.4)
    return k_t.clip(lower=0.0, upper=1.0)


@dataclass
class CloudScaledWeatherProvider(WeatherProvider):
    """Compose Open-Meteo cloud cover with clear-sky irradiance.

    Fetches cloudcover (%) plus temperature/wind from Open-Meteo, generates
    clear-sky irradiance using pvlib, then scales GHI/DNI/DHI by a clearness
    factor derived from cloud cover.
    """

    base_provider: OpenMeteoWeatherProvider
    debug: DebugCollector
    cloud_to_clearness: Callable[[pd.Series], pd.Series] = default_cloud_to_clearness

    def __init__(
        self,
        base_provider: OpenMeteoWeatherProvider | None = None,
        debug: DebugCollector | None = None,
        cloud_to_clearness: Callable[[pd.Series], pd.Series] | None = None,
    ):
        self.debug = debug or NullDebugCollector()
        self.base_provider = base_provider or OpenMeteoWeatherProvider(debug=self.debug)
        # Store mapper on the instance to avoid descriptor binding of class-level function.
        self.cloud_to_clearness = cloud_to_clearness or default_cloud_to_clearness

    def get_forecast(
        self,
        locations: Iterable[Dict[str, str | float]],
        start: str,
        end: str,
        timestep: str = "1h",
    ) -> Dict[str, pd.DataFrame]:
        # Extend base params to request cloudcover (%).
        provider = self.base_provider
        wx = provider.get_forecast(locations, start, end, timestep)

        results: Dict[str, pd.DataFrame] = {}
        for loc in locations:
            loc_id = str(loc["id"])
            df = wx[loc_id]
            loc_debug = ScopedDebugCollector(self.debug, site=loc_id)

            if "cloudcover" not in df.columns:
                raise ValueError("Open-Meteo response missing cloudcover for cloud-scaled mode")
            cloud_frac = (df["cloudcover"] / 100.0).astype(float).clip(lower=0.0, upper=1.0)

            clearness = self.cloud_to_clearness(cloud_frac)

            # Build clear-sky irradiance using site coordinates and timezone from df index.
            tz = str(df.index.tz) if df.index.tz is not None else None
            cs = clear_sky_irradiance(
                lat=float(loc["lat"]),
                lon=float(loc["lon"]),
                times=df.index,
                tz=tz,
                elevation_m=loc.get("elevation_m"),
                debug=loc_debug,
            )

            scaled = pd.DataFrame(index=df.index)
            for col in ("ghi_wm2", "dni_wm2", "dhi_wm2"):
                scaled[col] = (cs[col] * clearness).clip(lower=0.0)

            # Preserve temp/wind from Open-Meteo
            scaled["temp_air_c"] = df["temp_air_c"]
            scaled["wind_ms"] = df["wind_ms"]

            loc_debug.emit(
                "cloudscaled.summary",
                {
                    "clearness_mean": float(clearness.mean()) if len(clearness) else None,
                    "clearness_min": float(clearness.min()) if len(clearness) else None,
                    "clearness_max": float(clearness.max()) if len(clearness) else None,
                    "ghi_max": float(scaled["ghi_wm2"].max()) if not scaled.empty else None,
                },
                ts=df.index[0] if len(df.index) else None,
            )

            results[loc_id] = scaled

        return results


__all__ = ["CloudScaledWeatherProvider", "default_cloud_to_clearness"]
