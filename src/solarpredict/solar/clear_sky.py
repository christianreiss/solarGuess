"""Clear-sky irradiance helpers (Ineichen via pvlib)."""

from __future__ import annotations

import pandas as pd
import pvlib

from solarpredict.core.debug import DebugCollector, NullDebugCollector


def clear_sky_irradiance(
    lat: float,
    lon: float,
    times: pd.DatetimeIndex,
    tz: str | None = None,
    elevation_m: float | None = None,
    model: str = "ineichen",
    debug: DebugCollector | None = None,
) -> pd.DataFrame:
    """Compute clear-sky GHI/DNI/DHI (W/m^2) for given coordinates and times.

    Parameters
    ----------
    lat, lon : float
        Site coordinates in degrees.
    times : DatetimeIndex
        Timestamps (tz-aware recommended). When a tz-naive index is passed,
        ``tz`` is used to localize; otherwise pvlib uses the index tz.
    tz : str | None
        Optional timezone to localize naive timestamps.
    elevation_m : float | None
        Site elevation meters. pvlib defaults to sea level when None.
    model : str
        pvlib clear-sky model name; default Ineichen per plan requirements.
    debug : DebugCollector
        Emits ``clearsky.summary`` with min/max GHI for traceability.
    """

    debug = debug or NullDebugCollector()

    # pvlib Location handles timezone localization; leave conversion to caller.
    location = pvlib.location.Location(latitude=lat, longitude=lon, tz=tz, altitude=elevation_m)
    cs = location.get_clearsky(times, model=model)
    cs = cs.rename(columns={"ghi": "ghi_wm2", "dni": "dni_wm2", "dhi": "dhi_wm2"})

    debug.emit(
        "clearsky.summary",
        {
            "ghi_min": float(cs["ghi_wm2"].min()) if not cs.empty else None,
            "ghi_max": float(cs["ghi_wm2"].max()) if not cs.empty else None,
        },
        ts=times[0] if len(times) else None,
    )

    return cs


__all__ = ["clear_sky_irradiance"]
