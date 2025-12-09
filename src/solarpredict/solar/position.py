"""Solar position utilities built on pvlib."""
from __future__ import annotations

import pandas as pd
import pvlib

from solarpredict.core.debug import DebugCollector, NullDebugCollector
from solarpredict.core.models import Location


def solar_position(
    location: Location,
    times: pd.DatetimeIndex,
    debug: DebugCollector | None = None,
) -> pd.DataFrame:
    """Compute solar position for a location at the given times.

    Parameters
    ----------
    location: Location
        Site location with latitude/longitude and optional elevation.
    times: pandas.DatetimeIndex
        Must be timezone-aware. pvlib assumes UTC if naive, which would be wrong for local times.
    debug: DebugCollector | None
        Collector for summary debug info.

    Returns
    -------
    pandas.DataFrame
        Columns include at least zenith, elevation, azimuth as provided by pvlib.
    """
    if times.tz is None:
        raise ValueError("times must be timezone-aware (tzinfo set)")

    debug = debug or NullDebugCollector()

    pvloc = pvlib.location.Location(latitude=location.lat, longitude=location.lon, tz=times.tz, altitude=location.elevation_m)
    df = pvloc.get_solarposition(times)

    # Standardize key columns for downstream expectations
    expected_cols = ["zenith", "elevation", "azimuth"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"pvlib missing expected columns: {missing}")

    _emit_summary(debug, df, location.id)
    return df[expected_cols + [c for c in df.columns if c not in expected_cols]]


def _emit_summary(debug: DebugCollector, df: pd.DataFrame, site_id: str | None) -> None:
    payload = {
        "elevation_min": float(df["elevation"].min()),
        "elevation_max": float(df["elevation"].max()),
        "zenith_min": float(df["zenith"].min()),
        "zenith_max": float(df["zenith"].max()),
        "has_nans": bool(df.isna().any().any()),
    }
    ts0 = df.index[0] if not df.empty else None
    debug.emit("solar_position.summary", payload, ts=ts0, site=site_id)


__all__ = ["solar_position"]
