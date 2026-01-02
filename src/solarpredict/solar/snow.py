"""Snow cover loss modeling for PV arrays."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector


_SNOW_DEPTH_CLEAR_CM = 0.5
_SNOW_DEPTH_FULL_CM = 5.0
_SNOW_LOSS_MAX = 0.7
_SNOW_TEMP_C = 0.0
_SNOW_CM_PER_MM = 1.0


@dataclass(frozen=True)
class SnowLossResult:
    factor: pd.Series
    depth_cm: pd.Series
    coverage: pd.Series
    source: str


def snow_cover_loss(weather: pd.DataFrame, *, debug: DebugCollector | None = None) -> SnowLossResult:
    """Compute snow cover loss factor from weather inputs.

    Uses snow depth when available; otherwise approximates depth from snowfall
    or cold precipitation (scaled by a fixed cm/mm ratio). Depth is mapped to a
    linear coverage fraction between fixed clear/full thresholds.
    """

    debug = debug or NullDebugCollector()
    index = weather.index

    source = "none"
    depth = None
    if "snow_depth_cm" in weather.columns:
        depth = weather["snow_depth_cm"].astype(float).fillna(0.0)
        source = "snow_depth_cm"
    else:
        snowfall_cm = None
        if "snowfall_cm" in weather.columns:
            snowfall_cm = weather["snowfall_cm"].astype(float).fillna(0.0)
            source = "snowfall_cm"
        elif "precip_mm" in weather.columns:
            precip_mm = weather["precip_mm"].astype(float).fillna(0.0)
            if "temp_air_c" in weather.columns:
                snowfall_cm = precip_mm.where(weather["temp_air_c"] <= _SNOW_TEMP_C, 0.0) * _SNOW_CM_PER_MM
                source = "precip_mm_below_temp"
            else:
                snowfall_cm = precip_mm * _SNOW_CM_PER_MM
                source = "precip_mm"

        if snowfall_cm is not None:
            depth = snowfall_cm.clip(lower=0.0).cumsum()

    if depth is None:
        depth = pd.Series(0.0, index=index)
        source = "none"

    depth = depth.astype(float).clip(lower=0.0)

    if _SNOW_DEPTH_FULL_CM <= _SNOW_DEPTH_CLEAR_CM:
        coverage = (depth >= _SNOW_DEPTH_FULL_CM).astype(float)
    else:
        coverage = ((depth - _SNOW_DEPTH_CLEAR_CM) / (_SNOW_DEPTH_FULL_CM - _SNOW_DEPTH_CLEAR_CM)).clip(
            lower=0.0, upper=1.0
        )

    loss = (coverage * _SNOW_LOSS_MAX).clip(lower=0.0, upper=1.0)
    factor = (1.0 - loss).clip(lower=0.0, upper=1.0)
    factor.name = "snow_loss_factor"
    depth.name = "snow_depth_cm"
    coverage.name = "snow_coverage"

    ts = index[0] if len(index) else None
    debug.emit(
        "snow.loss.summary",
        {
            "depth_source": source,
            "depth_clear_cm": float(_SNOW_DEPTH_CLEAR_CM),
            "depth_full_cm": float(_SNOW_DEPTH_FULL_CM),
            "max_loss": float(_SNOW_LOSS_MAX),
            "snow_temp_c": float(_SNOW_TEMP_C),
            "snow_cm_per_mm": float(_SNOW_CM_PER_MM),
            "depth_min_cm": float(depth.min()) if len(depth) else None,
            "depth_max_cm": float(depth.max()) if len(depth) else None,
            "coverage_mean": float(coverage.mean()) if len(coverage) else None,
            "loss_max": float(loss.max()) if len(loss) else None,
        },
        ts=ts,
    )

    return SnowLossResult(factor=factor, depth_cm=depth, coverage=coverage, source=source)


__all__ = ["SnowLossResult", "snow_cover_loss"]
