"""Incidence angle modifier helpers."""
from __future__ import annotations

import pandas as pd
import pvlib

from solarpredict.core.debug import DebugCollector, NullDebugCollector


def apply_iam(
    poa_df: pd.DataFrame,
    iam_model: str | None,
    iam_coefficient: float | None = None,
    aoi: pd.Series | None = None,
    debug: DebugCollector | None = None,
) -> pd.DataFrame:
    """Apply incidence-angle modifier to POA direct component (optional).

    Supported models:
    - None: passthrough
    - \"ashrae\": uses pvlib.iam.ashrae with coefficient b0 (default 0.05 if not given)

    We adjust only the direct beam; diffuse/ground stay untouched.
    """

    debug = debug or NullDebugCollector()
    if iam_model is None:
        return poa_df

    model = iam_model.lower()
    if model != "ashrae":
        raise ValueError("iam_model must be one of: ashrae, or None")

    b0 = iam_coefficient if iam_coefficient is not None else 0.05
    poa = poa_df.copy()
    if "poa_direct" in poa:
        if aoi is None:
            debug.emit("iam.skip", {"reason": "missing_aoi"}, ts=poa.index[0] if len(poa.index) else None)
            return poa
        iam = pvlib.iam.ashrae(aoi, b=b0).clip(lower=0.0)
        poa["poa_direct"] = poa["poa_direct"] * iam
        poa["poa_global"] = poa["poa_direct"] + poa.get("poa_diffuse", 0) + poa.get("poa_ground_diffuse", 0)
        debug.emit(
            "iam.applied",
            {"model": "ashrae", "b0": float(b0), "iam_min": float(iam.min()), "iam_max": float(iam.max())},
            ts=poa.index[0] if len(poa.index) else None,
        )
    return poa


__all__ = ["apply_iam"]
