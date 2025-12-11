"""Irradiance decomposition helpers (GHI -> DNI/DHI)."""
from __future__ import annotations

import pandas as pd
import pvlib

from solarpredict.core.debug import DebugCollector, NullDebugCollector


def fill_dni_dhi(
    ghi_wm2: pd.Series,
    solar_zenith_deg: pd.Series,
    dni_wm2: pd.Series | None = None,
    dhi_wm2: pd.Series | None = None,
    method: str = "erbs",
    debug: DebugCollector | None = None,
) -> tuple[pd.Series, pd.Series]:
    """Ensure DNI/DHI exist, deriving from GHI + zenith when absent.

    - If both DNI and DHI are present (non-null for most rows), they are returned unchanged.
    - If either is missing, uses pvlib decomposition (default Erbs) to compute both.
    - Negative outputs are clipped to zero for determinism.
    """

    debug = debug or NullDebugCollector()
    ghi = ghi_wm2.astype(float)
    zenith = solar_zenith_deg.astype(float)

    have_dni = dni_wm2 is not None and not dni_wm2.isna().all()
    have_dhi = dhi_wm2 is not None and not dhi_wm2.isna().all()

    if have_dni and have_dhi:
        return dni_wm2.clip(lower=0), dhi_wm2.clip(lower=0)

    if method.lower() != "erbs":
        raise ValueError("Only 'erbs' decomposition is supported currently")

    # pvlib expects zenith in degrees.
    dec = pvlib.irradiance.erbs(ghi, zenith, ghi.index)
    dni = dec["dni"].clip(lower=0)
    dhi = dec["dhi"].clip(lower=0)

    payload = {
        "method": method,
        "filled_dni": int(not have_dni),
        "filled_dhi": int(not have_dhi),
        "ghi_min": float(ghi.min()) if not ghi.empty else None,
        "ghi_max": float(ghi.max()) if not ghi.empty else None,
    }
    ts0 = ghi.index[0] if len(ghi.index) else None
    debug.emit("weather.decompose", payload, ts=ts0)
    return dni, dhi


__all__ = ["fill_dni_dhi"]
