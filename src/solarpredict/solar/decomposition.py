"""Irradiance decomposition helpers (GHI -> DNI/DHI)."""
from __future__ import annotations

import numpy as np
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
    - If exactly one component is missing, fill it by closure (GHI = DHI + DNI*cos(zenith)).
    - If both are missing, uses pvlib decomposition (default Erbs).
    - Negative outputs are clipped to zero for determinism.
    """

    debug = debug or NullDebugCollector()
    ghi = ghi_wm2.astype(float)
    zenith = solar_zenith_deg.astype(float).reindex(ghi.index)

    have_dni = dni_wm2 is not None and not dni_wm2.isna().all()
    have_dhi = dhi_wm2 is not None and not dhi_wm2.isna().all()

    if have_dni and have_dhi and (not dni_wm2.isna().any()) and (not dhi_wm2.isna().any()):
        return dni_wm2.clip(lower=0), dhi_wm2.clip(lower=0)

    dni = dni_wm2.astype(float).copy() if dni_wm2 is not None else pd.Series(index=ghi.index, dtype=float)
    dhi = dhi_wm2.astype(float).copy() if dhi_wm2 is not None else pd.Series(index=ghi.index, dtype=float)
    dni = dni.reindex(ghi.index)
    dhi = dhi.reindex(ghi.index)
    dni = dni.clip(lower=0)
    dhi = dhi.clip(lower=0)

    if method.lower() != "erbs":
        raise ValueError("Only 'erbs' decomposition is supported currently")

    cosz = pd.Series(np.cos(np.deg2rad(zenith.astype(float))), index=ghi.index).astype(float)
    cosz = cosz.where(cosz > 1e-6, 0.0)

    filled_closure_dni = 0
    filled_closure_dhi = 0
    filled_erbs = 0

    # Fill per-row by closure when exactly one component is missing.
    missing_dni = dni.isna()
    missing_dhi = dhi.isna()
    if have_dhi and missing_dni.any():
        mask = missing_dni & (~missing_dhi)
        if mask.any():
            dni.loc[mask] = ((ghi.loc[mask] - dhi.loc[mask]) / cosz.loc[mask]).where(cosz.loc[mask] > 0, 0.0)
            filled_closure_dni = int(mask.sum())
    if have_dni and missing_dhi.any():
        mask = missing_dhi & (~missing_dni)
        if mask.any():
            dhi.loc[mask] = (ghi.loc[mask] - dni.loc[mask] * cosz.loc[mask])
            filled_closure_dhi = int(mask.sum())

    dni = dni.clip(lower=0)
    dhi = dhi.clip(lower=0)

    # Remaining rows (both missing or unfilled) fall back to Erbs.
    need_erbs = dni.isna() | dhi.isna()
    if need_erbs.any():
        dec = pvlib.irradiance.erbs(ghi, zenith, ghi.index)
        dni_erbs = dec["dni"].clip(lower=0)
        dhi_erbs = dec["dhi"].clip(lower=0)
        if dni.isna().any():
            dni = dni.where(~dni.isna(), dni_erbs)
        if dhi.isna().any():
            dhi = dhi.where(~dhi.isna(), dhi_erbs)
        filled_erbs = int(need_erbs.sum())

    payload = {
        "method": method,
        "filled_dni": int((not have_dni) or filled_closure_dni > 0 or filled_erbs > 0),
        "filled_dhi": int((not have_dhi) or filled_closure_dhi > 0 or filled_erbs > 0),
        "filled_closure_dni_rows": filled_closure_dni,
        "filled_closure_dhi_rows": filled_closure_dhi,
        "filled_erbs_rows": filled_erbs,
        "ghi_min": float(ghi.min()) if not ghi.empty else None,
        "ghi_max": float(ghi.max()) if not ghi.empty else None,
    }
    ts0 = ghi.index[0] if len(ghi.index) else None
    debug.emit("weather.decompose", payload, ts=ts0)
    return dni, dhi


__all__ = ["fill_dni_dhi"]
