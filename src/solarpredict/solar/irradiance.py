"""Plane-of-array irradiance helpers built on pvlib."""
from __future__ import annotations

import pandas as pd
import pvlib

from solarpredict.core.debug import DebugCollector, NullDebugCollector
from solarpredict.solar.incidence import apply_iam


# Numerical noise from pvlib or inconsistent inputs can yield tiny negatives; zero them out.
_NEG_EPS = 1e-6

def _interp_horizon(horizon_deg: list[float], solar_azimuth: pd.Series) -> pd.Series:
    """Interpolate a circular horizon profile (deg) to per-sample azimuth.

    horizon_deg is N evenly spaced samples starting at 0° (north) increasing clockwise.
    """
    n = len(horizon_deg)
    if n == 0:
        return pd.Series([0.0] * len(solar_azimuth), index=solar_azimuth.index)
    az = solar_azimuth % 360.0
    # Build a repeated array to avoid edge discontinuity; use lower bin then linear blend to next bin.
    step = 360.0 / n
    lower_idx = (az // step).astype(int)
    upper_idx = (lower_idx + 1) % n
    frac = (az - lower_idx * step) / step
    lower_vals = [horizon_deg[i] for i in lower_idx]
    upper_vals = [horizon_deg[i] for i in upper_idx]
    # Handle wrap between last bin and first: ensure circular interpolation
    blended = []
    for lo, up, fr in zip(lower_vals, upper_vals, frac):
        # if crossing boundary 330→0 etc, still linear in circular sense
        diff = up - lo
        # detect wrap and adjust diff to shortest path
        if diff > 90:  # e.g., lo=350, up=0 (after normalization)
            diff -= 360
        if diff < -90:  # unlikely with evenly spaced bins, but guard
            diff += 360
        blended.append(lo + fr * diff)
    return pd.Series(blended, index=az.index)


def poa_irradiance(
    surface_tilt: float,
    surface_azimuth: float,
    dni: pd.Series,
    ghi: pd.Series,
    dhi: pd.Series,
    solar_zenith: pd.Series,
    solar_azimuth: pd.Series,
    albedo: float = 0.2,
    model: str = "perez",
    horizon_deg: list[float] | None = None,
    iam_model: str | None = None,
    iam_coefficient: float | None = None,
    debug: DebugCollector | None = None,
) -> pd.DataFrame:
    """Compute plane-of-array irradiance using pvlib's total irradiance models.

    All input series must share the same DateTimeIndex. Negative POA outputs caused by
    floating-point noise are clipped to zero using `_NEG_EPS`.
    """

    debug = debug or NullDebugCollector()

    # Normalize azimuth to pvlib convention [0,360), 0=N, 180=S, positive clockwise.
    surface_azimuth = float(surface_azimuth)
    while surface_azimuth < 0:
        surface_azimuth += 360
    surface_azimuth %= 360

    # Guardrail: pvlib will error on perez without dni_extra, and negative inputs can
    # yield small positive POA; sanitize up front for determinism.
    dni = dni.clip(lower=0)
    ghi = ghi.clip(lower=0)
    dhi = dhi.clip(lower=0)

    dni_extra = pvlib.irradiance.get_extra_radiation(dni.index)

    poa = pvlib.irradiance.get_total_irradiance(
        surface_tilt=surface_tilt,
        surface_azimuth=surface_azimuth,
        dni=dni,
        ghi=ghi,
        dhi=dhi,
        dni_extra=dni_extra,
        solar_zenith=solar_zenith,
        solar_azimuth=solar_azimuth,
        albedo=albedo,
        model=model,
    )

    # Apply horizon masking: zero direct beam when sun elevation below local horizon.
    df = pd.DataFrame(poa)
    if horizon_deg is not None:
        horizon = _interp_horizon(horizon_deg, solar_azimuth).astype(float)
        solar_elev = 90.0 - solar_zenith
        mask = solar_elev < horizon
        if "poa_direct" in df:
            df.loc[mask, "poa_direct"] = 0.0
            if "poa_global" in df:
                df.loc[mask, "poa_global"] = df.loc[mask, "poa_diffuse"] + df.loc[mask, "poa_ground_diffuse"]
        blocked = int(mask.sum())
        debug.emit(
            "poa.horizon_mask",
            {"blocked_samples": blocked, "total": len(mask), "blocked_pct": (blocked / len(mask)) if len(mask) else 0.0},
            ts=df.index[0] if not df.empty else None,
        )

    # Ensure deterministic column order and clip negatives.
    columns = ["poa_global", "poa_direct", "poa_diffuse", "poa_ground_diffuse"]
    df = pd.DataFrame({col: df.get(col) for col in columns}, index=dni.index)
    aoi = poa.get("aoi") if isinstance(poa, dict) else None
    df = apply_iam(df, iam_model=iam_model, iam_coefficient=iam_coefficient, aoi=aoi, debug=debug)
    df = df.apply(lambda s: s.where(s > -_NEG_EPS, 0.0).clip(lower=0.0))

    _emit_summary(debug, df)
    return df


def _emit_summary(debug: DebugCollector, df: pd.DataFrame) -> None:
    if df.empty:
        debug.emit("poa.summary", {"poa_wh_m2": 0.0, "poa_global_max": 0.0}, ts=None)
        return

    # Infer timestep to integrate energy (Wh/m^2).
    if len(df.index) > 1:
        deltas = df.index.to_series().diff().dt.total_seconds().dropna()
        step_seconds = float(deltas.median()) if not deltas.empty else 0.0
    else:
        step_seconds = 0.0
    energy_wh_m2 = float((df["poa_global"] * (step_seconds / 3600.0)).sum()) if step_seconds > 0 else 0.0

    payload = {
        "poa_wh_m2": energy_wh_m2,
        "poa_global_max": float(df["poa_global"].max()),
    }
    debug.emit("poa.summary", payload, ts=df.index[0])


__all__ = ["poa_irradiance"]
