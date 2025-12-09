"""PV power models built on pvlib PVWatts DC + inverter models.

Provides small wrappers that keep naming consistent across the project and emit
summary debug events for auditability.
"""
from __future__ import annotations

import pandas as pd
import pvlib

from solarpredict.core.debug import DebugCollector, NullDebugCollector


def pvwatts_dc(
    effective_irradiance: pd.Series,
    temp_cell: pd.Series,
    pdc0_w: float,
    gamma_pdc: float,
    temp_ref_c: float = 25.0,
    debug: DebugCollector | None = None,
) -> pd.Series:
    """Compute DC power using PVWatts DC model.

    Wraps :func:`pvlib.pvsystem.pvwatts_dc` to keep column naming consistent and
    add debug summaries. Inputs are Series aligned on the same DateTimeIndex.
    """

    debug = debug or NullDebugCollector()

    pdc = pvlib.pvsystem.pvwatts_dc(
        effective_irradiance=effective_irradiance,
        temp_cell=temp_cell,
        pdc0=pdc0_w,
        gamma_pdc=gamma_pdc,
        temp_ref=temp_ref_c,
    )

    _emit_dc_summary(debug, pdc)
    return pd.Series(pdc, index=effective_irradiance.index, name="pdc_w")


def pvwatts_ac(
    pdc_w: pd.Series,
    pdc0_inv_w: float,
    eta_inv_nom: float = 0.96,
    debug: DebugCollector | None = None,
) -> pd.Series:
    """Convert DC power to AC using PVWatts inverter model.

    ``pdc0_inv_w`` is the inverter's DC input limit. AC output is clipped at the
    corresponding AC rating (``pac0 = eta_inv_nom * pdc0_inv_w``).
    """

    debug = debug or NullDebugCollector()

    pac = pvlib.inverter.pvwatts(pdc=pdc_w, pdc0=pdc0_inv_w, eta_inv_nom=eta_inv_nom)
    pac_series = pd.Series(pac, index=pdc_w.index, name="pac_w")
    _emit_ac_summary(debug, pac_series, pdc0_inv_w, eta_inv_nom)
    return pac_series


def apply_losses(p_ac_w: pd.Series, losses_percent: float, debug: DebugCollector | None = None) -> pd.Series:
    """Apply lump-sum system losses to AC power.

    ``losses_percent`` is interpreted as percentage (0-100)."""

    debug = debug or NullDebugCollector()
    factor = max(0.0, 1.0 - losses_percent / 100.0)
    out = p_ac_w * factor
    _emit_losses_summary(debug, losses_percent, out)
    return pd.Series(out, index=p_ac_w.index, name="pac_net_w")


def inverter_pdc0_from_dc_ac_ratio(pdc0_w: float, dc_ac_ratio: float, eta_inv_nom: float = 0.96) -> float:
    """Compute inverter DC input rating from array STC rating and DC/AC ratio.

    PVWatts defines DC/AC ratio as ``pdc0_array / pac0_ac``. Rearranging gives
    ``pdc0_inv = pdc0_array / dc_ac_ratio / eta_inv_nom`` because ``pac0 =
    eta_inv_nom * pdc0_inv``.
    """

    if dc_ac_ratio <= 0:
        raise ValueError("dc_ac_ratio must be positive")
    if not (0 < eta_inv_nom <= 1):
        raise ValueError("eta_inv_nom must be in (0, 1]")

    return pdc0_w / dc_ac_ratio / eta_inv_nom


def _emit_dc_summary(debug: DebugCollector, pdc: pd.Series) -> None:
    payload = {
        "pdc_max_w": float(pdc.max()) if not pdc.empty else 0.0,
        "pdc_min": float(pdc.min()) if not pdc.empty else 0.0,
        "pdc_max": float(pdc.max()) if not pdc.empty else 0.0,
    }
    ts = pdc.index[0] if not pdc.empty else None
    debug.emit("pv.dc.summary", payload, ts=ts)


def _emit_ac_summary(debug: DebugCollector, pac: pd.Series, pdc0_inv_w: float, eta_inv_nom: float) -> None:
    payload = {
        "pac0_w": float(eta_inv_nom * pdc0_inv_w),
        "pac_min": float(pac.min()) if not pac.empty else 0.0,
        "pac_max": float(pac.max()) if not pac.empty else 0.0,
    }
    ts = pac.index[0] if not pac.empty else None
    debug.emit("pv.ac.summary", payload, ts=ts)


def _emit_losses_summary(debug: DebugCollector, losses_percent: float, pac_net: pd.Series) -> None:
    payload = {
        "losses_percent": float(losses_percent),
        "pac_net_min": float(pac_net.min()) if not pac_net.empty else 0.0,
        "pac_net_max": float(pac_net.max()) if not pac_net.empty else 0.0,
    }
    ts = pac_net.index[0] if not pac_net.empty else None
    debug.emit("pv.losses.summary", payload, ts=ts)


__all__ = [
    "pvwatts_dc",
    "pvwatts_ac",
    "apply_losses",
    "inverter_pdc0_from_dc_ac_ratio",
]
