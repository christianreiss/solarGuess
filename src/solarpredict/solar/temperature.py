"""Cell temperature models built on pvlib."""
from __future__ import annotations

import pandas as pd
import pvlib

from solarpredict.core.debug import DebugCollector, NullDebugCollector


def cell_temperature(
    poa_global: pd.Series,
    temp_air_c: pd.Series,
    wind_ms: pd.Series,
    model: str = "sapm",
    mounting: str = "open_rack_glass_glass",
    debug: DebugCollector | None = None,
) -> pd.Series:
    """Estimate cell temperature given POA irradiance, ambient temp and wind speed."""

    debug = debug or NullDebugCollector()

    if model != "sapm":
        raise ValueError("Only sapm temperature model is supported at present")

    try:
        params = pvlib.temperature.TEMPERATURE_MODEL_PARAMETERS["sapm"][mounting]
    except KeyError as exc:
        available = list(pvlib.temperature.TEMPERATURE_MODEL_PARAMETERS["sapm"].keys())
        raise ValueError(
            f"Unsupported mounting configuration: {mounting}; choose from {available}"
        ) from exc

    temp_cell = pvlib.temperature.sapm_cell(
        poa_global=poa_global,
        temp_air=temp_air_c,
        wind_speed=wind_ms,
        **params,
    )

    _emit_params(debug, params, poa_global.index[0] if not poa_global.empty else None)
    _emit_summary(debug, temp_cell)
    return pd.Series(temp_cell, index=poa_global.index, name="temp_cell_c")


def _emit_params(debug: DebugCollector, params: dict, ts) -> None:
    debug.emit("temp_model.params", {k: float(v) for k, v in params.items()}, ts=ts)


def _emit_summary(debug: DebugCollector, temp_cell: pd.Series) -> None:
    payload = {
        "temp_cell_min": float(temp_cell.min()) if not temp_cell.empty else 0.0,
        "temp_cell_max": float(temp_cell.max()) if not temp_cell.empty else 0.0,
    }
    ts = temp_cell.index[0] if not temp_cell.empty else None
    debug.emit("temp_cell.summary", payload, ts=ts)


__all__ = ["cell_temperature"]
