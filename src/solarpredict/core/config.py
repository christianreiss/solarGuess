"""Configuration loader for scenarios.

Supports YAML and JSON files containing serialized Scenario structures.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

try:
    import yaml
except ModuleNotFoundError as exc:  # pragma: no cover - defensive import
    raise ImportError("PyYAML is required to load YAML configs") from exc

from .models import Location, PVArray, Scenario, Site, ValidationError


class ConfigError(ValueError):
    """Raised when configuration cannot be parsed into domain models."""


_DEF_REQUIRED_SITE_KEYS = {"id", "location", "arrays"}
_DEF_REQUIRED_ARRAY_KEYS = {
    "id",
    "tilt_deg",
    "azimuth_deg",
    "pdc0_w",
    "gamma_pdc",
    "dc_ac_ratio",
    "eta_inv_nom",
    "losses_percent",
    "temp_model",
}
_OPTIONAL_ARRAY_KEYS = {"horizon_deg"}


def _load_raw(path: Path) -> Dict[str, Any]:
    text = path.read_text()
    if path.suffix.lower() in {".yaml", ".yml"}:
        return yaml.safe_load(text) or {}
    if path.suffix.lower() == ".json":
        return json.loads(text)
    raise ConfigError(f"Unsupported config extension: {path.suffix}")


def _parse_location(raw: Dict[str, Any]) -> Location:
    try:
        return Location(
            id=raw["id"],
            lat=float(raw["lat"]),
            lon=float(raw["lon"]),
            tz=raw.get("tz", "auto"),
            elevation_m=raw.get("elevation_m"),
        )
    except KeyError as exc:
        raise ConfigError(f"Missing location field: {exc}") from exc
    except ValidationError as exc:
        raise ConfigError(f"Invalid location: {exc}") from exc


def _parse_array(raw: Dict[str, Any]) -> PVArray:
    missing = _DEF_REQUIRED_ARRAY_KEYS - raw.keys()
    if missing:
        raise ConfigError(f"Missing array fields: {sorted(missing)}")
    try:
        horizon_raw = raw.get("horizon_deg")
        if horizon_raw is None:
            horizon = None
        elif isinstance(horizon_raw, str):
            horizon = [float(x.strip()) for x in horizon_raw.split(",") if x.strip()]
        elif isinstance(horizon_raw, (list, tuple)):
            horizon = [float(x) for x in horizon_raw]
        else:
            raise ConfigError("horizon_deg must be string CSV or list of numbers")
        return PVArray(
            id=raw["id"],
            tilt_deg=float(raw["tilt_deg"]),
            azimuth_deg=float(raw["azimuth_deg"]),
            pdc0_w=float(raw["pdc0_w"]),
            gamma_pdc=float(raw["gamma_pdc"]),
            dc_ac_ratio=float(raw["dc_ac_ratio"]),
            eta_inv_nom=float(raw["eta_inv_nom"]),
            losses_percent=float(raw["losses_percent"]),
            temp_model=raw["temp_model"],
            inverter_group_id=raw.get("inverter_group_id"),
            inverter_pdc0_w=float(raw["inverter_pdc0_w"]) if raw.get("inverter_pdc0_w") is not None else None,
            horizon_deg=horizon,
        )
    except ValidationError as exc:
        raise ConfigError(f"Invalid PVArray: {exc}") from exc


def _parse_site(raw: Dict[str, Any]) -> Site:
    missing = _DEF_REQUIRED_SITE_KEYS - raw.keys()
    if missing:
        raise ConfigError(f"Missing site fields: {sorted(missing)}")
    try:
        location = _parse_location(raw["location"])
        arrays = [_parse_array(arr) for arr in raw.get("arrays", [])]
        return Site(id=raw["id"], location=location, arrays=arrays)
    except ValidationError as exc:
        raise ConfigError(f"Invalid site: {exc}") from exc


def load_scenario(path: str | Path) -> Scenario:
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    raw = _load_raw(path)
    if "sites" not in raw:
        raise ConfigError("Config must contain 'sites' list")
    try:
        sites = [_parse_site(site) for site in raw["sites"]]
        return Scenario(sites=sites)
    except ValidationError as exc:
        raise ConfigError(f"Invalid scenario: {exc}") from exc


__all__ = [
    "ConfigError",
    "load_scenario",
]
