"""Shared CLI helpers to avoid circular imports."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import yaml

from solarpredict.core.config import ConfigError, load_scenario
from solarpredict.core.models import Location, PVArray, Scenario, Site


def scenario_to_dict(scenario: Scenario) -> dict:
    def loc_dict(loc: Location) -> dict:
        return {
            "id": loc.id,
            "lat": loc.lat,
            "lon": loc.lon,
            "tz": loc.tz,
            "elevation_m": loc.elevation_m,
        }

    def arr_dict(arr: PVArray) -> dict:
        data = {
            "id": arr.id,
            "tilt_deg": arr.tilt_deg,
            "azimuth_deg": arr.azimuth_deg,
            "pdc0_w": arr.pdc0_w,
            "gamma_pdc": arr.gamma_pdc,
            "dc_ac_ratio": arr.dc_ac_ratio,
            "eta_inv_nom": arr.eta_inv_nom,
            "losses_percent": arr.losses_percent,
            "temp_model": arr.temp_model,
        }
        if arr.inverter_group_id is not None:
            data["inverter_group_id"] = arr.inverter_group_id
        if arr.inverter_pdc0_w is not None:
            data["inverter_pdc0_w"] = arr.inverter_pdc0_w
        return data

    return {
        "sites": [
            {
                "id": site.id,
                "location": loc_dict(site.location),
                "arrays": [arr_dict(arr) for arr in site.arrays],
            }
            for site in scenario.sites
        ]
    }


def write_scenario(path: Path, scenario: Scenario) -> None:
    data = scenario_to_dict(scenario)
    if path.suffix.lower() in {".yaml", ".yml", ""}:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(data, sort_keys=False))
    elif path.suffix.lower() == ".json":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, sort_keys=False))
    else:
        raise ConfigError(f"Unsupported config extension: {path.suffix}")


def load_existing(path: Path) -> List[Site]:
    if not path.exists():
        return []
    scenario = load_scenario(path)
    return list(scenario.sites)


__all__ = ["scenario_to_dict", "write_scenario", "load_existing"]
