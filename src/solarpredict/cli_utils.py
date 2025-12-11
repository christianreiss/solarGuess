"""Shared CLI helpers to avoid circular imports."""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List

import yaml

from solarpredict.core.config import ConfigError, load_scenario, _load_raw
from solarpredict.core.models import Location, PVArray, Scenario, Site


def scenario_to_dict(scenario: Scenario) -> dict:
    """Convert Scenario dataclass tree into plain dict."""
    return {"sites": [asdict(site) for site in scenario.sites]}


def load_existing(path: Path) -> List[Site]:
    if not path.exists():
        return []
    scenario = load_scenario(path)
    return list(scenario.sites)


__all__ = ["scenario_to_dict", "load_existing"]
