from __future__ import annotations

from pathlib import Path

from solarpredict.core.config import load_scenario


def test_example_configs_load() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    example_paths = [
        repo_root / "etc" / "config.example.yaml",
        repo_root / "etc" / "examples" / "01-minimal-single-site.yaml",
        repo_root / "etc" / "examples" / "02-two-arrays-shared-inverter-shading.yaml",
        repo_root / "etc" / "examples" / "03-multi-site-composite-qc.yaml",
        repo_root / "etc" / "examples" / "04-mqtt-homeassistant-env.yaml",
    ]

    for path in example_paths:
        assert path.exists(), f"Missing example config: {path}"
        scenario = load_scenario(path)
        assert scenario.sites, f"{path} parsed but produced no sites"
        for site in scenario.sites:
            assert site.arrays, f"{path}: site {site.id!r} has no arrays"

