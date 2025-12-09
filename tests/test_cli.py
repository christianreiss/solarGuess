from pathlib import Path
import json

import pandas as pd
from typer.testing import CliRunner

from solarpredict import cli
from solarpredict.core.models import Location, PVArray, Scenario, Site
from solarpredict.core.config import load_scenario


runner = CliRunner()


def _write_fixture(tmp_path: Path) -> Path:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "sites:\n"
        "- id: site1\n"
        "  location:\n"
        "    id: loc1\n"
        "    lat: 0\n"
        "    lon: 0\n"
        "    tz: UTC\n"
        "  arrays:\n"
        "  - id: arr1\n"
        "    tilt_deg: 20\n"
        "    azimuth_deg: 0\n"
        "    pdc0_w: 5000\n"
        "    gamma_pdc: -0.004\n"
        "    dc_ac_ratio: 1.2\n"
        "    eta_inv_nom: 0.96\n"
        "    losses_percent: 5.0\n"
        "    temp_model: close_mount_glass_glass\n"
    )
    return cfg


def test_help_exits_zero():
    res = runner.invoke(cli.app, ["--help"])
    assert res.exit_code == 0
    assert "run" in res.stdout
    assert "config" in res.stdout


class DummyWeatherProvider:
    def get_forecast(self, locations, start, end, timestep="1h"):
        idx = pd.date_range(start, end, freq=timestep, tz="UTC", inclusive="left")
        data = {
            "ghi_wm2": pd.Series(800.0, index=idx),
            "dni_wm2": pd.Series(500.0, index=idx),
            "dhi_wm2": pd.Series(300.0, index=idx),
            "temp_air_c": pd.Series(20.0, index=idx),
            "wind_ms": pd.Series(1.0, index=idx),
        }
        return {loc["id"]: pd.DataFrame(data, index=idx) for loc in locations}


def test_run_command_smoke(monkeypatch, tmp_path):
    cfg = _write_fixture(tmp_path)
    out_json = tmp_path / "out.json"
    debug_path = tmp_path / "debug.jsonl"

    def fake_provider(debug):
        return DummyWeatherProvider()

    monkeypatch.setattr(cli, "default_weather_provider", fake_provider)

    res = runner.invoke(
        cli.app,
        [
            "run",
            "--config",
            str(cfg),
            "--date",
            "2025-06-01",
            "--timestep",
            "1h",
            "--debug",
            str(debug_path),
            "--format",
            "json",
            "--output",
            str(out_json),
        ],
    )
    assert res.exit_code == 0, res.stdout
    data = json.loads(out_json.read_text())
    assert data[0]["site"] == "site1"
    assert debug_path.exists()
    assert debug_path.read_text().strip() != ""


def test_config_command_add_edit_delete(monkeypatch, tmp_path):
    path = tmp_path / "config.yaml"

    inputs = iter(
        [
            "a",  # add site
            "site1",
            "0",
            "0",
            "UTC",
            "",
            "loc1",
            "array1",
            "20",
            "0",
            "5000",
            "-0.004",
            "1.2",
            "0.96",
            "5",
            "close_mount_glass_glass",
            "n",  # stop modifying arrays\n
            "s",  # save\n
        ]
    )

    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(inputs))

    res = runner.invoke(cli.app, ["config", str(path)])
    assert res.exit_code == 0, res.stdout

    scenario = load_scenario(path)
    assert len(scenario.sites) == 1
    assert scenario.sites[0].arrays[0].id == "array1"
