from pathlib import Path
import json
import datetime as dt

import pandas as pd
from typer.testing import CliRunner

from solarpredict import cli
from solarpredict import cli_utils
from solarpredict import cli_utils
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


def _write_fixture_with_run(tmp_path: Path, timestep: str = "15m") -> Path:
    cfg = _write_fixture(tmp_path)
    cfg.write_text(
        cfg.read_text()
        + "\nrun:\n"
        + f"  timestep: {timestep}\n"
        + "  actual_kwh_today: 1.5\n"
        + "  actual_limit_suppress: false\n"
        + "  base_load_w: 500\n"
        + "  min_duration_min: 30\n"
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
    # load_windows absent when base_load not provided; payload is list
    assert isinstance(data, list)
    assert data[0]["site"] == "site1"
    assert debug_path.exists()
    assert debug_path.read_text().strip() != ""


def test_run_skips_when_existing_generated_today(monkeypatch, tmp_path):
    cfg = _write_fixture(tmp_path)
    out_json = tmp_path / "out.json"

    # existing output with today's generated_at
    # use local date to match guard logic that compares to dt.date.today()
    today_iso = dt.datetime.combine(dt.date.today(), dt.datetime.now().time(), dt.timezone.utc).isoformat()
    out_json.write_text(json.dumps({"meta": {"generated_at": today_iso, "date": "2025-06-01"}, "sites": []}))

    called = {"simulate": False}

    def fake_provider(debug):
        return DummyWeatherProvider()

    def fake_simulate_day(*args, **kwargs):
        called["simulate"] = True
        return type("Result", (), {"daily": pd.DataFrame(), "timeseries": {}})()

    monkeypatch.setattr(cli, "default_weather_provider", fake_provider)
    monkeypatch.setattr(cli, "simulate_day", fake_simulate_day)

    res = runner.invoke(
        cli.app,
        [
            "run",
            "--config",
            str(cfg),
            "--date",
            "2025-06-01",
            "--format",
            "json",
            "--output",
            str(out_json),
        ],
    )
    assert res.exit_code == 0
    assert "already generated today" in res.stdout
    assert called["simulate"] is False


def test_run_force_bypasses_skip(monkeypatch, tmp_path):
    cfg = _write_fixture(tmp_path)
    out_json = tmp_path / "out.json"

    today_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    out_json.write_text(json.dumps({"meta": {"generated_at": today_iso}, "sites": []}))

    called = {"simulate": False}

    def fake_provider(debug):
        return DummyWeatherProvider()

    def fake_simulate_day(*args, **kwargs):
        called["simulate"] = True
        df = pd.DataFrame([{"site": "site1", "array": "arr1", "energy_kwh": 1.0}])
        return type("Result", (), {"daily": df, "timeseries": {}})()

    monkeypatch.setattr(cli, "default_weather_provider", fake_provider)
    monkeypatch.setattr(cli, "simulate_day", fake_simulate_day)

    res = runner.invoke(
        cli.app,
        [
            "run",
            "--config",
            str(cfg),
            "--date",
            "2025-06-01",
            "--format",
            "json",
            "--output",
            str(out_json),
            "--force",
        ],
    )
    assert res.exit_code == 0, res.stdout
    assert called["simulate"] is True


def test_config_command_add_edit_delete(tmp_path):
    path = tmp_path / "config.yaml"

    input_lines = [
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
        "",  # inverter group id blank
        "",  # inverter pdc0_w blank
        "",  # horizon blank
        "n",  # Add array? prompt default False
        "n",  # Edit existing array?
        "n",  # Delete array?
        "n",  # Modify arrays again?
        "s",  # save\n
    ]

    res = runner.invoke(cli.app, ["config", "--no-tui", str(path)], input="\n".join(input_lines))
    assert res.exit_code == 0, res.stdout

    scenario = load_scenario(path)
    assert len(scenario.sites) == 1
    assert scenario.sites[0].arrays[0].id == "array1"


def test_scenario_to_dict_preserves_inverter_fields():
    arr = PVArray(
        id="a",
        tilt_deg=30,
        azimuth_deg=0,
        pdc0_w=4000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.1,
        eta_inv_nom=0.96,
        losses_percent=5.0,
        temp_model="close_mount_glass_glass",
        inverter_group_id="g1",
        inverter_pdc0_w=6500,
    )
    site = Site(id="s", location=Location(id="loc", lat=0, lon=0, tz="UTC"), arrays=[arr])
    data = cli_utils.scenario_to_dict(Scenario(sites=[site]))
    arr_out = data["sites"][0]["arrays"][0]
    assert arr_out["inverter_group_id"] == "g1"
    assert arr_out["inverter_pdc0_w"] == 6500


def test_run_uses_config_timestep_default(monkeypatch, tmp_path):
    cfg = _write_fixture_with_run(tmp_path, timestep="15m")
    out_json = tmp_path / "out.json"

    captured = {}

    def fake_simulate_day(scenario, date, timestep, weather_provider, debug, weather_label, weather_mode):
        captured["timestep"] = timestep
        df = pd.DataFrame([{"site": "site1", "array": "arr1", "energy_kwh": 1.0}])
        ts = {( "site1", "arr1"): pd.DataFrame({"pac_net_w": pd.Series([100], index=pd.date_range(date, periods=1, freq="1h", tz="UTC")), "interval_h": pd.Series([1.0], index=pd.date_range(date, periods=1, freq="1h", tz="UTC"))})}
        return type("Result", (), {"daily": df, "timeseries": ts})()

    monkeypatch.setattr(cli, "default_weather_provider", lambda debug: DummyWeatherProvider())
    monkeypatch.setattr(cli, "simulate_day", fake_simulate_day)

    res = runner.invoke(
        cli.app,
        [
            "run",
            "--config",
            str(cfg),
            "--date",
            "2025-06-01",
            "--format",
            "json",
            "--output",
            str(out_json),
        ],
    )
    assert res.exit_code == 0, res.stdout
    assert captured["timestep"] == "15m"
