import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from solarpredict import cli


runner = CliRunner()


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


def test_run_writes_intervals_json(monkeypatch, tmp_path):
    cfg = _write_fixture(tmp_path)
    out_json = tmp_path / "out.json"
    intervals_path = tmp_path / "intervals.json"

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
            "2025-12-01",
            "--timestep",
            "1h",
            "--format",
            "json",
            "--output",
            str(out_json),
            "--intervals",
            str(intervals_path),
        ],
    )

    assert res.exit_code == 0, res.stdout
    intervals = json.loads(intervals_path.read_text())
    assert len(intervals) > 0
    first = intervals[0]
    assert set(first.keys()) == {"site", "array", "ts", "pac_net_w", "poa_global", "interval_h", "wh_period", "wh_cum"}
    assert first["wh_period"] == first["pac_net_w"] * first["interval_h"]
    last = intervals[-1]
    assert last["wh_cum"] >= first["wh_period"]


def test_run_writes_intervals_csv(monkeypatch, tmp_path):
    cfg = _write_fixture(tmp_path)
    out_json = tmp_path / "out.json"
    intervals_path = tmp_path / "intervals.csv"

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
            "2025-12-01",
            "--timestep",
            "1h",
            "--format",
            "json",
            "--output",
            str(out_json),
            "--intervals",
            str(intervals_path),
        ],
    )

    assert res.exit_code == 0, res.stdout
    df = pd.read_csv(intervals_path)
    assert set(df.columns) == {"site", "array", "ts", "pac_net_w", "poa_global", "interval_h", "wh_period", "wh_cum"}
    assert df.iloc[-1]["wh_cum"] >= df.iloc[0]["wh_period"]
