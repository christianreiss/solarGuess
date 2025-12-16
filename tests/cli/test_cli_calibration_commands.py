import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pytest
import typer
import yaml

from solarpredict import cli
from solarpredict.calibration.ha_tune import CalibrationGroup


def _write_base_config(tmp_path: Path, *, arrays: list[dict], run_section: str = "") -> Path:
    cfg = tmp_path / "config.yaml"
    base = [
        "sites:",
        "- id: site1",
        "  location:",
        "    id: loc1",
        "    lat: 0",
        "    lon: 0",
        "    tz: UTC",
        "  arrays:",
    ]
    for arr in arrays:
        base.extend(
            [
                "  - id: {id}".format(**arr),
                "    tilt_deg: {tilt_deg}".format(**arr),
                "    azimuth_deg: {azimuth_deg}".format(**arr),
                "    pdc0_w: {pdc0_w}".format(**arr),
                "    gamma_pdc: {gamma_pdc}".format(**arr),
                "    dc_ac_ratio: {dc_ac_ratio}".format(**arr),
                "    eta_inv_nom: {eta_inv_nom}".format(**arr),
                "    losses_percent: {losses_percent}".format(**arr),
                "    temp_model: {temp_model}".format(**arr),
            ]
        )
    if run_section:
        base.append("run:")
        base.extend([f"  {line}" for line in run_section.strip().splitlines() if line.strip()])
    cfg.write_text("\n".join(base) + "\n")
    return cfg


class _FakeResult:
    def __init__(self, energy: float, *, arrays: list[str] | None = None):
        arrays = arrays or ["arr1"]
        rows = []
        for arr in arrays:
            rows.append({"site": "site1", "array": arr, "energy_kwh": energy})
        self.daily = pd.DataFrame(rows)
        self.timeseries = {}


def _fake_weather_payload(locations, start, end, timestep):
    idx = pd.date_range(start, end, freq=timestep, tz="UTC", inclusive="left")
    frame = pd.DataFrame(
        {
            "ghi_wm2": 800.0,
            "dni_wm2": 600.0,
            "dhi_wm2": 200.0,
            "temp_air_c": 20.0,
            "wind_ms": 1.0,
        },
        index=idx,
    )
    return {loc["id"]: frame for loc in locations}


def test_ha_compare_generates_outputs(monkeypatch, tmp_path):
    cfg = _write_base_config(
        tmp_path,
        arrays=[
            {
                "id": "arr1",
                "tilt_deg": 30,
                "azimuth_deg": 0,
                "pdc0_w": 4000,
                "gamma_pdc": -0.004,
                "dc_ac_ratio": 1.1,
                "eta_inv_nom": 0.96,
                "losses_percent": 5,
                "temp_model": "close_mount_glass_glass",
            }
        ],
        run_section="""
scale_factor: 1.1
array_scale_factors:
  arr1: 1.05
        """,
    )

    class FakeExport:
        sensors = ["sensor.total_pv_energy_today"]

        def to_frame(self, *, entities=None, debug=None):
            return pd.DataFrame(
                [
                    {"entity_id": "sensor.total_pv_energy_today", "day": dt.date(2025, 12, 1), "energy_kwh": 10.0},
                ]
            )

    monkeypatch.setattr(cli.HaDailyMaxExport, "from_path", staticmethod(lambda path, debug=None: FakeExport()))
    monkeypatch.setattr(cli, "default_weather_provider", lambda debug: type("FakeWeather", (), {"get_forecast": staticmethod(_fake_weather_payload)})())
    called: dict[str, object] = {}

    def fake_simulate_day(*args, **kwargs):
        called["simulate_day"] = True
        return _FakeResult(energy=5.0)

    def fake_apply_output_scale(result, scale, debug=None):
        called["output_scale"] = scale
        return result

    def fake_apply_array_scale_factors(result, factors, debug=None):
        called["array_scale_factors"] = dict(factors)
        return result

    monkeypatch.setattr(cli, "simulate_day", fake_simulate_day)
    monkeypatch.setattr(cli, "apply_output_scale", fake_apply_output_scale)
    monkeypatch.setattr(cli, "apply_array_scale_factors", fake_apply_array_scale_factors)

    tuned_cfg = tmp_path / "tuned.json"
    rows_csv = tmp_path / "rows.csv"
    debug_path = tmp_path / "debug.json"

    cli.ha_compare(
        config=cfg,
        ha_export=tmp_path / "ha.json",
        entity_id="sensor.total_pv_energy_today",
        start="2025-12-01",
        end="2025-12-01",
        timestep="1h",
        weather_label="end",
        weather_source="open-meteo",
        weather_mode=None,
        scale_factor=None,
        min_actual_kwh=0.1,
        min_pred_kwh=0.1,
        write_config=tuned_cfg,
        out=rows_csv,
        debug=debug_path,
    )

    assert called["simulate_day"] is True
    assert called["output_scale"] == 1.1
    assert called["array_scale_factors"] == {"arr1": 1.05}
    assert rows_csv.exists()
    data = json.loads(tuned_cfg.read_text())
    assert data["run"]["scale_factor"] > 1.1
    assert debug_path.exists()
    debug_events = json.loads(debug_path.read_text())
    assert isinstance(debug_events, list)
    assert len(debug_events) > 0


def test_ha_compare_validates_array_scale_type(tmp_path):
    cfg = _write_base_config(
        tmp_path,
        arrays=[
            {
                "id": "arr1",
                "tilt_deg": 30,
                "azimuth_deg": 0,
                "pdc0_w": 4000,
                "gamma_pdc": -0.004,
                "dc_ac_ratio": 1.1,
                "eta_inv_nom": 0.96,
                "losses_percent": 5,
                "temp_model": "close_mount_glass_glass",
            }
        ],
        run_section="array_scale_factors: []",
    )
    ha_export = tmp_path / "ha.json"
    ha_export.write_text("{}")

    with pytest.raises(typer.Exit) as excinfo:
        cli.ha_compare(
            config=cfg,
            ha_export=ha_export,
            entity_id="sensor.total_pv_energy_today",
            start="2025-12-01",
            end="2025-12-01",
            timestep="1h",
            weather_label="end",
            weather_source="open-meteo",
            weather_mode=None,
            scale_factor=None,
            min_actual_kwh=1.0,
            min_pred_kwh=1.0,
            write_config=None,
            out=None,
            debug=None,
        )
    assert excinfo.value.exit_code == 1


def test_ha_tune_uses_weather_debug_and_writes_yaml(monkeypatch, tmp_path):
    arrays = [
        {
            "id": "north_roof",
            "tilt_deg": 30,
            "azimuth_deg": 0,
            "pdc0_w": 4000,
            "gamma_pdc": -0.004,
            "dc_ac_ratio": 1.1,
            "eta_inv_nom": 0.96,
            "losses_percent": 5,
            "temp_model": "close_mount_glass_glass",
        },
        {
            "id": "south_roof",
            "tilt_deg": 30,
            "azimuth_deg": 180,
            "pdc0_w": 4000,
            "gamma_pdc": -0.004,
            "dc_ac_ratio": 1.1,
            "eta_inv_nom": 0.96,
            "losses_percent": 5,
            "temp_model": "close_mount_glass_glass",
        },
    ]
    cfg = _write_base_config(tmp_path, arrays=arrays)

    weather_debug = tmp_path / "weather.jsonl"
    weather_debug.write_text(
        "\n".join(
            [
                json.dumps({
                    "stage": "weather.response_meta",
                    "site": "site1",
                    "payload": {"timezone": "UTC"},
                }),
                json.dumps({
                    "stage": "weather.raw",
                    "site": "site1",
                    "payload": {
                        "data": [
                            {"ts": "2025-12-01T00:00:00+00:00", "ghi_wm2": 800, "dni_wm2": 600, "dhi_wm2": 200, "temp_air_c": 20, "wind_ms": 1},
                            {"ts": "2025-12-02T00:00:00+00:00", "ghi_wm2": 500, "dni_wm2": 400, "dhi_wm2": 150, "temp_air_c": 18, "wind_ms": 2},
                        ]
                    },
                }),
            ]
        )
    )

    class FakeExport:
        sensors = [
            "sensor.north_roof_energy_today",
            "sensor.south_roof_energy_today",
        ]

        def to_frame(self, *, entities=None, debug=None):
            days = [dt.date(2025, 12, 1), dt.date(2025, 12, 2)]
            rows = []
            for day in days:
                rows.append({"entity_id": "sensor.north_roof_energy_today", "day": day, "energy_kwh": 8.0})
                rows.append({"entity_id": "sensor.south_roof_energy_today", "day": day, "energy_kwh": 12.0})
            return pd.DataFrame(rows)

    monkeypatch.setattr(cli.HaDailyMaxExport, "from_path", staticmethod(lambda path, debug=None: FakeExport()))

    groups = [
        CalibrationGroup(name="north_roof", ha_entities=["sensor.north_roof_energy_today"], arrays=[("site1", "north_roof")]),
        CalibrationGroup(name="south_roof", ha_entities=["sensor.south_roof_energy_today"], arrays=[("site1", "south_roof")]),
        CalibrationGroup(name="total_pv", ha_entities=["sensor.total_pv_energy_today"], arrays=[("site1", "north_roof"), ("site1", "south_roof")]),
    ]
    monkeypatch.setattr(cli, "auto_calibration_groups", lambda *args, **kwargs: groups)

    def fake_simulate_day(*args, **kwargs):
        return _FakeResult(energy=4.0, arrays=["north_roof", "south_roof"])

    monkeypatch.setattr(cli, "simulate_day", fake_simulate_day)

    tuned_yaml = tmp_path / "tuned.yaml"
    out_csv = tmp_path / "train.csv"
    debug_path = tmp_path / "tune.json"

    cli.ha_tune(
        config=cfg,
        ha_export=tmp_path / "ha.json",
        start="2025-12-01",
        end="2025-12-02",
        timestep="1h",
        weather_label="end",
        weather_source="open-meteo",
        weather_mode=None,
        weather_debug=weather_debug,
        min_actual_kwh=1.0,
        min_pred_kwh=0.5,
        write_config=tuned_yaml,
        out=out_csv,
        debug=debug_path,
    )

    assert tuned_yaml.exists()
    data = yaml.safe_load(tuned_yaml.read_text())
    assert data["run"]["scale_factor"] == 1.0
    assert set(data["run"]["array_scale_factors"].keys()) == {"site1/north_roof", "site1/south_roof"}
    assert out_csv.exists()
    assert debug_path.exists()
