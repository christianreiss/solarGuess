import datetime as dt
import json

import pytest


def _write_min_config(path, *, qc_pvgis: bool):
    path.write_text(
        "\n".join(
            [
                "sites:",
                "- id: s",
                "  location: {id: s, lat: 0.0, lon: 0.0, tz: UTC}",
                "  arrays:",
                "  - {id: a, tilt_deg: 30, azimuth_deg: 0, pdc0_w: 1000, gamma_pdc: -0.004, dc_ac_ratio: 1.1, eta_inv_nom: 0.96, losses_percent: 0, temp_model: close_mount_glass_glass}",
                "run:",
                f"  qc_pvgis: {'true' if qc_pvgis else 'false'}",
            ]
        )
        + "\n"
    )


class _DummyPVGISProvider:
    def __init__(self, *args, **kwargs):
        pass


def test_pvgis_qc_cloudy_band_does_not_clip_within_wide_range(tmp_path, monkeypatch):
    # Forecast POA is "cloudy" (<0.6 kWh/m²/day). If ratio is between [0.3, 2.0],
    # it should not be clipped (even if it's outside the clear-sky [0.6, 1.6] band).
    from solarpredict import cli as cli_mod

    config = tmp_path / "cfg.yaml"
    _write_min_config(config, qc_pvgis=True)

    monkeypatch.setattr(cli_mod, "PVGISWeatherProvider", _DummyPVGISProvider)

    def _fake_simulate_day(*args, **kwargs):
        import pandas as pd
        from solarpredict.engine.simulate import SimulationResult

        is_pvgis = isinstance(kwargs.get("weather_provider"), _DummyPVGISProvider)
        poa_kwh_m2 = 1.0 if is_pvgis else 0.5  # ratio=0.5, cloudy => should NOT clip
        daily = pd.DataFrame(
            [
                {
                    "site": "s",
                    "array": "a",
                    "date": dt.date(2025, 12, 12).isoformat(),
                    "energy_kwh": 2.0,
                    "peak_kw": 1.0,
                    "poa_kwh_m2": poa_kwh_m2,
                    "temp_cell_max": 25.0,
                }
            ]
        )
        return SimulationResult(daily=daily, timeseries={})

    monkeypatch.setattr(cli_mod, "simulate_day", _fake_simulate_day)

    out = tmp_path / "out.json"
    cli_mod.run(
        config=config,
        date="2025-12-12",
        timestep="1h",
        weather_label="end",
        weather_source="open-meteo",
        weather_mode="standard",
        iam_model=None,
        iam_coefficient=None,
        output_shape="records",
        pvgis_cache_dir=None,
        qc_pvgis=True,
        actual_kwh_today=None,
        actual_limit_suppress=None,
        actual_as_of=None,
        base_load_w=None,
        min_duration_min=None,
        required_wh=None,
        debug=None,
        format="json",
        output=out,
        intervals=None,
        force=True,
    )

    records = json.loads(out.read_text())
    assert isinstance(records, list)
    assert len(records) == 1
    rec = records[0]
    assert rec["poa_kwh_m2"] == 0.5
    assert rec.get("qc_clipped", False) is False


def test_pvgis_qc_cloudy_band_does_not_clip_low_days(tmp_path, monkeypatch):
    # PVGIS QC is warn-first; low ratios are plausible (very cloudy days) and should not be clamped.
    from solarpredict import cli as cli_mod

    config = tmp_path / "cfg.yaml"
    _write_min_config(config, qc_pvgis=True)

    monkeypatch.setattr(cli_mod, "PVGISWeatherProvider", _DummyPVGISProvider)

    def _fake_simulate_day(*args, **kwargs):
        import pandas as pd
        from solarpredict.engine.simulate import SimulationResult

        is_pvgis = isinstance(kwargs.get("weather_provider"), _DummyPVGISProvider)
        poa_kwh_m2 = 1.0 if is_pvgis else 0.1  # ratio=0.1, cloudy => warn, no clip
        daily = pd.DataFrame(
            [
                {
                    "site": "s",
                    "array": "a",
                    "date": dt.date(2025, 12, 12).isoformat(),
                    "energy_kwh": 2.0,
                    "peak_kw": 1.0,
                    "poa_kwh_m2": poa_kwh_m2,
                    "temp_cell_max": 25.0,
                }
            ]
        )
        return SimulationResult(daily=daily, timeseries={})

    monkeypatch.setattr(cli_mod, "simulate_day", _fake_simulate_day)

    out = tmp_path / "out.json"
    cli_mod.run(
        config=config,
        date="2025-12-12",
        timestep="1h",
        weather_label="end",
        weather_source="open-meteo",
        weather_mode="standard",
        iam_model=None,
        iam_coefficient=None,
        output_shape="records",
        pvgis_cache_dir=None,
        qc_pvgis=True,
        actual_kwh_today=None,
        actual_limit_suppress=None,
        actual_as_of=None,
        base_load_w=None,
        min_duration_min=None,
        required_wh=None,
        debug=None,
        format="json",
        output=out,
        intervals=None,
        force=True,
    )

    records = json.loads(out.read_text())
    assert isinstance(records, list)
    assert len(records) == 1
    rec = records[0]
    assert rec.get("qc_clipped", False) is False
    assert rec["poa_kwh_m2"] == pytest.approx(0.1)
    assert rec["energy_kwh"] == pytest.approx(2.0)


def test_pvgis_qc_clamps_only_when_exceeding_clearsky_ceiling(tmp_path, monkeypatch):
    from solarpredict import cli as cli_mod

    config = tmp_path / "cfg.yaml"
    _write_min_config(config, qc_pvgis=True)

    monkeypatch.setattr(cli_mod, "PVGISWeatherProvider", _DummyPVGISProvider)
    monkeypatch.setattr(cli_mod, "_compute_clearsky_poa_kwh_m2", lambda *args, **kwargs: {("s", "a"): 1.0})

    def _fake_simulate_day(*args, **kwargs):
        import pandas as pd
        from solarpredict.engine.simulate import SimulationResult

        is_pvgis = isinstance(kwargs.get("weather_provider"), _DummyPVGISProvider)
        poa_kwh_m2 = 1.0 if is_pvgis else 10.0  # exceeds clear-sky ceiling => should clamp
        daily = pd.DataFrame(
            [
                {
                    "site": "s",
                    "array": "a",
                    "date": dt.date(2025, 12, 12).isoformat(),
                    "energy_kwh": 5.0,
                    "peak_kw": 2.0,
                    "poa_kwh_m2": poa_kwh_m2,
                    "temp_cell_max": 25.0,
                }
            ]
        )
        return SimulationResult(daily=daily, timeseries={})

    monkeypatch.setattr(cli_mod, "simulate_day", _fake_simulate_day)

    out = tmp_path / "out.json"
    cli_mod.run(
        config=config,
        date="2025-12-12",
        timestep="1h",
        weather_label="end",
        weather_source="open-meteo",
        weather_mode="standard",
        iam_model=None,
        iam_coefficient=None,
        output_shape="records",
        pvgis_cache_dir=None,
        qc_pvgis=True,
        actual_kwh_today=None,
        actual_limit_suppress=None,
        actual_as_of=None,
        base_load_w=None,
        min_duration_min=None,
        required_wh=None,
        debug=None,
        format="json",
        output=out,
        intervals=None,
        force=True,
    )

    rec = json.loads(out.read_text())[0]
    assert rec["qc_clipped"] is True
    assert rec["qc_clip_reason"] == "clearsky_ceiling"
    assert rec["poa_kwh_m2"] == pytest.approx(1.15)
    assert rec["energy_kwh"] == pytest.approx(0.575)
    assert rec["peak_kw"] == pytest.approx(0.23)
