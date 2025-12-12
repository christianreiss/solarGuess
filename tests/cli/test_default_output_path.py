import datetime as dt
import json
from pathlib import Path

import pytest

from solarpredict import cli as cli_mod


class _DummyProvider:
    def get_forecast(self, locations, start, end, timestep="1h"):
        raise AssertionError("Weather provider should not be called in this test")


def test_default_output_path_is_date_keyed_json(tmp_path, monkeypatch):
    # Ensure run() writes json/YYYY-MM-DD.json when --output is omitted.
    config = tmp_path / "cfg.yaml"
    config.write_text(
        "\n".join(
            [
                "sites:",
                "- id: s",
                "  location: {id: s, lat: 0.0, lon: 0.0, tz: UTC}",
                "  arrays:",
                "  - {id: a, tilt_deg: 30, azimuth_deg: 0, pdc0_w: 1000, gamma_pdc: -0.004, dc_ac_ratio: 1.1, eta_inv_nom: 0.96, losses_percent: 0, temp_model: close_mount_glass_glass}",
                "run:",
                "  qc_pvgis: false",
            ]
        )
        + "\n"
    )

    def _fake_simulate_day(*args, **kwargs):
        import pandas as pd

        daily = pd.DataFrame(
            [
                {
                    "site": "s",
                    "array": "a",
                    "date": dt.date(2025, 12, 12).isoformat(),
                    "energy_kwh": 1.0,
                    "peak_kw": 1.0,
                    "poa_kwh_m2": 1.0,
                    "temp_cell_max": 25.0,
                }
            ]
        )
        return type("Res", (), {"daily": daily, "timeseries": {}})()

    monkeypatch.setattr(cli_mod, "simulate_day", _fake_simulate_day)

    monkeypatch.chdir(tmp_path)
    cli_mod.run(
        config=config,
        date="2025-12-12",
        timestep="1h",
        weather_label="end",
        weather_source="open-meteo",
        weather_mode="standard",
        iam_model=None,
        iam_coefficient=None,
        output_shape="hierarchical",
        pvgis_cache_dir=None,
        qc_pvgis=False,
        actual_kwh_today=None,
        actual_limit_suppress=None,
        actual_as_of=None,
        base_load_w=None,
        min_duration_min=None,
        required_wh=None,
        debug=None,
        format="json",
        output=None,
        intervals=None,
        force=True,
    )

    out = tmp_path / "json" / "2025-12-12.json"
    assert out.exists()
    payload = json.loads(out.read_text())
    assert payload["meta"]["date"] == "2025-12-12"

