import datetime as dt
from pathlib import Path

from solarpredict.integrations.ha_export import HaDailyMaxExport


def test_ha_export_parses_and_normalizes(tmp_path: Path):
    sample = Path("tests/fixtures/ha_export_sample.json").read_text()
    p = tmp_path / "sample.json"
    p.write_text(sample)

    exp = HaDailyMaxExport.from_path(p)
    assert exp.year == 2025
    assert "sensor.total_pv_energy_today" in exp.sensors

    df = exp.to_frame()
    assert set(df.columns) == {"entity_id", "day", "energy_kwh"}
    assert len(df) == 4
    assert df["day"].min() == dt.date(2025, 12, 11)
    assert df["day"].max() == dt.date(2025, 12, 12)


def test_ha_export_entity_filter(tmp_path: Path):
    sample = Path("tests/fixtures/ha_export_sample.json").read_text()
    p = tmp_path / "sample.json"
    p.write_text(sample)

    exp = HaDailyMaxExport.from_path(p)
    df = exp.to_frame(entities=["sensor.total_pv_energy_today"])
    assert df["entity_id"].unique().tolist() == ["sensor.total_pv_energy_today"]
    assert len(df) == 2

