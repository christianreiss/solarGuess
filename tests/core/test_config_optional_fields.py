from pathlib import Path

from solarpredict.core.config import load_scenario


def test_load_scenario_carries_inverter_fields(tmp_path: Path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "sites:\n"
        "- id: s1\n"
        "  location:\n"
        "    id: loc1\n"
        "    lat: 0\n"
        "    lon: 0\n"
        "    tz: UTC\n"
        "  arrays:\n"
        "  - id: a1\n"
        "    tilt_deg: 30\n"
        "    azimuth_deg: 0\n"
        "    pdc0_w: 4000\n"
        "    gamma_pdc: -0.004\n"
        "    dc_ac_ratio: 1.1\n"
        "    eta_inv_nom: 0.96\n"
        "    losses_percent: 5\n"
        "    temp_model: close_mount_glass_glass\n"
        "    inverter_group_id: g1\n"
        "    inverter_pdc0_w: 6500\n"
    )

    scenario = load_scenario(cfg)
    arr = scenario.sites[0].arrays[0]
    assert arr.inverter_group_id == "g1"
    assert arr.inverter_pdc0_w == 6500
