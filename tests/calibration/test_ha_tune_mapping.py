from solarpredict.calibration.ha_tune import auto_calibration_groups, normalize_ha_entity_id
from solarpredict.core.models import Location, PVArray, Scenario, Site


def _scenario() -> Scenario:
    loc = Location(id="zirpennest", lat=51.0, lon=7.0, tz="Europe/Berlin")
    arrays = [
        PVArray(
            id="pv_array_south_primary",
            tilt_deg=45,
            azimuth_deg=180,
            pdc0_w=5000,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=7,
            temp_model="close_mount_glass_glass",
        ),
        PVArray(
            id="pv_array_south_secondary",
            tilt_deg=0,
            azimuth_deg=180,
            pdc0_w=1000,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=7,
            temp_model="close_mount_glass_glass",
        ),
        PVArray(
            id="pv_array_north_primary",
            tilt_deg=45,
            azimuth_deg=0,
            pdc0_w=5000,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=7,
            temp_model="close_mount_glass_glass",
        ),
        PVArray(
            id="pv_array_north_secondary",
            tilt_deg=0,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=7,
            temp_model="close_mount_glass_glass",
        ),
        PVArray(
            id="solarfarm",
            tilt_deg=0,
            azimuth_deg=180,
            pdc0_w=5000,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.1,
            eta_inv_nom=0.97,
            losses_percent=7,
            temp_model="close_mount_glass_glass",
        ),
        PVArray(
            id="playhouse",
            tilt_deg=45,
            azimuth_deg=180,
            pdc0_w=500,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.05,
            eta_inv_nom=0.97,
            losses_percent=7,
            temp_model="close_mount_glass_glass",
        ),
    ]
    return Scenario(sites=[Site(id="zirpennest", location=loc, arrays=arrays)])


def test_normalize_ha_entity_id():
    assert normalize_ha_entity_id("sensor.solarfarm_one_energy_today") == "solarfarm"
    assert normalize_ha_entity_id("sensor.playhouse_phase_b_energy_today") == "playhouse"
    assert normalize_ha_entity_id("sensor.house_north_energy_today") == "house_north"


def test_auto_calibration_groups_maps_expected_arrays():
    scenario = _scenario()
    sensors = [
        "sensor.house_north_energy_today",
        "sensor.house_south_energy_today",
        "sensor.playhouse_phase_b_energy_today",
        "sensor.solarfarm_one_energy_today",
        "sensor.solarfarm_two_energy_today",
        "sensor.total_pv_energy_today",
    ]
    groups = auto_calibration_groups(scenario, sensors, include_total=True)
    by_name = {g.name: g for g in groups}

    assert "house_north" in by_name
    assert "house_south" in by_name
    assert "playhouse" in by_name
    assert "solarfarm" in by_name
    assert "total_pv" in by_name

    assert set(by_name["house_north"].arrays) == {
        ("zirpennest", "pv_array_north_primary"),
        ("zirpennest", "pv_array_north_secondary"),
    }
    assert set(by_name["house_south"].arrays) == {
        ("zirpennest", "pv_array_south_primary"),
        ("zirpennest", "pv_array_south_secondary"),
    }
    assert by_name["playhouse"].arrays == [("zirpennest", "playhouse")]
    assert by_name["solarfarm"].arrays == [("zirpennest", "solarfarm")]
    assert set(by_name["solarfarm"].ha_entities) == {
        "sensor.solarfarm_one_energy_today",
        "sensor.solarfarm_two_energy_today",
    }

