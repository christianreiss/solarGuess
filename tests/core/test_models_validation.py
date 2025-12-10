import pytest

from solarpredict.core.models import Location, PVArray, Site, Scenario, ValidationError


def test_location_invalid_lat_lon():
    with pytest.raises(ValidationError):
        Location(id="loc", lat=95, lon=0)
    with pytest.raises(ValidationError):
        Location(id="loc", lat=0, lon=190)


def test_pvarray_validation():
    with pytest.raises(ValidationError):
        PVArray(
            id="",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=14,
            temp_model="noct",
        )
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=-1,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=14,
            temp_model="noct",
        )
    arr_norm = PVArray(
        id="arr",
        tilt_deg=10,
        azimuth_deg=999,
        pdc0_w=1000,
        gamma_pdc=-0.003,
        dc_ac_ratio=1.2,
        eta_inv_nom=0.96,
        losses_percent=14,
        temp_model="noct",
    )
    assert arr_norm.azimuth_deg == -81
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=-5,
            gamma_pdc=-0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=14,
            temp_model="noct",
        )
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=14,
            temp_model="noct",
        )
    arr = PVArray(
        id="arr",
        tilt_deg=10,
        azimuth_deg=270,
        pdc0_w=1000,
        gamma_pdc=-0.003,
        dc_ac_ratio=1.2,
        eta_inv_nom=0.96,
        losses_percent=14,
        temp_model="noct",
    )
    assert arr.azimuth_deg == -90
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.003,
            dc_ac_ratio=0,
            eta_inv_nom=0.96,
            losses_percent=14,
            temp_model="noct",
        )
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0,
            losses_percent=14,
            temp_model="noct",
        )
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            inverter_pdc0_w=-1.0,
            losses_percent=14,
            temp_model="noct",
        )
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            inverter_pdc0_w=-1.0,
            losses_percent=14,
            temp_model="noct",
        )
    with pytest.raises(ValidationError):
        PVArray(
            id="arr",
            tilt_deg=10,
            azimuth_deg=0,
            pdc0_w=1000,
            gamma_pdc=-0.003,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.96,
            losses_percent=120,
            temp_model="noct",
        )


def test_site_and_scenario_validation():
    location = Location(id="loc", lat=1, lon=1)
    array = PVArray(
        id="arr",
        tilt_deg=30,
        azimuth_deg=180,
        pdc0_w=3000,
        gamma_pdc=-0.003,
        dc_ac_ratio=1.2,
        eta_inv_nom=0.96,
        losses_percent=14,
        temp_model="noct",
    )

    with pytest.raises(ValidationError):
        Site(id="", location=location, arrays=[array])
    with pytest.raises(ValidationError):
        Site(id="site", location="notloc", arrays=[array])
    with pytest.raises(ValidationError):
        Site(id="site", location=location, arrays=[])
    with pytest.raises(ValidationError):
        Scenario(sites=[])

    good_site = Site(id="site", location=location, arrays=[array])
    scenario = Scenario(sites=[good_site])
    assert scenario.sites[0].id == "site"
