import pandas as pd

from solarpredict.core.debug import ListDebugCollector
from solarpredict.solar.irradiance import poa_irradiance


def test_poa_summary_uses_interval_h_when_provided():
    times = pd.date_range("2025-01-01 12:00", periods=3, freq="1h", tz="UTC")
    solar_zenith = pd.Series([30.0, 30.0, 30.0], index=times)
    solar_azimuth = pd.Series([180.0, 180.0, 180.0], index=times)

    dni = pd.Series([700.0, 700.0, 700.0], index=times)
    ghi = pd.Series([800.0, 800.0, 800.0], index=times)
    dhi = pd.Series([100.0, 100.0, 100.0], index=times)

    # Deliberately "wrong" interval widths: 2h per sample to prove the summary integrates using interval_h.
    interval_h = pd.Series([2.0, 2.0, 2.0], index=times)

    debug = ListDebugCollector()
    df = poa_irradiance(
        surface_tilt=30.0,
        surface_azimuth=180.0,
        dni=dni,
        ghi=ghi,
        dhi=dhi,
        solar_zenith=solar_zenith,
        solar_azimuth=solar_azimuth,
        interval_h=interval_h,
        debug=debug,
    )

    summary = next(e for e in debug.events if e["stage"] == "poa.summary")
    expected_kwh_m2 = float(((df["poa_global"] / 1000.0) * interval_h).sum())

    assert summary["payload"]["integration"] == "interval_h"
    assert summary["payload"]["poa_kwh_m2"] == expected_kwh_m2
    # Guard against the fallback path silently being used.
    wrong_kwh_m2 = float(((df["poa_global"] / 1000.0) * 1.0).sum())
    assert summary["payload"]["poa_kwh_m2"] != wrong_kwh_m2

