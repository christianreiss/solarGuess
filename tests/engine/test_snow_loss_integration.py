import datetime as dt

import pandas as pd
import pytest
from solarpredict.core.debug import ListDebugCollector
from solarpredict.core.models import Location, PVArray, Scenario, Site
from solarpredict.engine.simulate import simulate_day
from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider
from solarpredict.weather.prefetched import PrefetchedWeatherProvider


class SnowyWeather:
    def __init__(self):
        idx = pd.date_range("2025-01-01", periods=3, freq="1h", tz="UTC")
        self.df = pd.DataFrame(
            {
                "ghi_wm2": [200, 400, 200],
                "dni_wm2": [150, 300, 150],
                "dhi_wm2": [50, 100, 50],
                "temp_air_c": [-2.0, -2.0, -2.0],
                "wind_ms": [1.0, 1.0, 1.0],
                "snow_depth_cm": [10.0, 10.0, 10.0],
            },
            index=idx,
        )

    def get_forecast(self, locations, start, end, timestep="1h"):
        return {loc["id"]: self.df for loc in locations}


def _scenario():
    loc = Location(id="loc", lat=0, lon=0, tz="UTC")
    arr = PVArray(
        id="a",
        tilt_deg=30,
        azimuth_deg=180,
        pdc0_w=10000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.1,
        eta_inv_nom=0.96,
        losses_percent=0,
        temp_model="close_mount_glass_glass",
    )
    site = Site(id="s", location=loc, arrays=[arr])
    return Scenario(sites=[site])


def test_snow_loss_reduces_energy_and_marks_timeseries(monkeypatch):
    wx = SnowyWeather()
    clear_df = wx.df.drop(columns=["snow_depth_cm"]).copy()

    class WeatherWithoutSnow:
        def get_forecast(self, locations, start, end, timestep="1h"):
            return {loc["id"]: clear_df for loc in locations}

    weather_provider = WeatherWithoutSnow()

    def _fake_open_meteo_clear(self, locations, start, end, timestep="1h"):
        return {loc["id"]: clear_df for loc in locations}

    monkeypatch.setattr(OpenMeteoWeatherProvider, "get_forecast", _fake_open_meteo_clear)
    result_clear = simulate_day(_scenario(), dt.date(2025, 1, 1), timestep="1h", weather_provider=weather_provider)

    snow_df = wx.df.copy()
    snow_df["snow_depth_cm"] = [10.0, 10.0, 10.0]

    def _fake_open_meteo_snow(self, locations, start, end, timestep="1h"):
        return {loc["id"]: snow_df for loc in locations}

    monkeypatch.setattr(OpenMeteoWeatherProvider, "get_forecast", _fake_open_meteo_snow)
    result_snow = simulate_day(_scenario(), dt.date(2025, 1, 1), timestep="1h", weather_provider=weather_provider)

    clear_energy = result_clear.daily.iloc[0]["energy_kwh"]
    snow_energy = result_snow.daily.iloc[0]["energy_kwh"]
    assert snow_energy < clear_energy

    ts = result_snow.timeseries[("s", "a")]
    assert "snow_loss_factor" in ts.columns
    assert ts["snow_loss_factor"].iloc[0] == pytest.approx(0.3)


def test_prefetched_weather_with_snow_columns_does_not_side_fetch(monkeypatch):
    wx = SnowyWeather()

    def _unexpected_open_meteo(*args, **kwargs):
        raise AssertionError("Open-Meteo snow fallback should not be used when weather already includes snow columns")

    monkeypatch.setattr(OpenMeteoWeatherProvider, "get_forecast", _unexpected_open_meteo)

    provider = PrefetchedWeatherProvider({"s": wx.df})
    result = simulate_day(_scenario(), dt.date(2025, 1, 1), timestep="1h", weather_provider=provider)

    ts = result.timeseries[("s", "a")]
    assert ts["snow_loss_factor"].iloc[0] == pytest.approx(0.3)


def test_empty_weather_slice_raises_friendly_error():
    idx = pd.date_range("2025-01-02", periods=3, freq="1h", tz="UTC")
    df = pd.DataFrame(
        {
            "ghi_wm2": [200.0, 400.0, 200.0],
            "dni_wm2": [150.0, 300.0, 150.0],
            "dhi_wm2": [50.0, 100.0, 50.0],
            "temp_air_c": [0.0, 1.0, 0.0],
            "wind_ms": [1.0, 1.0, 1.0],
        },
        index=idx,
    )
    provider = PrefetchedWeatherProvider({"s": df})
    debug = ListDebugCollector()

    with pytest.raises(ValueError, match="No weather samples for site s on 2025-01-01\\."):
        simulate_day(
            _scenario(),
            dt.date(2025, 1, 1),
            timestep="1h",
            weather_provider=provider,
            snow_weather_provider=False,
            debug=debug,
        )

    assert any(event["stage"] == "weather.empty" for event in debug.events)
