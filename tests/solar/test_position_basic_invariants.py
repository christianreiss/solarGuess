import pandas as pd
import pytz

from solarpredict.core.models import Location
from solarpredict.solar.position import solar_position


def test_nighttime_negative_elevation():
    # Antarctica winter night
    loc = Location(id="mcmurdo", lat=-77.8419, lon=166.6863, tz="Antarctica/McMurdo")
    times = pd.date_range("2025-06-21 00:00", periods=3, freq="1h", tz=pytz.timezone(loc.tz))
    df = solar_position(loc, times)
    assert (df["elevation"] < 0).all()


def test_bounds():
    loc = Location(id="equator", lat=0.0, lon=0.0, tz="UTC")
    times = pd.date_range("2025-03-21 06:00", periods=6, freq="1h", tz="UTC")
    df = solar_position(loc, times)
    assert ((df["zenith"] >= 0) & (df["zenith"] <= 180)).all()
    assert ((df["azimuth"] >= -360) & (df["azimuth"] <= 360)).all()
