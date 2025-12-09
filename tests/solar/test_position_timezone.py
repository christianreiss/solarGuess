import pandas as pd
import pytz
import pytest

from solarpredict.core.models import Location
from solarpredict.solar.position import solar_position


def test_times_must_be_timezone_aware():
    loc = Location(id="somewhere", lat=10.0, lon=10.0, tz="UTC")
    times = pd.date_range("2025-01-01", periods=2, freq="1h")  # naive
    with pytest.raises(ValueError):
        solar_position(loc, times)


def test_timezone_handling_outputs():
    loc = Location(id="berlin", lat=52.52, lon=13.405, tz="Europe/Berlin")
    tz = pytz.timezone(loc.tz)
    times = pd.date_range("2025-03-20 10:00", periods=3, freq="1h", tz=tz)
    df = solar_position(loc, times)
    assert str(df.index.tz) == str(tz)
