"""Weather provider interfaces and implementations."""

from .base import WeatherProvider
from .open_meteo import OpenMeteoWeatherProvider
from .pvgis import PVGISWeatherProvider
from .composite import CompositeWeatherProvider

__all__ = ["WeatherProvider", "OpenMeteoWeatherProvider", "PVGISWeatherProvider", "CompositeWeatherProvider"]
