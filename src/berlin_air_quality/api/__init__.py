"""API clients for external data sources."""

from .eea_client import EEAClient
from .weather_client import WeatherClient

__all__ = ["EEAClient", "WeatherClient"]