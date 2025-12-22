"""
Open-Meteo Weather API Client.

Fetches weather observations and forecasts for Berlin.

API Documentation: https://open-meteo.com/en/docs
Free, no API key required.
"""

import httpx
from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# Data Models (Pydantic)
# =============================================================================

class WeatherObservation(BaseModel):
    """Hourly weather observation."""
    
    timestamp: datetime = Field(..., description="Observation timestamp (UTC)")
    temperature_c: float = Field(..., ge=-60, le=60, description="Temperature in Celsius")
    wind_speed_ms: float = Field(..., ge=0, le=150, description="Wind speed in m/s")
    wind_direction_deg: int = Field(..., ge=0, le=360, description="Wind direction in degrees")
    precipitation_mm: float = Field(..., ge=0, description="Precipitation in mm")
    humidity_pct: float = Field(..., ge=0, le=100, description="Relative humidity %")
    pressure_hpa: float = Field(..., ge=800, le=1100, description="Pressure in hPa")
    cloud_cover_pct: float = Field(..., ge=0, le=100, description="Cloud cover %")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "timestamp": "2024-12-20T14:00:00Z",
                "temperature_c": 5.2,
                "wind_speed_ms": 3.5,
                "wind_direction_deg": 270,
                "precipitation_mm": 0.0,
                "humidity_pct": 78.0,
                "pressure_hpa": 1013.25,
                "cloud_cover_pct": 65.0
            }
        }
    )


class WeatherForecast(BaseModel):
    """Hourly weather forecast."""
    
    timestamp: datetime = Field(..., description="Forecast timestamp (UTC)")
    forecast_time: datetime = Field(..., description="When forecast was made")
    temperature_c: float = Field(..., ge=-60, le=60)
    wind_speed_ms: float = Field(..., ge=0, le=150)
    wind_direction_deg: int = Field(..., ge=0, le=360)
    precipitation_mm: float = Field(..., ge=0)
    precipitation_probability_pct: Optional[float] = Field(None, ge=0, le=100)
    humidity_pct: float = Field(..., ge=0, le=100)
    pressure_hpa: float = Field(..., ge=800, le=1100)
    cloud_cover_pct: float = Field(..., ge=0, le=100)


# =============================================================================
# Weather Client
# =============================================================================

class WeatherClient:
    """
    Client for Open-Meteo Weather API.
    
    Fetches historical weather and forecasts for Berlin.
    """
    
    HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
    
    BERLIN_LAT = 52.52
    BERLIN_LON = 13.405
    
    HOURLY_VARS = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "pressure_msl",
        "cloud_cover",
        "wind_speed_10m",
        "wind_direction_10m"
    ]
    
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
    
    @property
    def client(self) -> httpx.AsyncClient:
        """Get HTTP client, creating if needed."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client
    
    async def get_historical(
        self,
        start_date: date,
        end_date: date
    ) -> list[WeatherObservation]:
        params = {
            "latitude": self.BERLIN_LAT,
            "longitude": self.BERLIN_LON,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(self.HOURLY_VARS),
            "timezone": "UTC"
        }
        
        try:
            response = await self.client.get(self.HISTORICAL_URL, params=params)
            response.raise_for_status()
            data = response.json()
            return self._parse_hourly_response(data)
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch historical weather: {e}")
            raise
    
    async def get_forecast(
        self,
        days: int = 7
    ) -> list[WeatherForecast]:
        params = {
            "latitude": self.BERLIN_LAT,
            "longitude": self.BERLIN_LON,
            "hourly": ",".join(self.HOURLY_VARS + ["precipitation_probability"]),
            "forecast_days": min(days, 16),
            "timezone": "UTC"
        }
        
        try:
            response = await self.client.get(self.FORECAST_URL, params=params)
            response.raise_for_status()
            data = response.json()
            return self._parse_forecast_response(data, datetime.utcnow())
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch forecast: {e}")
            raise

    async def get_current(self) -> WeatherObservation:
        """Get current weather for Berlin."""
        params = {
            "latitude": self.BERLIN_LAT,
            "longitude": self.BERLIN_LON,
            "current": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "pressure_msl",
                "cloud_cover",
                "wind_speed_10m",
                "wind_direction_10m"
            ]),
            "timezone": "UTC"
        }
        
        try:
            response = await self.client.get(self.FORECAST_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            current = data.get("current", {})
            return WeatherObservation(
                timestamp=datetime.fromisoformat(current.get("time", "").replace("Z", "+00:00")),
                temperature_c=current.get("temperature_2m", 0),
                wind_speed_ms=current.get("wind_speed_10m", 0) / 3.6,
                wind_direction_deg=current.get("wind_direction_10m", 0),
                precipitation_mm=current.get("precipitation", 0),
                humidity_pct=current.get("relative_humidity_2m", 0),
                pressure_hpa=current.get("pressure_msl", 1013),
                cloud_cover_pct=current.get("cloud_cover", 0)
            )
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch current weather: {e}")
            raise
    
    def _parse_hourly_response(self, data: dict) -> list[WeatherObservation]:
        observations = []
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        
        # Helper to get value at index with safety check
        def get_val(key, i, default=0):
            lst = hourly.get(key, [])
            return lst[i] if i < len(lst) else default

        for i, time_str in enumerate(times):
            try:
                obs = WeatherObservation(
                    timestamp=datetime.fromisoformat(time_str.replace("Z", "+00:00")),
                    temperature_c=get_val("temperature_2m", i),
                    wind_speed_ms=get_val("wind_speed_10m", i) / 3.6,
                    wind_direction_deg=int(get_val("wind_direction_10m", i)),
                    precipitation_mm=get_val("precipitation", i),
                    humidity_pct=get_val("relative_humidity_2m", i),
                    pressure_hpa=get_val("pressure_msl", i, 1013),
                    cloud_cover_pct=get_val("cloud_cover", i)
                )
                observations.append(obs)
            except Exception:
                continue
        return observations

    def _parse_forecast_response(self, data: dict, forecast_time: datetime) -> list[WeatherForecast]:
        forecasts = []
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        def get_val(key, i, default=0):
            lst = hourly.get(key, [])
            return lst[i] if i < len(lst) else default

        for i, time_str in enumerate(times):
            try:
                fc = WeatherForecast(
                    timestamp=datetime.fromisoformat(time_str.replace("Z", "+00:00")),
                    forecast_time=forecast_time,
                    temperature_c=get_val("temperature_2m", i),
                    wind_speed_ms=get_val("wind_speed_10m", i) / 3.6,
                    wind_direction_deg=int(get_val("wind_direction_10m", i)),
                    precipitation_mm=get_val("precipitation", i),
                    precipitation_probability_pct=get_val("precipitation_probability", i, None),
                    humidity_pct=get_val("relative_humidity_2m", i),
                    pressure_hpa=get_val("pressure_msl", i, 1013),
                    cloud_cover_pct=get_val("cloud_cover", i)
                )
                forecasts.append(fc)
            except Exception:
                continue
        return forecasts