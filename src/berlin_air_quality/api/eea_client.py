"""
EEA (European Environment Agency) API Client.

Fetches PM2.5 air quality data for Berlin monitoring stations.

API Documentation: https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
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

class PM25Measurement(BaseModel):
    """Single PM2.5 measurement from a station."""
    
    station_id: str = Field(..., description="EEA station identifier")
    station_name: str = Field(..., description="Human-readable station name")
    timestamp: datetime = Field(..., description="Measurement timestamp (UTC)")
    value_ugm3: float = Field(..., ge=0, le=1000, description="PM2.5 in µg/m³")
    is_valid: bool = Field(default=True, description="Data quality flag")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "station_id": "STA.DE.DEBB021",
                "station_name": "Berlin Neukölln",
                "timestamp": "2024-12-20T14:00:00Z",
                "value_ugm3": 18.5,
                "is_valid": True
            }
        }
    )


class StationInfo(BaseModel):
    """Metadata about a monitoring station."""
    
    station_id: str
    station_name: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    altitude_m: Optional[float] = None
    station_type: Optional[str] = None  # background, traffic, industrial
    area_type: Optional[str] = None  # urban, suburban, rural


# =============================================================================
# EEA Client
# =============================================================================

class EEAClient:
    """
    Client for EEA Air Quality API.
    
    Fetches PM2.5 data for Berlin stations from the European Environment Agency.
    """
    
    # EEA API endpoints
    BASE_URL = "https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw"
    
    # Berlin bounding box (approximate)
    BERLIN_BBOX = {
        "lat_min": 52.33,
        "lat_max": 52.68,
        "lon_min": 13.08,
        "lon_max": 13.76
    }
    
    # Germany country code
    COUNTRY_CODE = "DE"
    
    # Berlin city code prefix
    BERLIN_PREFIX = "DEBB"  # DE = Germany, BB = Berlin (Bundesland code)
    
    def __init__(self, timeout: float = 30.0):
        """Initialize EEA client."""
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
    
    async def get_berlin_stations(self) -> list[StationInfo]:
        """
        Get list of PM2.5 monitoring stations in Berlin.
        
        Returns:
            List of StationInfo objects for Berlin stations.
        """
        params = {
            "CountryCode": self.COUNTRY_CODE,
            "Pollutant": "6001",  # PM2.5 pollutant code
            "Year_from": "2024",
            "Year_to": "2024",
            "Source": "E1a",  # Validated data
            "Output": "TEXT",
            "TimeCoverage": "Year"
        }
        
        try:
            response = await self.client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            
            # Parse CSV response and filter Berlin stations
            stations = self._parse_stations_from_response(response.text)
            berlin_stations = [s for s in stations if self.BERLIN_PREFIX in s.station_id]
            
            logger.info(f"Found {len(berlin_stations)} Berlin PM2.5 stations")
            return berlin_stations
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch stations: {e}")
            raise
    
    async def get_pm25_measurements(
        self,
        start_date: date,
        end_date: date,
        station_ids: Optional[list[str]] = None
    ) -> list[PM25Measurement]:
        """
        Fetch PM2.5 measurements for Berlin stations.
        """
        params = {
            "CountryCode": self.COUNTRY_CODE,
            "Pollutant": "6001",  # PM2.5
            "Year_from": str(start_date.year),
            "Year_to": str(end_date.year),
            "Source": "E2a",  # Up-to-date (unvalidated) for recent data
            "Output": "TEXT",
            "TimeCoverage": "Year"
        }
        
        try:
            response = await self.client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            
            measurements = self._parse_measurements_from_response(
                response.text, 
                start_date, 
                end_date,
                station_ids
            )
            
            logger.info(f"Fetched {len(measurements)} PM2.5 measurements")
            return measurements
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch measurements: {e}")
            raise
    
    def _parse_stations_from_response(self, csv_text: str) -> list[StationInfo]:
        """Parse station info from EEA CSV response."""
        stations = []
        lines = csv_text.strip().split('\n')
        
        if len(lines) < 2:
            return stations
        
        # Skip header
        for line in lines[1:]:
            try:
                parts = line.split('\t')
                if len(parts) >= 6:
                    station = StationInfo(
                        station_id=parts[0].strip(),
                        station_name=parts[1].strip() if len(parts) > 1 else "Unknown",
                        latitude=float(parts[2]) if len(parts) > 2 and parts[2] else 52.52,
                        longitude=float(parts[3]) if len(parts) > 3 and parts[3] else 13.40,
                        station_type=parts[4].strip() if len(parts) > 4 else None,
                        area_type=parts[5].strip() if len(parts) > 5 else None
                    )
                    stations.append(station)
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse station line: {e}")
                continue
        
        return stations
    
    def _parse_measurements_from_response(
        self,
        csv_text: str,
        start_date: date,
        end_date: date,
        station_ids: Optional[list[str]] = None
    ) -> list[PM25Measurement]:
        """Parse measurements from EEA CSV response."""
        measurements = []
        lines = csv_text.strip().split('\n')
        
        if len(lines) < 2:
            return measurements
        
        # Skip header
        for line in lines[1:]:
            try:
                parts = line.split('\t')
                if len(parts) >= 4:
                    station_id = parts[0].strip()
                    
                    # Filter Berlin stations
                    if self.BERLIN_PREFIX not in station_id:
                        continue
                    
                    # Filter specific stations if provided
                    if station_ids and station_id not in station_ids:
                        continue
                    
                    # Parse timestamp
                    timestamp = datetime.fromisoformat(parts[2].replace('Z', '+00:00'))
                    
                    # Filter date range
                    if not (start_date <= timestamp.date() <= end_date):
                        continue
                    
                    # Parse value
                    value = float(parts[3]) if parts[3] else None
                    if value is None or value < 0:
                        continue
                    
                    measurement = PM25Measurement(
                        station_id=station_id,
                        station_name=parts[1].strip() if len(parts) > 1 else "Unknown",
                        timestamp=timestamp,
                        value_ugm3=value,
                        is_valid=True
                    )
                    measurements.append(measurement)
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse measurement line: {e}")
                continue
        
        return measurements