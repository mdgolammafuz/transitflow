"""
Pydantic validators for ingestion data.

Principle #1: Ingestion is a Contract
- Strict validation before producing to Kafka
- Invalid records go to DLQ, never silently dropped
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator
import logging

logger = logging.getLogger(__name__)


class PM25Record(BaseModel):
    """
    Validated PM2.5 measurement record.
    
    Strict validation ensures only clean data enters the pipeline.
    """
    
    station_id: str = Field(..., min_length=1, max_length=100)
    station_name: str = Field(..., min_length=1, max_length=200)
    timestamp: datetime
    value_ugm3: float = Field(..., ge=0, le=1000)  # Physical constraints
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    is_valid: bool = True
    
    @field_validator('station_id')
    @classmethod
    def validate_station_id(cls, v: str) -> str:
        """Ensure station_id follows expected pattern."""
        v = v.strip()
        if not v:
            raise ValueError("station_id cannot be empty")
        return v
    
    @field_validator('value_ugm3')
    @classmethod
    def validate_pm25_value(cls, v: float) -> float:
        """
        Validate PM2.5 value is physically reasonable.
        
        - Negative values: sensor error
        - > 1000 µg/m³: extremely rare, likely error
        - Common error codes: 999, -999, -1
        """
        if v < 0:
            raise ValueError(f"PM2.5 cannot be negative: {v}")
        if v > 1000:
            raise ValueError(f"PM2.5 value suspiciously high: {v}")
        # Check for common error codes
        if v in [999, 9999, -999, -1]:
            raise ValueError(f"PM2.5 value appears to be error code: {v}")
        return v
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        """Ensure timestamp is not in the future."""
        now = datetime.utcnow()
        if v.replace(tzinfo=None) > now:
            raise ValueError(f"Timestamp cannot be in the future: {v}")
        return v
    
    def to_avro_dict(self) -> dict:
        """Convert to Avro-compatible dictionary."""
        return {
            "station_id": self.station_id,
            "station_name": self.station_name,
            "timestamp": int(self.timestamp.timestamp() * 1000),  # ms since epoch
            "value_ugm3": self.value_ugm3,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "is_valid": self.is_valid,
            "ingested_at": int(datetime.utcnow().timestamp() * 1000)
        }


class WeatherRecord(BaseModel):
    """
    Validated weather observation/forecast record.
    """
    
    timestamp: datetime
    temperature_c: float = Field(..., ge=-60, le=60)
    wind_speed_ms: float = Field(..., ge=0, le=150)
    wind_direction_deg: int = Field(..., ge=0, le=360)
    precipitation_mm: float = Field(..., ge=0, le=500)
    humidity_pct: float = Field(..., ge=0, le=100)
    pressure_hpa: float = Field(..., ge=800, le=1100)
    cloud_cover_pct: float = Field(..., ge=0, le=100)
    is_forecast: bool = False
    forecast_timestamp: Optional[datetime] = None
    
    @model_validator(mode='after')
    def validate_forecast_fields(self):
        """If is_forecast=True, forecast_timestamp should be set."""
        if self.is_forecast and self.forecast_timestamp is None:
            # Auto-set to now if not provided
            self.forecast_timestamp = datetime.utcnow()
        return self
    
    def to_avro_dict(self) -> dict:
        """Convert to Avro-compatible dictionary."""
        return {
            "timestamp": int(self.timestamp.timestamp() * 1000),
            "temperature_c": self.temperature_c,
            "wind_speed_ms": self.wind_speed_ms,
            "wind_direction_deg": self.wind_direction_deg,
            "precipitation_mm": self.precipitation_mm,
            "humidity_pct": self.humidity_pct,
            "pressure_hpa": self.pressure_hpa,
            "cloud_cover_pct": self.cloud_cover_pct,
            "is_forecast": self.is_forecast,
            "forecast_timestamp": (
                int(self.forecast_timestamp.timestamp() * 1000) 
                if self.forecast_timestamp else None
            ),
            "ingested_at": int(datetime.utcnow().timestamp() * 1000)
        }


class DeadLetterRecord(BaseModel):
    """Record for the Dead Letter Queue."""
    
    original_topic: str
    error_type: str
    error_message: str
    raw_payload: str
    source: str
    failed_at: datetime = Field(default_factory=datetime.utcnow)
    retry_count: int = 0
    
    def to_avro_dict(self) -> dict:
        """Convert to Avro-compatible dictionary."""
        return {
            "original_topic": self.original_topic,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "raw_payload": self.raw_payload,
            "source": self.source,
            "failed_at": int(self.failed_at.timestamp() * 1000),
            "retry_count": self.retry_count
        }


# =============================================================================
# Validation Functions
# =============================================================================

def validate_pm25(data: dict, source: str = "unknown") -> tuple[Optional[PM25Record], Optional[DeadLetterRecord]]:
    """
    Validate PM2.5 data and return either valid record or DLQ record.
    
    Args:
        data: Raw data dictionary
        source: Data source identifier
        
    Returns:
        Tuple of (valid_record, dlq_record) - one will be None
    """
    try:
        record = PM25Record(**data)
        return record, None
    except Exception as e:
        import json
        dlq = DeadLetterRecord(
            original_topic="raw.berlin.pm25",
            error_type=type(e).__name__,
            error_message=str(e),
            raw_payload=json.dumps(data, default=str),
            source=source
        )
        logger.warning(f"PM2.5 validation failed: {e}")
        return None, dlq


def validate_weather(data: dict, source: str = "open-meteo") -> tuple[Optional[WeatherRecord], Optional[DeadLetterRecord]]:
    """
    Validate weather data and return either valid record or DLQ record.
    
    Args:
        data: Raw data dictionary
        source: Data source identifier
        
    Returns:
        Tuple of (valid_record, dlq_record) - one will be None
    """
    try:
        record = WeatherRecord(**data)
        return record, None
    except Exception as e:
        import json
        dlq = DeadLetterRecord(
            original_topic="raw.berlin.weather",
            error_type=type(e).__name__,
            error_message=str(e),
            raw_payload=json.dumps(data, default=str),
            source=source
        )
        logger.warning(f"Weather validation failed: {e}")
        return None, dlq
