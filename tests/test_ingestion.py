"""
Tests for ingestion validators.

Run with: pytest tests/test_ingestion.py -v
"""

import pytest
from datetime import datetime, timedelta, timezone

from berlin_air_quality.ingestion.validators import (
    PM25Record, WeatherRecord, DeadLetterRecord,
    validate_pm25, validate_weather
)


class TestPM25Validation:
    """Tests for PM2.5 record validation."""
    
    def test_valid_record(self):
        """Test valid PM2.5 record passes."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": datetime.now(timezone.utc) - timedelta(hours=1),
            "value_ugm3": 18.5,
            "latitude": 52.52,
            "longitude": 13.405
        }
        
        record, dlq = validate_pm25(data)
        
        assert record is not None
        assert dlq is None
        assert record.value_ugm3 == 18.5
        assert record.station_id == "DEBB001"
    
    def test_negative_value_rejected(self):
        """Test negative PM2.5 values go to DLQ."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": -5.0
        }
        
        record, dlq = validate_pm25(data)
        
        assert record is None
        assert dlq is not None
        # Pydantic error says "greater than or equal to 0"
        assert "greater" in dlq.error_message.lower() or "negative" in dlq.error_message.lower()
    
    def test_error_code_rejected(self):
        """Test error code values (999) go to DLQ."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": 999
        }
        
        record, dlq = validate_pm25(data)
        
        assert record is None
        assert dlq is not None
        assert "error code" in dlq.error_message.lower()
    
    def test_high_value_rejected(self):
        """Test unreasonably high values (>1000) go to DLQ."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": 1500
        }
        
        record, dlq = validate_pm25(data)
        
        assert record is None
        assert dlq is not None
    
    def test_future_timestamp_rejected(self):
        """Test future timestamps go to DLQ."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": datetime.now(timezone.utc) + timedelta(days=1),
            "value_ugm3": 20.0
        }
        
        record, dlq = validate_pm25(data)
        
        assert record is None
        assert dlq is not None
        assert "future" in dlq.error_message.lower()
    
    def test_missing_required_field(self):
        """Test missing required fields go to DLQ."""
        data = {
            "station_id": "DEBB001",
            # Missing station_name
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": 20.0
        }
        
        record, dlq = validate_pm25(data)
        
        assert record is None
        assert dlq is not None
    
    def test_avro_dict_conversion(self):
        """Test conversion to Avro-compatible dict."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": datetime(2024, 12, 20, 14, 0, 0, tzinfo=timezone.utc),
            "value_ugm3": 18.5
        }
        
        record, _ = validate_pm25(data)
        avro_dict = record.to_avro_dict()
        
        assert isinstance(avro_dict["timestamp"], int)  # Milliseconds
        assert avro_dict["station_id"] == "DEBB001"
        assert avro_dict["value_ugm3"] == 18.5
        assert "ingested_at" in avro_dict


class TestWeatherValidation:
    """Tests for weather record validation."""
    
    def test_valid_observation(self):
        """Test valid weather observation passes."""
        data = {
            "timestamp": datetime.now(timezone.utc) - timedelta(hours=1),
            "temperature_c": 5.2,
            "wind_speed_ms": 3.5,
            "wind_direction_deg": 270,
            "precipitation_mm": 0.0,
            "humidity_pct": 78.0,
            "pressure_hpa": 1013.25,
            "cloud_cover_pct": 65.0
        }
        
        record, dlq = validate_weather(data)
        
        assert record is not None
        assert dlq is None
        assert record.temperature_c == 5.2
    
    def test_valid_forecast(self):
        """Test valid forecast with is_forecast=True."""
        data = {
            "timestamp": datetime.now(timezone.utc) + timedelta(days=1),
            "temperature_c": 3.0,
            "wind_speed_ms": 5.0,
            "wind_direction_deg": 180,
            "precipitation_mm": 2.5,
            "humidity_pct": 85.0,
            "pressure_hpa": 1008.0,
            "cloud_cover_pct": 90.0,
            "is_forecast": True
        }
        
        record, dlq = validate_weather(data)
        
        assert record is not None
        assert record.is_forecast is True
        assert record.forecast_timestamp is not None
    
    def test_invalid_temperature(self):
        """Test temperature out of range goes to DLQ."""
        data = {
            "timestamp": datetime.now(timezone.utc),
            "temperature_c": 100.0,  # Too hot
            "wind_speed_ms": 3.5,
            "wind_direction_deg": 270,
            "precipitation_mm": 0.0,
            "humidity_pct": 78.0,
            "pressure_hpa": 1013.25,
            "cloud_cover_pct": 65.0
        }
        
        record, dlq = validate_weather(data)
        
        assert record is None
        assert dlq is not None
    
    def test_invalid_humidity(self):
        """Test humidity >100% goes to DLQ."""
        data = {
            "timestamp": datetime.now(timezone.utc),
            "temperature_c": 5.0,
            "wind_speed_ms": 3.5,
            "wind_direction_deg": 270,
            "precipitation_mm": 0.0,
            "humidity_pct": 150.0,  # Invalid
            "pressure_hpa": 1013.25,
            "cloud_cover_pct": 65.0
        }
        
        record, dlq = validate_weather(data)
        
        assert record is None
        assert dlq is not None


class TestDeadLetterRecord:
    """Tests for DLQ record creation."""
    
    def test_dlq_record_creation(self):
        """Test DLQ record has all required fields."""
        dlq = DeadLetterRecord(
            original_topic="raw.berlin.pm25",
            error_type="ValidationError",
            error_message="Value out of range",
            raw_payload='{"value": -5}',
            source="eea"
        )
        
        assert dlq.retry_count == 0
        assert dlq.failed_at is not None
    
    def test_dlq_avro_conversion(self):
        """Test DLQ converts to Avro dict."""
        dlq = DeadLetterRecord(
            original_topic="raw.berlin.pm25",
            error_type="ValidationError",
            error_message="Test error",
            raw_payload='{}',
            source="test"
        )
        
        avro_dict = dlq.to_avro_dict()
        
        assert isinstance(avro_dict["failed_at"], int)
        assert avro_dict["original_topic"] == "raw.berlin.pm25"


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""
    
    def test_pm25_boundary_good(self):
        """Test PM2.5 at Good/Moderate boundary (15)."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Test",
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": 15.0
        }
        
        record, _ = validate_pm25(data)
        assert record is not None
    
    def test_pm25_boundary_moderate(self):
        """Test PM2.5 at Moderate/Unhealthy boundary (35)."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Test",
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": 35.0
        }
        
        record, _ = validate_pm25(data)
        assert record is not None
    
    def test_pm25_zero_valid(self):
        """Test PM2.5 = 0 is valid."""
        data = {
            "station_id": "DEBB001",
            "station_name": "Test",
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": 0.0
        }
        
        record, _ = validate_pm25(data)
        assert record is not None
        assert record.value_ugm3 == 0.0
    
    def test_empty_station_id_rejected(self):
        """Test empty station_id is rejected."""
        data = {
            "station_id": "",
            "station_name": "Test",
            "timestamp": datetime.now(timezone.utc),
            "value_ugm3": 20.0
        }
        
        record, dlq = validate_pm25(data)
        assert record is None
        assert dlq is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
