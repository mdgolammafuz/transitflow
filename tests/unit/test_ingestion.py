"""
Unit tests for ingestion module.

Run with: pytest tests/unit/test_ingestion.py -v
"""

import json
from datetime import datetime, timezone

import pytest

from src.ingestion.models import InvalidEvent, RawHSLPayload, VehiclePosition


class TestVehiclePosition:
    """Tests for VehiclePosition model validation."""

    def test_valid_position(self):
        """Basic valid position."""
        pos = VehiclePosition(
            vehicle_id=1234,
            timestamp=datetime(2024, 12, 31, 10, 0, 0, tzinfo=timezone.utc),
            latitude=60.17,
            longitude=24.94,
            speed_ms=12.5,
            heading=180,
            delay_seconds=120,
            door_status=False,  # Use Boolean
            line_id="600",
            direction_id=1,
            operator_id=22,
        )
        assert pos.vehicle_id == 1234
        assert pos.delay_seconds == 120
        # Check computed field
        assert pos.event_time_ms == 1735639200000 

    def test_timestamp_parsing(self):
        """ISO timestamp string parsing."""
        pos = VehiclePosition(
            vehicle_id=1234,
            timestamp="2024-12-31T10:00:00Z",
            latitude=60.17,
            longitude=24.94,
            speed_ms=0,
            heading=0,
            delay_seconds=0,
            door_status=False,
            line_id="600",
            direction_id=1,
            operator_id=22,
        )
        assert pos.timestamp.year == 2024
        assert pos.timestamp.month == 12
        assert pos.timestamp.tzinfo == timezone.utc

    def test_latitude_bounds(self):
        """Latitude must be within Helsinki region (59-61)."""
        with pytest.raises(ValueError):
            VehiclePosition(
                vehicle_id=1234,
                timestamp=datetime.now(timezone.utc),
                latitude=50.0,  # FAIL: Outside Helsinki
                longitude=24.94,
                speed_ms=0,
                heading=0,
                delay_seconds=0,
                door_status=False,
                line_id="600",
                direction_id=1,
                operator_id=22,
            )

    def test_speed_bounds(self):
        """Speed must be reasonable (<= 40m/s)."""
        with pytest.raises(ValueError):
            VehiclePosition(
                vehicle_id=1234,
                timestamp=datetime.now(timezone.utc),
                latitude=60.17,
                longitude=24.94,
                speed_ms=100.0,  # FAIL: 360 km/h is unrealistic
                heading=0,
                delay_seconds=0,
                door_status=False,
                line_id="600",
                direction_id=1,
                operator_id=22,
            )


class TestRawHSLPayload:
    """Tests for raw HSL payload parsing and mapping."""

    def test_convert_to_vehicle_position(self):
        """Convert raw payload (int/floats) to validated model (bools)."""
        data = {
            "desi": "600",
            "dir": "1",
            "oper": 22,
            "veh": 1234,
            "tst": "2024-12-31T10:00:00.000Z",
            "tsi": 1735639200,
            "spd": 12.5,
            "hdg": 180,
            "lat": 60.17,
            "long": 24.94,
            "dl": 120,
            "drst": 1, # Raw is int (1)
        }
        raw = RawHSLPayload.model_validate(data)
        pos = raw.to_vehicle_position()
        
        assert pos.vehicle_id == 1234
        assert pos.line_id == "600"
        assert pos.speed_ms == 12.5
        assert pos.door_status is True # Converted to Bool

    def test_minimal_payload(self):
        """Parse minimal required fields."""
        data = {
            "veh": 1234,
            "tst": "2024-12-31T10:00:00.000Z",
            "tsi": 1735639200,
            "lat": 60.17,
            "long": 24.94,
        }
        raw = RawHSLPayload.model_validate(data)
        pos = raw.to_vehicle_position()
        
        assert pos.vehicle_id == 1234
        assert pos.speed_ms == 0.0  # Default
        assert pos.door_status is False # Default

    def test_null_handling(self):
        """Handle null optional fields explicitly."""
        data = {
            "veh": 1234,
            "tst": "2024-12-31T10:00:00.000Z",
            "tsi": 1735639200,
            "lat": 60.17,
            "long": 24.94,
            "spd": None,
            "hdg": None,
            "dl": None,
            "drst": None,
        }
        raw = RawHSLPayload.model_validate(data)
        pos = raw.to_vehicle_position()
        
        assert pos.speed_ms == 0.0
        assert pos.heading == 0
        assert pos.door_status is False


class TestInvalidEvent:
    """Tests for invalid event model (DLQ)."""

    def test_serialization(self):
        """Invalid event can be serialized to JSON."""
        invalid = InvalidEvent(
            raw_payload='{"test": 1}',
            error_message="Test error",
            error_field="veh"
        )
        json_str = invalid.model_dump_json()
        parsed = json.loads(json_str)
        
        assert parsed["error_message"] == "Test error"
        assert parsed["error_field"] == "veh"
        assert "received_at" in parsed