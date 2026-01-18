"""
Unit tests for ingestion module.
Pattern: Clean Code / Domain Validation
Aligned: Updated to verify String-IDs, forced UTC, and HSL-compliant integer door status.
Fixed: Corrected 2026 epoch millisecond assertion.
"""

import json
from datetime import datetime, timezone

import pytest

from src.ingestion.models import InvalidEvent, RawHSLPayload, VehiclePosition


class TestVehiclePosition:
    """Tests for VehiclePosition model validation and UTC alignment."""

    def test_valid_position(self):
        """Basic valid position for Jan 2026 with String-ID contract."""
        pos = VehiclePosition(
            vehicle_id="1234",  # Aligned: ID as String
            timestamp=datetime(2026, 1, 17, 22, 0, 0, tzinfo=timezone.utc),
            latitude=60.17,
            longitude=24.94,
            speed_ms=12.5,
            heading=180,
            delay_seconds=120,
            door_status=0,
            line_id="600",
            direction_id=1,
            operator_id=22,
        )
        # Aligned: Asserting String ID
        assert pos.vehicle_id == "1234"
        assert pos.delay_seconds == 120
        # Unix Epoch for 2026-01-17 22:00:00 UTC
        # Calculation: 1768687200 seconds * 1000
        assert pos.event_time_ms == 1768687200000

    def test_timestamp_parsing_utc(self):
        """Ensure ISO timestamp strings are forced to UTC awareness."""
        # Test with 'Z' suffix
        pos_z = VehiclePosition(
            vehicle_id="1234",
            timestamp="2026-01-17T22:00:00Z",
            latitude=60.17,
            longitude=24.94,
            speed_ms=0,
            heading=0,
            delay_seconds=0,
            door_status=0,
            line_id="600",
            direction_id=1,
            operator_id=22,
        )
        assert pos_z.timestamp.tzinfo == timezone.utc

        # Test with naive string (forced to UTC by our validator)
        pos_naive = VehiclePosition(
            vehicle_id="1234",
            timestamp="2026-01-17T22:00:00",
            latitude=60.17,
            longitude=24.94,
            speed_ms=0,
            heading=0,
            delay_seconds=0,
            door_status=0,
            line_id="600",
            direction_id=1,
            operator_id=22,
        )
        assert pos_naive.timestamp.tzinfo == timezone.utc

    def test_latitude_bounds(self):
        """Latitude must be within Helsinki region (59-61)."""
        with pytest.raises(ValueError):
            VehiclePosition(
                vehicle_id="1234",
                timestamp=datetime.now(timezone.utc),
                latitude=50.0,  # FAIL: Outside Helsinki
                longitude=24.94,
                speed_ms=0,
                heading=0,
                delay_seconds=0,
                door_status=0,
                line_id="600",
                direction_id=1,
                operator_id=22,
            )

    def test_speed_bounds(self):
        """Speed must be reasonable (<= 40m/s)."""
        with pytest.raises(ValueError):
            VehiclePosition(
                vehicle_id="1234",
                timestamp=datetime.now(timezone.utc),
                latitude=60.17,
                longitude=24.94,
                speed_ms=100.0,  # FAIL: 360 km/h is unrealistic
                heading=0,
                delay_seconds=0,
                door_status=0,
                line_id="600",
                direction_id=1,
                operator_id=22,
            )


class TestRawHSLPayload:
    """Tests for raw HSL payload parsing and mapping logic."""

    def test_convert_to_vehicle_position(self):
        """Convert raw payload (ints) to validated model (Strings/UTC)."""
        data = {
            "desi": "600",
            "dir": "1",
            "oper": 22,
            "veh": 1234,  # Raw input is integer
            "tst": "2026-01-17T22:00:00.000Z",
            "tsi": 1737151200,
            "spd": 12.5,
            "hdg": 180,
            "lat": 60.17,
            "long": 24.94,
            "dl": 120,
            "drst": 1,
        }
        raw = RawHSLPayload.model_validate(data)
        pos = raw.to_vehicle_position()

        # Aligned: Raw int 1234 must be string "1234"
        assert pos.vehicle_id == "1234"
        assert pos.line_id == "600"
        assert pos.door_status == 1  # Maintained as int per HSL spec

    def test_null_handling(self):
        """Handle null optional fields by providing safe defaults."""
        data = {
            "veh": 1234,
            "tst": "2026-01-17T22:00:00.000Z",
            "tsi": 1737151200,
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
        assert pos.door_status == 0


class TestInvalidEvent:
    """Tests for Dead Letter Queue (DLQ) model serialization."""

    def test_serialization(self):
        """Invalid event can be serialized to JSON for DLQ production."""
        invalid = InvalidEvent(
            raw_payload='{"test": 1}', error_message="Test error", error_field="veh"
        )
        json_str = invalid.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["error_message"] == "Test error"
        assert parsed["error_field"] == "veh"
        # Verify default factory for UTC time is working
        assert "received_at" in parsed
        assert parsed["received_at"].endswith("+00:00") or parsed["received_at"].endswith("Z")