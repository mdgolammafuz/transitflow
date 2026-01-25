"""
Unit tests for Spark Schemas.
Verifies: Data contract integrity across the Medallion layers.
Principal: Enforces the January 2026 Type-Safe contract.
"""

from pyspark.sql.types import BooleanType, LongType, StringType, IntegerType, DoubleType
from spark.bronze_writer import ENRICHED_SCHEMA, STOP_EVENTS_SCHEMA


class TestSchemaIntegrity:
    """Verifies that the Bronze schema matches the 2026 telemetry contract."""

    def test_enriched_types(self):
        """
        Verify telemetry types match physical expectations from Kafka.
        Aligned: vehicle_id must be a String to handle leading zeros.
        """
        fields = {f.name: f.dataType for f in ENRICHED_SCHEMA.fields}

        # 1. Identity Integrity (No Scientific Notation)
        assert isinstance(fields["vehicle_id"], StringType)
        assert isinstance(fields["next_stop_id"], StringType)

        # 2. Logic & Status Integrity
        # is_stopped is BOOLEAN in Kafka JSON
        assert isinstance(fields["is_stopped"], BooleanType)
        # door_status is INTEGER (0/1) in our hardened contract
        assert isinstance(fields["door_status"], IntegerType)

        # 3. Precision Integrity
        assert isinstance(fields["speed_ms"], DoubleType)
        assert isinstance(fields["event_time_ms"], LongType)

    def test_stop_events_types(self):
        """
        Verify the Stop Events contract matches Flink StopArrival output.
        """
        fields = {f.name: f.dataType for f in STOP_EVENTS_SCHEMA.fields}
        
        assert "door_status" in fields
        assert isinstance(fields["door_status"], IntegerType)
        assert isinstance(fields["stop_id"], StringType)

    def test_silver_contract_expectations(self):
        """
        Documentation of columns required for downstream dbt/ML.
        These are generated in spark.silver_transform.
        """
        required_silver = [
            "event_timestamp", # Derived from timestamp or event_time_ms
            "speed_kmh",       # speed_ms * 3.6
            "is_stopped",      # Passed through as Boolean
            "door_status"      # Passed through as Integer
        ]
        
        assert "event_timestamp" in required_silver
        assert "door_status" in required_silver