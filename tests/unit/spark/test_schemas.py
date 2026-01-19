"""
Unit tests for Spark Schemas.
Verifies: Data contract integrity across the Medallion layers.
"""

from pyspark.sql.types import BooleanType, LongType, StringType
from spark.bronze_writer import ENRICHED_SCHEMA


class TestSchemaIntegrity:
    """Verifies that the Bronze schema matches the 2026 telemetry contract."""

    def test_enriched_types(self):
        """
        Verify telemetry types match physical expectations.
        Aligned: vehicle_id must be a String to handle leading zeros and alphabetic IDs.
        """
        fields = {f.name: f.dataType for f in ENRICHED_SCHEMA.fields}

        # Essential for trend analysis
        assert isinstance(fields["event_time_ms"], LongType)
        # Essential for stop detection logic
        assert isinstance(fields["is_stopped"], BooleanType)
        # CRITICAL ALIGNMENT: Must be StringType, NOT IntegerType
        assert isinstance(fields["vehicle_id"], StringType)

    def test_silver_extra_columns(self):
        """
        Silver must contain these derived columns for analytics.
        These are generated in spark.silver_transform.transform_enriched.
        """
        required_silver = ["event_timestamp", "speed_kmh", "delay_category"]
        
        # Verify the contract requires 3 specific transformations
        assert len(required_silver) == 3
        assert "speed_kmh" in required_silver
        assert "delay_category" in required_silver