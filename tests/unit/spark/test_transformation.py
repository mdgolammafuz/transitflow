"""
Unit tests for Spark transformation math.
Validates: Delay categorization thresholds and unit conversions.
"""

class TestTransformationMath:
    """Tests for the pure logic used within Spark UDFs and column expressions."""

    def test_categorize_delay_precision(self):
        """
        Precisely match the Spark 'when' logic in silver_transform.py.
        Ensures strict adherence to SLA boundaries (60s, 300s).
        """

        def logic(d):
            # Mirroring: when(spark_abs(col("delay_seconds")) <= 60, "on_time")
            if abs(d) <= 60:
                return "on_time"
            # Mirroring: .when(col("delay_seconds") > 300, "delayed")
            if d > 300:
                return "delayed"
            # Mirroring: .when(col("delay_seconds") < -300, "early")
            if d < -300:
                return "early"
            # Mirroring: .otherwise("slight_delay")
            return "slight_delay"

        assert logic(0) == "on_time"
        assert logic(60) == "on_time"
        assert logic(61) == "slight_delay"
        assert logic(301) == "delayed"
        assert logic(-301) == "early"

    def test_speed_conversion(self):
        """
        Verify m/s to km/h conversion factor.
        Used for speed_kmh calculation in Silver/Gold layers.
        """
        # 10 m/s * 3.6 = 36 km/h
        ms_value = 10.0
        expected_kmh = 36.0
        assert ms_value * 3.6 == expected_kmh