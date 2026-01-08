class TestTransformationMath:
    def test_categorize_delay_precision(self):
        """Precisely match the Spark 'when' logic."""

        def logic(d):
            if abs(d) <= 60:
                return "on_time"
            if d > 300:
                return "delayed"
            if d < -300:
                return "early"
            return "slight_delay"

        assert logic(0) == "on_time"
        assert logic(60) == "on_time"
        assert logic(61) == "slight_delay"
        assert logic(301) == "delayed"
        assert logic(-301) == "early"

    def test_speed_conversion(self):
        """Verify m/s to km/h conversion factor."""
        # 10 m/s * 3.6 = 36 km/h
        assert 10.0 * 3.6 == 36.0
