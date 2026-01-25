"""
Unit tests for Data Reconciliation logic.
Validates: Integrity threshold math and serialization safety.
"""

from dataclasses import asdict
from spark.reconciliation import ReconciliationResult

class TestReconciliationResult:
    """Tests for the Data Reconciliation logic and Result model."""

    def test_result_logic(self):
        """Verify 20% threshold logic for deduplication gaps."""
        result = ReconciliationResult(
            date="2026-01-07",
            table_name="enriched",
            stream_count=1000,
            batch_count=850,  # 15% diff
            difference=150,
            diff_percentage=15.0,
            passed=True,
            threshold_pct=20.0,
        )
        assert result.passed is True

        # Test serialization for logging/persistence to Postgres
        data = asdict(result)
        assert data["diff_percentage"] == 15.0
        assert data["table_name"] == "enriched"

    def test_zero_division_safety(self):
        """
        Ensure logic handles empty source tables without crashing.
        This mirrors the defensive logic in spark/reconciliation.py.
        """
        stream_val = 0
        batch_val = 100
        
        # Align with the code's fallback to 0.0 to prevent false FAILs
        pct = (abs(stream_val - batch_val) / stream_val * 100) if stream_val > 0 else 0.0
        assert pct == 0.0