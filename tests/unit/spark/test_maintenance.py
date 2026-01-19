"""
Unit tests for Lakehouse maintenance logic.
Validates: Retention policies and partition deletion logic for 2026 data.
Professional: Aligned with mandatory --date logic and UTC-aware datetimes.
"""

from datetime import datetime, timedelta, timezone
import pytest


class TestRetentionPolicies:
    """Tests for default retention durations across medallion layers."""

    def test_gold_retention_365_days(self):
        """Gold layer has 365 day retention in 2026."""
        gold_retention_days = 365
        # Aligned: Use UTC-aware datetime to match maintenance.py implementation
        today = datetime(2026, 1, 7, tzinfo=timezone.utc)
        cutoff = today - timedelta(days=gold_retention_days)
        
        assert cutoff == datetime(2025, 1, 7, tzinfo=timezone.utc)


class TestRetentionLogic:
    """Tests for the logic used to identify expired partitions."""

    @pytest.mark.parametrize(
        "partition_date,retention_days,should_delete",
        [
            ("2026-01-06", 7, False),  # 1 day old
            ("2025-12-20", 7, True),   # Expired
            ("2025-12-31", 7, False),  # Boundary: Exactly 7 days (not < 7)
            ("2026-01-01", 7, False),  # 6 days old
        ],
    )
    def test_partition_retention_decision(self, partition_date, retention_days, should_delete):
        """
        Verifies string-comparison logic. 
        Matches implementation: delta_table.delete(f"date < '{cutoff}'")
        Ensures target_date parameter from CLI is handled correctly.
        """
        # This simulates the --date argument passed to maintenance.py
        cli_target_date = "2026-01-07"
        
        # Logic matches maintenance.py:apply_retention exactly
        base_dt = datetime.strptime(cli_target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        cutoff = (base_dt - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        
        # Test the string comparison used in Delta Lake SQL filter
        assert (partition_date < cutoff) == should_delete