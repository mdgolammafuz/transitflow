from datetime import datetime, timedelta

import pytest


class TestRetentionPolicies:
    def test_gold_retention_365_days(self):
        """Gold layer has 365 day retention in 2026."""
        gold_retention_days = 365
        today = datetime(2026, 1, 7)
        cutoff = today - timedelta(days=gold_retention_days)
        assert cutoff == datetime(2025, 1, 7)


class TestRetentionLogic:
    @pytest.mark.parametrize(
        "partition_date,retention_days,should_delete",
        [
            ("2026-01-06", 7, False),  # 1 day old
            ("2025-12-20", 7, True),  # Expired
        ],
    )
    def test_partition_retention_decision(self, partition_date, retention_days, should_delete):
        reference_date = datetime(2026, 1, 7)
        cutoff = (reference_date - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        assert (partition_date < cutoff) == should_delete
