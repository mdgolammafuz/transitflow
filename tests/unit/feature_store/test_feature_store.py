"""
Unit tests for Feature Store.

Validates the internal logic of configuration, data structures, 
and service orchestration using mocks.
"""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

# Note: We import inside tests to ensure the mock environment is set up first

class TestFeatureStoreConfig:
    """Tests for FeatureStoreConfig initialization and methods."""

    def test_config_from_env_defaults(self):
        """Verify default values are used when environment is empty."""
        with patch.dict(os.environ, {}, clear=True):
            from feature_store.config import FeatureStoreConfig
            config = FeatureStoreConfig() # Pydantic BaseSettings handles defaults
            assert config.redis_host == "localhost"
            assert config.postgres_host == "localhost"

    def test_redis_url_format(self):
        """Verify the Redis connection string construction."""
        from feature_store.config import FeatureStoreConfig
        config = FeatureStoreConfig(redis_host="myredis", redis_port=6379)
        assert config.redis_url() == "redis://myredis:6379/0"

    def test_config_immutable(self):
        """Ensure config cannot be changed at runtime (Data Integrity)."""
        from feature_store.config import FeatureStoreConfig
        config = FeatureStoreConfig()
        with pytest.raises(Exception):
            config.redis_host = "hacker_host"


class TestFeatureVectorConsistency:
    """Tests for Training-Serving Consistency (Pattern: DE#8)."""

    def test_feature_vector_keys_stay_constant(self):
        """
        Critical test: The ML model expects a fixed set of keys.
        Empty or missing data must still return the full schema with defaults.
        """
        from feature_store.feature_service import CombinedFeatures

        combined_empty = CombinedFeatures(
            vehicle_id=1362,
            request_timestamp=1704067200500,
            online=None,
            offline=None,
            online_available=False,
            offline_available=False,
        )

        vector = combined_empty.to_feature_vector()

        expected_keys = [
            "vehicle_id", "current_delay", "delay_trend", "current_speed",
            "speed_trend", "is_stopped", "stopped_duration_ms", "latitude",
            "longitude", "feature_age_ms", "historical_avg_delay",
            "historical_stddev_delay", "historical_p90_delay", "historical_sample_count"
        ]

        for key in expected_keys:
            assert key in vector, f"ML Contract Broken: Missing key {key}"
        
        # Verify default values for missing data
        assert vector["current_delay"] == 0
        assert vector["historical_avg_delay"] == 0.0
        assert vector["feature_age_ms"] == -1


class TestStoreHealthState:
    """Tests logic for store state reporting."""

    def test_online_store_not_connected_behavior(self):
        """Verify the store correctly reports its own health when disconnected."""
        from feature_store.config import FeatureStoreConfig
        from feature_store.online_store import OnlineStore

        config = FeatureStoreConfig()
        store = OnlineStore(config)
        assert store.is_healthy() is False