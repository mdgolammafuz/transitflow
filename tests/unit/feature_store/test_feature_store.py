"""
Unit tests for Feature Store.
Hardened: Validates schema alignment with dbt marts and coordinate handling.
Updated: Adapted for Connection Pooling architecture.
"""

import os
import pytest
from unittest.mock import MagicMock, patch


class TestFeatureStoreConfig:
    """Tests for FeatureStoreConfig initialization and methods."""

    def test_config_from_env_defaults(self):
        """Verify default values and required field validation."""
        env_mock = {"POSTGRES_USER": "test_user", "POSTGRES_PASSWORD": "test_password"}
        with patch.dict(os.environ, env_mock, clear=True):
            from feature_store.config import FeatureStoreConfig

            config = FeatureStoreConfig()
            assert config.redis_host == "localhost"
            assert config.postgres_host == "localhost"
            assert config.postgres_schema == "marts"

    def test_postgres_dsn_format(self):
        """Verify the Postgres connection string construction."""
        from feature_store.config import FeatureStoreConfig

        config = FeatureStoreConfig(
            postgres_host="pg_prod",
            postgres_user="transit_app",
            postgres_password="secure_password",
        )
        assert "pg_prod" in config.postgres_dsn
        assert "transit_app" in config.postgres_dsn


class TestFeatureVectorConsistency:
    """Tests for Training-Serving Consistency (Pattern: DE#8)."""

    def test_feature_vector_keys_stay_constant(self):
        """
        Verify the ML Contract: The model expects a fixed set of keys.
        Aligns with the new naming used in fct_stop_arrivals.
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
            "vehicle_id",
            "current_delay",
            "delay_trend",
            "current_speed",
            "speed_trend",
            "is_stopped",
            "stopped_duration_ms",
            "latitude",
            "longitude",
            "feature_age_ms",
            "historical_avg_delay",
            "historical_stddev_delay",
            "avg_dwell_time_ms",
            "historical_arrival_count",
        ]

        for key in expected_keys:
            assert key in vector, f"ML Contract Broken: Missing key {key}"

        assert vector["current_delay"] == 0
        assert vector["historical_avg_delay"] == 0.0
        assert vector["feature_age_ms"] == -1

    def test_coordinate_hardening_in_vector(self):
        """
        Ensures coordinates are correctly propagated in the feature vector.
        """
        from feature_store.feature_service import CombinedFeatures
        from feature_store.storage.online_store import OnlineFeatures

        online_data = OnlineFeatures(
            vehicle_id=1362,
            line_id="600",
            current_delay=10,
            delay_trend=0.1,
            current_speed=15.5,
            speed_trend=0.0,
            is_stopped=False,
            stopped_duration_ms=0,
            latitude=60.1699,
            longitude=24.9384,
            next_stop_id="1010101",
            updated_at=1704067200000,
            feature_age_ms=500,
        )

        combined = CombinedFeatures(
            vehicle_id=1362,
            request_timestamp=1704067200500,
            online=online_data,
            online_available=True,
        )

        vector = combined.to_feature_vector()
        assert vector["latitude"] == 60.1699
        assert vector["longitude"] == 24.9384


class TestStoreHealthState:
    """Tests logic for store state reporting."""

    def test_online_store_not_connected_behavior(self):
        """Verify the store correctly reports its own health when disconnected."""
        from feature_store.config import FeatureStoreConfig
        from feature_store.storage.online_store import OnlineStore

        config = FeatureStoreConfig(postgres_user="test", postgres_password="test")
        store = OnlineStore(config)
        # Without calling connect(), client is None
        assert store.is_healthy() is False

    @patch("psycopg2.pool.SimpleConnectionPool")
    def test_offline_store_pooling_initialization(self, mock_pool_cls):
        """
        Verify the store initializes the Connection Pool correctly.
        Updated for Phase 5 Architecture.
        """
        from feature_store.config import FeatureStoreConfig
        from feature_store.storage.offline_store import OfflineStore

        config = FeatureStoreConfig(
            postgres_user="test", postgres_password="test", postgres_schema="marts"
        )
        store = OfflineStore(config)
        store.connect()

        # Check if the pool was initialized with correct params
        mock_pool_cls.assert_called_once()
        call_kwargs = mock_pool_cls.call_args[1]
        assert call_kwargs["minconn"] == 1
        assert call_kwargs["maxconn"] == 10
        assert call_kwargs["user"] == "test"