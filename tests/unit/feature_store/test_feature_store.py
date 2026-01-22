"""
Unit tests for Feature Store.
Hardened: Validates schema alignment with dbt marts and coordinate handling.
"""

import os
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
        # Accessing it as a property, not a function call
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
        from feature_store.online_store import OnlineFeatures

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
        from feature_store.online_store import OnlineStore

        config = FeatureStoreConfig(postgres_user="test", postgres_password="test")
        store = OnlineStore(config)
        assert store.is_healthy() is False

    @patch("psycopg2.connect")
    def test_offline_store_search_path(self, mock_connect):
        """Verify the store explicitly sets the search path to marts."""
        from feature_store.config import FeatureStoreConfig
        from feature_store.offline_store import OfflineStore

        # 1. Setup chain of mocks for 'with self._conn.cursor() as cur:'
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn

        # This mocks the context manager enter behavior
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        config = FeatureStoreConfig(
            postgres_user="test", postgres_password="test", postgres_schema="marts"
        )
        store = OfflineStore(config)
        store.connect()

        # 2. Check if execute was called on the cursor object within the context
        mock_cursor.execute.assert_any_call("SET search_path TO marts, public")
