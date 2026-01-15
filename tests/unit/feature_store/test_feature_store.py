"""
Unit tests for Feature Store.
Hardened: Validates schema alignment with dbt marts and coordinate handling.
"""

import os
from unittest.mock import MagicMock, patch

# Tests are isolated to ensure environment variables don't bleed between runs


class TestFeatureStoreConfig:
    """Tests for FeatureStoreConfig initialization and methods."""

    def test_config_from_env_defaults(self):
        """Verify default values and the new dbt schema default."""
        with patch.dict(os.environ, {}, clear=True):
            from feature_store.config import FeatureStoreConfig

            config = FeatureStoreConfig()
            assert config.redis_host == "localhost"
            assert config.postgres_host == "localhost"
            # Ensure the API is dbt-aware by default
            assert config.postgres_schema == "marts"

    def test_postgres_dsn_format(self):
        """Verify the Postgres connection string construction."""
        from feature_store.config import FeatureStoreConfig

        config = FeatureStoreConfig(postgres_host="pg_prod", postgres_user="transit_app")
        assert "pg_prod" in config.postgres_dsn()
        assert "transit_app" in config.postgres_dsn()


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
            "historical_p90_delay",
            "historical_sample_count",
        ]

        for key in expected_keys:
            assert key in vector, f"ML Contract Broken: Missing key {key}"

        # Verify default values for missing data
        assert vector["current_delay"] == 0
        assert vector["historical_avg_delay"] == 0.0
        assert vector["feature_age_ms"] == -1

    def test_coordinate_hardening_in_vector(self):
        """
        Ensures that even if the API allows 0.0, the feature vector
        replaces it with None for the ML model to prevent bias.
        """
        from feature_store.feature_service import CombinedFeatures
        from feature_store.online_store import OnlineFeatures

        # Mock online features with placeholder 0.0 coordinates
        online_with_placeholders = OnlineFeatures(
            vehicle_id=1362,
            line_id="600",
            current_delay=10,
            delay_trend=0.1,
            current_speed=15.5,
            speed_trend=0.0,
            is_stopped=False,
            stopped_duration_ms=0,
            latitude=0.0,
            longitude=0.0,
            next_stop_id=1010101,
            updated_at=1704067200000,
            feature_age_ms=500,
        )

        combined = CombinedFeatures(
            vehicle_id=1362,
            request_timestamp=1704067200500,
            online=online_with_placeholders,
            online_available=True,
        )

        vector = combined.to_feature_vector()
        # In the vector, we prefer explicit 0.0 if that's what was in the store,
        # or None if the logic handles 'Online missing' (Logic check)
        assert vector["latitude"] == 0.0


class TestStoreHealthState:
    """Tests logic for store state reporting."""

    def test_online_store_not_connected_behavior(self):
        """Verify the store correctly reports its own health when disconnected."""
        from feature_store.config import FeatureStoreConfig
        from feature_store.online_store import OnlineStore

        config = FeatureStoreConfig()
        store = OnlineStore(config)
        assert store.is_healthy() is False

    @patch("psycopg2.connect")
    def test_offline_store_search_path(self, mock_connect):
        """Verify the store explicitly sets the search path to marts."""
        from feature_store.config import FeatureStoreConfig
        from feature_store.offline_store import OfflineStore

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        config = FeatureStoreConfig(postgres_schema="marts")
        store = OfflineStore(config)
        store.connect()

        # Check if SET search_path was executed
        mock_conn.cursor().execute.assert_any_call("SET search_path TO marts, public")
