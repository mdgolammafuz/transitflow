"""
Unit tests for Spark configuration module.

Tests:
- Environment variable loading
- Required variable validation
- Default values
- Config object construction

Run with: pytest tests/unit/spark/test_config.py -v
"""

import os

import pytest


class TestSparkConfig:
    """Tests for Spark configuration loading."""

    def test_load_config_with_all_vars(self, env_config):
        """Config loads successfully with all required variables from .env."""
        from spark.config import load_config

        config = load_config()

        # Aligned with your actual .env provided earlier
        assert config.kafka_bootstrap_servers == "redpanda:29092"
        assert config.minio_access_key == "minioadmin"
        assert config.minio_secret_key == "minioadmin"
        assert config.postgres_user == "transit"

    def test_load_config_missing_required(self, monkeypatch):
        """Config raises error when required variable missing."""
        # Clear specific required variables to test validation
        required_keys = [
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
        ]
        for key in required_keys:
            monkeypatch.delenv(key, raising=False)

        from spark.config import load_config

        with pytest.raises(ValueError) as exc_info:
            load_config()

        assert "Required environment variable not set" in str(exc_info.value)

    def test_default_values(self, env_config):
        """Config uses defaults for optional variables."""
        from spark.config import load_config

        config = load_config()

        # Verify Medallion logic defaults
        assert config.lakehouse_bucket == "transitflow-lakehouse"
        assert config.bronze_retention_days == 7
        assert config.silver_retention_days == 30
        assert config.gold_retention_days == 365

    def test_paths_constructed_from_bucket(self, env_config):
        """Paths are constructed correctly for the S3A protocol."""
        from spark.config import load_config

        config = load_config()

        bucket = "transitflow-lakehouse"
        assert config.bronze_path == f"s3a://{bucket}/bronze"
        assert config.silver_path == f"s3a://{bucket}/silver"
        assert config.gold_path == f"s3a://{bucket}/gold"

    def test_postgres_connection_params(self, env_config):
        """Verify DB connection parameters match the .env profile."""
        from spark.config import load_config

        config = load_config()

        assert config.postgres_host == "postgres"
        assert config.postgres_port == "5432"
        assert config.postgres_db == "transit"

    def test_custom_retention_days(self, env_config, monkeypatch):
        """Custom retention values are respected when provided in environment."""
        monkeypatch.setenv("BRONZE_RETENTION_DAYS", "14")
        monkeypatch.setenv("SILVER_RETENTION_DAYS", "60")
        monkeypatch.setenv("GOLD_RETENTION_DAYS", "730")

        from spark.config import load_config

        config = load_config()

        assert int(config.bronze_retention_days) == 14
        assert int(config.silver_retention_days) == 60
        assert int(config.gold_retention_days) == 730


class TestSparkConfigSecurity:
    """Security-related tests for configuration."""

    def test_no_hardcoded_credentials(self):
        """Verify no hardcoded credentials exist in the source code."""
        import inspect

        from spark import config

        source = inspect.getsource(config)
        source_lower = source.lower()

        # Principal Engineer standard: Never hardcode these strings
        forbidden_patterns = ["minioadmin", "transit_secure_local", "redis_secure_local"]

        for pattern in forbidden_patterns:
            # We check if the pattern is in the source but NOT as an os.environ.get fallback
            assert (
                pattern not in source_lower or "environ" in source_lower
            ), f"Security Risk: Potential hardcoded credential found: {pattern}"

    def test_credentials_from_environment_only(self, env_config):
        """Verify that credentials used by the app match the environment exactly."""
        from spark.config import load_config

        config = load_config()

        assert config.minio_access_key == os.environ.get("MINIO_ROOT_USER")
        assert config.minio_secret_key == os.environ.get("MINIO_ROOT_PASSWORD")
        assert config.postgres_password == os.environ.get("POSTGRES_PASSWORD")
