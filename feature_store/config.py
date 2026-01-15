"""
Feature Store configuration.

All configuration via environment variables or .env files.
Uses Pydantic Settings for validation and type safety.
"""

import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class FeatureStoreConfig(BaseSettings):
    """
    Configuration for Feature Store.
    Automatically loads from environment variables or .env file.
    """

    # Redis Configuration (Online Store)
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", 6379))
    redis_key_prefix: str = "features:vehicle:"
    redis_ttl_seconds: int = 300
    redis_password: str = os.getenv("REDIS_PASSWORD", "")

    # PostgreSQL Configuration (Offline Store)
    postgres_user: str = os.getenv("POSTGRES_USER", "transit")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "transit_secure_local")
    postgres_db: str = os.getenv("POSTGRES_DB", "transit")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", 5432))

    # DBT/Schema Configuration
    # Pattern: Explicitly defining the schema allows the Offline Store
    # to find the dbt Marts regardless of the default public search path.
    postgres_schema: str = os.getenv("POSTGRES_SCHEMA", "marts")

    # API and Service Settings
    cache_ttl_seconds: int = 60
    request_timeout_seconds: float = 1.0

    # Load from .env file if it exists
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @classmethod
    def from_env(cls) -> "FeatureStoreConfig":
        """Helper to maintain API consistency with previous implementation."""
        return cls()

    def redis_url(self) -> str:
        """Get Redis connection URL with authentication."""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/0"
        return f"redis://{self.redis_host}:{self.redis_port}/0"

    def postgres_dsn(self) -> str:
        """Get PostgreSQL connection string formatted for SQLAlchemy/Psycopg."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
