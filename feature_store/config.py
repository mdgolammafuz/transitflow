"""
Feature Store configuration.

All configuration via environment variables or .env files.
Uses Pydantic Settings for validation and type safety.
"""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class FeatureStoreConfig(BaseSettings):
    """
    Configuration for Feature Store.
    Automatically loads from environment variables or .env file.
    """
    # Redis Configuration
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_key_prefix: str = "features:vehicle:"
    redis_ttl_seconds: int = 300

    # PostgreSQL Configuration
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "transit"
    postgres_password: str = ""
    postgres_db: str = "transit"

    # API and Service Settings
    cache_ttl_seconds: int = 60
    request_timeout_seconds: float = 1.0

    # Load from .env file if it exists
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @classmethod
    def from_env(cls) -> "FeatureStoreConfig":
        """Helper to maintain API consistency with previous implementation."""
        return cls()

    def redis_url(self) -> str:
        """Get Redis connection URL."""
        return f"redis://{self.redis_host}:{self.redis_port}/0"

    def postgres_dsn(self) -> str:
        """Get PostgreSQL connection string."""
        # Using a DSN format compatible with most async and sync drivers
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )