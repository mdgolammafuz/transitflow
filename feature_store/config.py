"""
Feature Store configuration.

Pattern: Zero-Secret Architecture
Pattern: Leaf Module (No internal project imports to prevent circularity)
Uses Pydantic Settings for validation and type safety.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FeatureStoreConfig(BaseSettings):
    """
    Configuration for Feature Store.
    Automatically loads from environment variables or .env file.
    No hardcoded defaults for sensitive credentials.
    """

    # Redis Configuration (Online Store)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_key_prefix: str = "features:vehicle:"
    redis_ttl_seconds: int = 300
    # No default password
    redis_password: str = Field(default="")

    # PostgreSQL Configuration (Offline Store)
    postgres_user: str
    postgres_password: str
    postgres_db: str = "transit"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    # DBT/Schema Configuration
    postgres_schema: str = "marts"

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
        """Factory method to load and validate config from environment."""
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