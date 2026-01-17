"""
Feature Store configuration.

Pattern: Zero-Secret Architecture
Pattern: Leaf Module (No project imports)
Robust: Includes helper for search_path to ensure mart isolation.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FeatureStoreConfig(BaseSettings):
    """
    Configuration for Feature Store.
    Synchronized with Java Flink Sink and Spark Sync Job.
    """

    # Redis Configuration (Online Store)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_key_prefix: str = "features:vehicle:"
    redis_ttl_seconds: int = 300
    redis_password: str = Field(default="")

    # PostgreSQL Configuration (Offline Store)
    postgres_user: str
    postgres_password: str
    postgres_db: str = "transit"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    # DBT/Schema Configuration
    # Principal Fix: Ensuring this matches the dbt project schema
    postgres_schema: str = "marts"

    # API and Service Settings
    cache_ttl_seconds: int = 60
    request_timeout_seconds: float = 2.0  # Increased slightly for spatial joins

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8", 
        extra="ignore"
    )

    @classmethod
    def from_env(cls) -> "FeatureStoreConfig":
        return cls()

    def get_redis_params(self) -> dict:
        """Helper for redis-py connection parameters."""
        params = {
            "host": self.redis_host,
            "port": self.redis_port,
            "decode_responses": True,
            "socket_timeout": self.request_timeout_seconds,
        }
        if self.redis_password:
            params["password"] = self.redis_password
        return params

    def get_postgres_params(self) -> dict:
        """Helper for psycopg2 connection parameters."""
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "user": self.postgres_user,
            "password": self.postgres_password,
            "dbname": self.postgres_db,
            "connect_timeout": int(self.request_timeout_seconds),
        }