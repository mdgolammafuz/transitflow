"""
Configuration for the ingestion service.
Uses pydantic-settings for environment variable binding.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MQTTConfig(BaseSettings):
    """HSL MQTT broker configuration."""

    model_config = SettingsConfigDict(env_prefix="MQTT_")

    host: str = Field(default="mqtt.hsl.fi")
    port: int = Field(default=1883)
    use_tls: bool = Field(default=False)

    # Topic filter: /hfp/v2/journey/ongoing/vp/{transport_mode}/#
    topic: str = Field(default="/hfp/v2/journey/ongoing/vp/bus/#")

    keepalive: int = Field(default=60)
    reconnect_delay_min: int = Field(default=1)
    reconnect_delay_max: int = Field(default=60)


class KafkaConfig(BaseSettings):
    """Kafka producer configuration."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = Field(default="localhost:9092")

    # Topic names must match infrastructure scripts
    topic_raw: str = Field(default="fleet.telemetry.raw")
    topic_dlq: str = Field(default="fleet.telemetry.dlq")

    acks: str = Field(default="all")
    retries: int = Field(default=3)
    linger_ms: int = Field(default=5)
    batch_size: int = Field(default=16384)

    schema_registry_url: str = Field(default="http://localhost:8081")


class MetricsConfig(BaseSettings):
    """Prometheus metrics configuration."""

    model_config = SettingsConfigDict(env_prefix="METRICS_")

    enabled: bool = Field(default=True)
    # Changed default to 9091 to prevent collision with Feature API (8000)
    port: int = Field(default=9091)


class Settings(BaseSettings):
    """Root settings."""

    # This prefix means Pydantic looks for APP_KAFKA_..., APP_METRICS_..., etc.
    model_config = SettingsConfigDict(env_prefix="APP_")

    mqtt: MQTTConfig = Field(default_factory=MQTTConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)

    # Feature Flags / Filters
    filter_line: str = Field(default="")  # Empty = all lines
    log_level: str = Field(default="INFO")


def get_settings() -> Settings:
    return Settings()
