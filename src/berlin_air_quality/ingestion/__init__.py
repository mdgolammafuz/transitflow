"""
Ingestion module for Berlin Air Quality Predictor.

Handles data ingestion through Kafka with schema validation.
"""

from .validators import validate_pm25, validate_weather

# Optional imports (require confluent-kafka)
try:
    from .producer import AirQualityProducer, SimpleProducer
    from .schema_registry import SchemaRegistryClient
    __all__ = [
        "AirQualityProducer",
        "SimpleProducer",
        "SchemaRegistryClient", 
        "validate_pm25",
        "validate_weather"
    ]
except ImportError:
    __all__ = [
        "validate_pm25",
        "validate_weather"
    ]
