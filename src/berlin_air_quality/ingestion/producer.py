"""
Kafka Producer for Air Quality data.

Produces validated records to Kafka topics with Avro serialization.
Invalid records are routed to Dead Letter Queue.

Principle: Ingestion is a Contract
- Schema validation before producing
- DLQ for failed records
- Exactly-once semantics (idempotent producer)
"""

import json
import asyncio
from datetime import datetime
from typing import Optional, Any
import logging

# Optional Kafka imports
try:
    from confluent_kafka import Producer
    from confluent_kafka.serialization import SerializationContext, MessageField
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    Producer = None

from .validators import (
    PM25Record, WeatherRecord, DeadLetterRecord,
    validate_pm25, validate_weather
)

logger = logging.getLogger(__name__)


class AirQualityProducer:
    """
    Kafka producer for air quality data with schema validation.
    
    Features:
    - Avro serialization with Schema Registry
    - Pydantic validation before producing
    - Dead Letter Queue for invalid records
    - Delivery confirmation callbacks
    
    Usage:
        producer = AirQualityProducer()
        
        # Produce PM2.5 data
        success = producer.produce_pm25({
            "station_id": "DEBB001",
            "station_name": "Berlin Mitte",
            "timestamp": "2024-12-20T14:00:00Z",
            "value_ugm3": 18.5
        })
        
        # Flush to ensure delivery
        producer.flush()
    """
    
    # Topic names
    TOPIC_PM25 = "raw.berlin.pm25"
    TOPIC_WEATHER = "raw.berlin.weather"
    TOPIC_DLQ = "dlq.ingestion"
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        schema_registry_url: str = "http://localhost:8081",
        schemas_dir: str = "schemas"
    ):
        """
        Initialize the producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            schemas_dir: Directory containing Avro schema files
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.schemas_dir = schemas_dir
        
        # Statistics
        self.stats = {
            "pm25_produced": 0,
            "pm25_failed": 0,
            "weather_produced": 0,
            "weather_failed": 0,
            "dlq_produced": 0
        }
        
        # Initialize producer and serializers
        self._producer: Optional[Producer] = None
        self._schema_registry: Optional[SchemaRegistryClient] = None
        self._pm25_serializer: Optional[AvroSerializer] = None
        self._weather_serializer: Optional[AvroSerializer] = None
        self._dlq_serializer: Optional[AvroSerializer] = None
    
    def _init_producer(self):
        """Lazy initialization of producer."""
        if self._producer is not None:
            return
        
        # Producer config with idempotence for exactly-once
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'berlin-air-quality-producer',
            'enable.idempotence': True,  # Exactly-once semantics
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'retry.backoff.ms': 100,
            'compression.type': 'snappy'
        }
        
        self._producer = Producer(config)
        
        # Schema Registry client
        self._schema_registry = SchemaRegistryClient({
            'url': self.schema_registry_url
        })
        
        # Load schemas and create serializers
        self._init_serializers()
    
    def _init_serializers(self):
        """Initialize Avro serializers for each topic."""
        from pathlib import Path
        
        schemas_path = Path(self.schemas_dir)
        
        # PM2.5 serializer
        pm25_schema = (schemas_path / "pm25_measurement.avsc").read_text()
        self._pm25_serializer = AvroSerializer(
            self._schema_registry,
            pm25_schema,
            to_dict=lambda obj, ctx: obj  # Already a dict
        )
        
        # Weather serializer
        weather_schema = (schemas_path / "weather_observation.avsc").read_text()
        self._weather_serializer = AvroSerializer(
            self._schema_registry,
            weather_schema,
            to_dict=lambda obj, ctx: obj
        )
        
        # DLQ serializer
        dlq_schema = (schemas_path / "dead_letter.avsc").read_text()
        self._dlq_serializer = AvroSerializer(
            self._schema_registry,
            dlq_schema,
            to_dict=lambda obj, ctx: obj
        )
    
    def _delivery_callback(self, err, msg):
        """Callback for delivery confirmation."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(
                f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
            )
    
    def produce_pm25(self, data: dict, source: str = "eea") -> bool:
        """
        Validate and produce PM2.5 measurement.
        
        Args:
            data: Raw PM2.5 data dictionary
            source: Data source identifier
            
        Returns:
            True if produced successfully, False if sent to DLQ
        """
        self._init_producer()
        
        # Validate
        record, dlq_record = validate_pm25(data, source)
        
        if record:
            # Valid record - produce to main topic
            try:
                avro_data = record.to_avro_dict()
                self._producer.produce(
                    topic=self.TOPIC_PM25,
                    key=record.station_id.encode('utf-8'),
                    value=self._pm25_serializer(
                        avro_data,
                        SerializationContext(self.TOPIC_PM25, MessageField.VALUE)
                    ),
                    callback=self._delivery_callback
                )
                self.stats["pm25_produced"] += 1
                return True
            except Exception as e:
                logger.error(f"Failed to produce PM2.5: {e}")
                # Create DLQ record for serialization failure
                dlq_record = DeadLetterRecord(
                    original_topic=self.TOPIC_PM25,
                    error_type="SerializationError",
                    error_message=str(e),
                    raw_payload=json.dumps(data, default=str),
                    source=source
                )
        
        # Invalid record - send to DLQ
        if dlq_record:
            self._produce_dlq(dlq_record)
            self.stats["pm25_failed"] += 1
        
        return False
    
    def produce_weather(self, data: dict, source: str = "open-meteo") -> bool:
        """
        Validate and produce weather observation.
        
        Args:
            data: Raw weather data dictionary
            source: Data source identifier
            
        Returns:
            True if produced successfully, False if sent to DLQ
        """
        self._init_producer()
        
        # Validate
        record, dlq_record = validate_weather(data, source)
        
        if record:
            try:
                avro_data = record.to_avro_dict()
                # Use timestamp as key for partitioning
                key = str(int(record.timestamp.timestamp())).encode('utf-8')
                
                self._producer.produce(
                    topic=self.TOPIC_WEATHER,
                    key=key,
                    value=self._weather_serializer(
                        avro_data,
                        SerializationContext(self.TOPIC_WEATHER, MessageField.VALUE)
                    ),
                    callback=self._delivery_callback
                )
                self.stats["weather_produced"] += 1
                return True
            except Exception as e:
                logger.error(f"Failed to produce weather: {e}")
                dlq_record = DeadLetterRecord(
                    original_topic=self.TOPIC_WEATHER,
                    error_type="SerializationError",
                    error_message=str(e),
                    raw_payload=json.dumps(data, default=str),
                    source=source
                )
        
        if dlq_record:
            self._produce_dlq(dlq_record)
            self.stats["weather_failed"] += 1
        
        return False
    
    def _produce_dlq(self, dlq_record: DeadLetterRecord):
        """Send record to Dead Letter Queue."""
        try:
            avro_data = dlq_record.to_avro_dict()
            self._producer.produce(
                topic=self.TOPIC_DLQ,
                key=dlq_record.original_topic.encode('utf-8'),
                value=self._dlq_serializer(
                    avro_data,
                    SerializationContext(self.TOPIC_DLQ, MessageField.VALUE)
                ),
                callback=self._delivery_callback
            )
            self.stats["dlq_produced"] += 1
            logger.warning(
                f"Sent to DLQ: {dlq_record.error_type} - {dlq_record.error_message}"
            )
        except Exception as e:
            logger.error(f"Failed to produce to DLQ: {e}")
    
    def flush(self, timeout: float = 10.0) -> int:
        """
        Flush all pending messages.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Number of messages still in queue (0 if all flushed)
        """
        if self._producer:
            return self._producer.flush(timeout)
        return 0
    
    def poll(self, timeout: float = 0.0) -> int:
        """
        Poll for delivery callbacks.
        
        Args:
            timeout: Time to block in seconds
            
        Returns:
            Number of callbacks processed
        """
        if self._producer:
            return self._producer.poll(timeout)
        return 0
    
    def get_stats(self) -> dict:
        """Get production statistics."""
        return self.stats.copy()
    
    def close(self):
        """Close the producer."""
        if self._producer:
            self._producer.flush()
            # Note: confluent-kafka Producer doesn't have close()
            self._producer = None


# =============================================================================
# Simple Producer (JSON, no Avro - for testing without Schema Registry)
# =============================================================================

class SimpleProducer:
    """
    Simple JSON producer for testing without Schema Registry.
    
    Use this when Schema Registry is not available.
    """
    
    TOPIC_PM25 = "raw.berlin.pm25"
    TOPIC_WEATHER = "raw.berlin.weather"
    TOPIC_DLQ = "dlq.ingestion"
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[Producer] = None
        self.stats = {
            "pm25_produced": 0,
            "pm25_failed": 0,
            "weather_produced": 0,
            "weather_failed": 0,
            "dlq_produced": 0
        }
    
    def _init_producer(self):
        if self._producer is None:
            self._producer = Producer({
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'berlin-air-quality-simple-producer'
            })
    
    def produce_pm25(self, data: dict, source: str = "eea") -> bool:
        """Produce PM2.5 with JSON serialization."""
        self._init_producer()
        
        record, dlq_record = validate_pm25(data, source)
        
        if record:
            try:
                self._producer.produce(
                    topic=self.TOPIC_PM25,
                    key=record.station_id.encode('utf-8'),
                    value=json.dumps(record.to_avro_dict()).encode('utf-8')
                )
                self.stats["pm25_produced"] += 1
                return True
            except Exception as e:
                logger.error(f"Failed to produce: {e}")
                dlq_record = DeadLetterRecord(
                    original_topic=self.TOPIC_PM25,
                    error_type="ProductionError",
                    error_message=str(e),
                    raw_payload=json.dumps(data, default=str),
                    source=source
                )
        
        if dlq_record:
            self._produce_dlq(dlq_record)
            self.stats["pm25_failed"] += 1
        
        return False
    
    def produce_weather(self, data: dict, source: str = "open-meteo") -> bool:
        """Produce weather with JSON serialization."""
        self._init_producer()
        
        record, dlq_record = validate_weather(data, source)
        
        if record:
            try:
                key = str(int(record.timestamp.timestamp())).encode('utf-8')
                self._producer.produce(
                    topic=self.TOPIC_WEATHER,
                    key=key,
                    value=json.dumps(record.to_avro_dict()).encode('utf-8')
                )
                self.stats["weather_produced"] += 1
                return True
            except Exception as e:
                logger.error(f"Failed to produce: {e}")
                dlq_record = DeadLetterRecord(
                    original_topic=self.TOPIC_WEATHER,
                    error_type="ProductionError",
                    error_message=str(e),
                    raw_payload=json.dumps(data, default=str),
                    source=source
                )
        
        if dlq_record:
            self._produce_dlq(dlq_record)
            self.stats["weather_failed"] += 1
        
        return False
    
    def _produce_dlq(self, dlq_record: DeadLetterRecord):
        """Send to DLQ with JSON."""
        try:
            self._producer.produce(
                topic=self.TOPIC_DLQ,
                key=dlq_record.original_topic.encode('utf-8'),
                value=json.dumps(dlq_record.to_avro_dict()).encode('utf-8')
            )
            self.stats["dlq_produced"] += 1
        except Exception as e:
            logger.error(f"Failed to produce to DLQ: {e}")
    
    def flush(self, timeout: float = 10.0) -> int:
        if self._producer:
            return self._producer.flush(timeout)
        return 0
    
    def get_stats(self) -> dict:
        return self.stats.copy()
