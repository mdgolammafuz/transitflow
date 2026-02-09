"""
Kafka producer for vehicle telemetry.
Pattern: Idempotent Producer (Exactly-once delivery intent)
Pattern: Keyed Partitioning (Ensures vehicle-state affinity for Flink)
"""

import logging
import sys
from typing import Optional

try:
    from confluent_kafka import KafkaError, Message, Producer
except ImportError:
    print("CRITICAL: confluent-kafka not installed.")
    sys.exit(1)

from .config import KafkaConfig
from .metrics import get_metrics
from .models import InvalidEvent, VehiclePosition

logger = logging.getLogger(__name__)


class TelemetryProducer:
    """
    Kafka producer for vehicle position events using JSON serialization.
    Hardened for high-throughput Helsinki telemetry.
    """

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.metrics = get_metrics()
        self._producer = self._init_producer()

    def _init_producer(self) -> Producer:
        """Initialize Kafka producer with industry-standard reliability settings."""
        conf = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "acks": self.config.acks,  # Usually 'all' for high durability
            "retries": self.config.retries,
            "linger.ms": self.config.linger_ms,  # Optimize for batching throughput
            "batch.size": self.config.batch_size,
            "compression.type": "zstd",  # Highest compression ratio for JSON
            # --- Reliability & Performance ---
            "enable.idempotence": True,  # Prevents duplicates on retries
            "max.in.flight.requests.per.connection": 5,
            "queue.buffering.max.messages": 100000,
        }

        try:
            producer = Producer(conf)
            logger.info("Kafka producer initialized: %s", self.config.bootstrap_servers)
            return producer
        except Exception as e:
            logger.critical("Failed to connect to Kafka: %s", e)
            raise

    def _delivery_callback(self, err: Optional[KafkaError], msg: Message):
        """Callback for delivery confirmation from the broker."""
        if err:
            self.metrics.record_kafka_error(str(err))
            logger.error("Message delivery failed [Topic: %s]: %s", msg.topic(), err)
        else:
            # Successfully produced to raw or dlq topic
            self.metrics.record_produced()

    def produce(self, position: VehiclePosition):
        """
        Produce vehicle position to Kafka.
        Uses vehicle_id as key to ensure ordering/partition affinity for Flink.
        """
        try:
            # Keying by vehicle_id is CRITICAL for downstream stateful processing
            key = str(position.vehicle_id).encode("utf-8")
            value = position.model_dump_json().encode("utf-8")

            self._producer.produce(
                topic=self.config.topic_raw,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )

            # Serve delivery callbacks asynchronously
            self._producer.poll(0)

        except BufferError:
            logger.warning("Local Kafka producer buffer full. Retrying backoff...")
            # Backoff for 1 second to allow the background thread to clear the queue
            self._producer.poll(1.0)
            try:
                self._producer.produce(
                    topic=self.config.topic_raw,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )
            except Exception as e:
                self.metrics.record_kafka_error("buffer_overflow")
                logger.error("Dropped message after buffer retry: %s", e)

        except Exception as e:
            self.metrics.record_kafka_error("serialization_error")
            logger.exception("Produce operation failed: %s", e)

    def produce_dlq(self, invalid: InvalidEvent):
        """Produce invalid event to dead letter queue for observability."""
        self.metrics.record_dlq()
        try:
            value = invalid.model_dump_json().encode("utf-8")
            self._producer.produce(
                topic=self.config.topic_dlq,
                value=value,
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
        except Exception as e:
            logger.exception("Failed to write to DLQ: %s", e)

    def flush(self, timeout: float = 10.0) -> int:
        """Force flush of all pending messages in the producer queue."""
        logger.info("Flushing Kafka producer...")
        return self._producer.flush(timeout=timeout)

    def close(self):
        """
        Gracefully shutdown the producer.
        Ensures all in-flight messages reach the broker.
        """
        leftover = self.flush(timeout=10.0)
        if leftover > 0:
            logger.warning("Producer closed with %d unconfirmed messages", leftover)
        logger.info("Kafka producer closed.")
