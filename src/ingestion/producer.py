"""
Kafka producer for vehicle telemetry.
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
    """

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.metrics = get_metrics()
        self._producer = self._init_producer()

    def _init_producer(self) -> Producer:
        """Initialize Kafka producer with ZSTD compression."""
        conf = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "acks": self.config.acks,
            "retries": self.config.retries,
            "linger.ms": self.config.linger_ms,
            "batch.size": self.config.batch_size,
            "compression.type": "zstd",
        }

        try:
            producer = Producer(conf)
            logger.info("Kafka producer initialized: %s", self.config.bootstrap_servers)
            return producer
        except Exception as e:
            logger.critical("Failed to connect to Kafka: %s", e)
            raise

    def _delivery_callback(self, err: Optional[KafkaError], msg: Message):
        """Callback for delivery confirmation."""
        if err:
            self.metrics.record_kafka_error(str(err))
            logger.error("Message delivery failed: %s", err)
        else:
            self.metrics.record_produced()

    def produce(self, position: VehiclePosition):
        """Produce vehicle position to Kafka."""
        try:
            key = str(position.vehicle_id).encode("utf-8")
            value = position.model_dump_json().encode("utf-8")

            self._producer.produce(
                topic=self.config.topic_raw,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )

            # Serve delivery callbacks
            self._producer.poll(0)

        except BufferError:
            logger.warning("Local Kafka buffer full. Retrying...")
            self._producer.poll(1.0)
            try:
                self._producer.produce(
                    topic=self.config.topic_raw,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )
            except Exception:
                self.metrics.record_kafka_error("buffer_overflow")
                logger.error("Dropped message due to buffer overflow")

        except Exception:
            self.metrics.record_kafka_error("serialization_error")
            logger.exception("Produce failed")

    def produce_dlq(self, invalid: InvalidEvent):
        """Produce invalid event to dead letter queue."""
        self.metrics.record_dlq()
        try:
            value = invalid.model_dump_json().encode("utf-8")
            self._producer.produce(
                topic=self.config.topic_dlq,
                value=value,
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
        except Exception:
            logger.exception("Failed to write to DLQ")

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages."""
        return self._producer.flush(timeout=timeout)

    def close(self):
        """Flush pending messages and close producer."""
        leftover = self.flush(timeout=10.0)
        if leftover > 0:
            logger.warning("Producer closed with %d unconfirmed messages", leftover)
