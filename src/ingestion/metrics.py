"""
Prometheus metrics for the ingestion service.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server


class IngestMetrics:
    """
    Metrics collector for MQTT-to-Kafka bridge.
    
    Counters track totals (always increasing).
    Gauges track current values (can go up/down).
    Histograms track distributions (latency, sizes).
    """

    def __init__(self):
        # Connection status
        self.mqtt_connected = Gauge(
            "transit_mqtt_connected",
            "MQTT connection status (1=connected, 0=disconnected)",
        )
        
        # Message counters
        self.messages_received = Counter(
            "transit_messages_received_total",
            "Total messages received from MQTT",
        )
        self.messages_validated = Counter(
            "transit_messages_validated_total",
            "Messages that passed validation",
        )
        self.messages_invalid = Counter(
            "transit_messages_invalid_total",
            "Messages that failed validation",
            ["reason"],
        )
        self.messages_produced = Counter(
            "transit_messages_produced_total",
            "Messages produced to Kafka",
        )
        self.messages_dlq = Counter(
            "transit_messages_dlq_total",
            "Messages sent to dead letter queue",
        )
        self.kafka_errors = Counter(
            "transit_kafka_errors_total",
            "Kafka producer errors",
            ["error_type"],
        )
        
        # Gauges for current state
        self.active_vehicles = Gauge(
            "transit_active_vehicles",
            "Unique vehicles seen in last 60 seconds",
        )
        self.messages_per_second = Gauge(
            "transit_messages_per_second",
            "Current message rate",
        )
        
        # Latency histograms
        self.mqtt_latency = Histogram(
            "transit_mqtt_latency_seconds",
            "Time from event timestamp to receipt",
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
        )
        self.kafka_produce_latency = Histogram(
            "transit_kafka_produce_latency_seconds",
            "Kafka produce latency",
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
        )
        self.processing_latency = Histogram(
            "transit_processing_latency_seconds",
            "End-to-end processing latency (MQTT receive to Kafka produce)",
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
        )

    def start_server(self, port: int = 8000):
        """Start Prometheus HTTP server."""
        start_http_server(port)

    def record_received(self):
        self.messages_received.inc()

    def record_validated(self):
        self.messages_validated.inc()

    def record_invalid(self, reason: str):
        self.messages_invalid.labels(reason=reason).inc()

    def record_produced(self):
        self.messages_produced.inc()

    def record_dlq(self):
        self.messages_dlq.inc()

    def record_kafka_error(self, error_type: str):
        self.kafka_errors.labels(error_type=error_type).inc()

    def set_mqtt_connected(self, connected: bool):
        self.mqtt_connected.set(1 if connected else 0)

    def set_active_vehicles(self, count: int):
        self.active_vehicles.set(count)

    def set_message_rate(self, rate: float):
        self.messages_per_second.set(rate)

    def observe_mqtt_latency(self, seconds: float):
        self.mqtt_latency.observe(seconds)

    def observe_kafka_latency(self, seconds: float):
        self.kafka_produce_latency.observe(seconds)

    def observe_processing_latency(self, seconds: float):
        self.processing_latency.observe(seconds)


# Module-level singleton
_metrics: IngestMetrics | None = None


def get_metrics() -> IngestMetrics:
    """Get or create metrics singleton."""
    global _metrics
    if _metrics is None:
        _metrics = IngestMetrics()
    return _metrics
