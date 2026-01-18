"""
MQTT-to-Kafka bridge orchestrator.
Ties together MQTT client, Kafka producer, and metrics.
Aligned: Uses String-based IDs and ensures UTC-aware observability.
"""

import logging
import signal
import threading
import time

from .config import Settings, get_settings
from .metrics import get_metrics
from .models import InvalidEvent, VehiclePosition
from .mqtt_client import HSLMQTTClient
from .producer import TelemetryProducer

logger = logging.getLogger(__name__)


class Bridge:
    """
    Main orchestrator for MQTT-to-Kafka data flow.
    Coordinates message flow, tracks statistics, handles shutdown.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.metrics = get_metrics()

        # Statistics tracking
        self._message_count = 0
        self._last_count_time = time.time()
        # Aligned: vehicle_id is now str to match Lakehouse schema
        self._vehicles_seen: dict[str, float] = {}
        self._stats_lock = threading.Lock()

        # Components
        self._producer = TelemetryProducer(settings.kafka)
        self._mqtt_client = HSLMQTTClient(
            config=settings.mqtt,
            on_message=self._handle_message,
            on_invalid=self._handle_invalid,
            filter_line=settings.filter_line,
        )

        # Shutdown handling
        self._running = False

    def _handle_message(self, position: VehiclePosition):
        """Handle validated vehicle position and produce to Kafka."""
        process_start = time.time()

        # Produce to Kafka - This is our main I/O operation
        self._producer.produce(position)

        # Track statistics (Thread-safe)
        with self._stats_lock:
            self._message_count += 1
            self._vehicles_seen[position.vehicle_id] = time.time()

        # Record processing latency in Prometheus
        latency = time.time() - process_start
        self.metrics.observe_processing_latency(latency)

    def _handle_invalid(self, invalid: InvalidEvent):
        """Handle invalid message - send to Dead Letter Queue (DLQ)."""
        logger.warning("Invalid event detected: %s", invalid.error_message)
        self._producer.produce_dlq(invalid)

    def _update_stats(self):
        """Periodic stats update for gauges (Runs in daemon thread)."""
        while self._running:
            time.sleep(10)  # Update every 10 seconds

            now = time.time()
            with self._stats_lock:
                # Calculate throughput (messages per second)
                elapsed = now - self._last_count_time
                if elapsed > 0:
                    rate = self._message_count / elapsed
                    self.metrics.set_message_rate(rate)

                self._message_count = 0
                self._last_count_time = now

                # Count active vehicles seen in the last sliding window (60s)
                cutoff = now - 60
                active = sum(1 for t in self._vehicles_seen.values() if t > cutoff)
                self.metrics.set_active_vehicles(active)

                # Memory Management: Prune vehicles not seen in the window
                self._vehicles_seen = {
                    vid: t for vid, t in self._vehicles_seen.items() if t > cutoff
                }

    def _setup_signal_handlers(self):
        """Setup graceful shutdown on Unix signals."""

        def handler(signum, frame):
            logger.info("Received signal %d, initiating graceful shutdown...", signum)
            self.stop()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def start(self):
        """Start the bridge orchestrator."""
        self._running = True
        self._setup_signal_handlers()

        # Start Prometheus metrics server
        if self.settings.metrics.enabled:
            target_port = self.settings.metrics.port
            logger.info("Starting metrics server on port %d", target_port)
            self.metrics.start_server(target_port)

        # Start background stats thread
        stats_thread = threading.Thread(target=self._update_stats, daemon=True)
        stats_thread.start()

        # Log Architectural Context
        logger.info("Starting MQTT-to-Kafka bridge [UTC Alignment Enabled]")
        logger.info("MQTT Source: %s:%d", self.settings.mqtt.host, self.settings.mqtt.port)
        logger.info("Kafka Sink: %s", self.settings.kafka.bootstrap_servers)

        if self.settings.filter_line:
            logger.info("Line Filter: %s", self.settings.filter_line)

        # Start MQTT client (This blocks the main thread)
        try:
            self._mqtt_client.start()
        except KeyboardInterrupt:
            logger.info("Bridge execution interrupted by user")
        except Exception as e:
            logger.error("Bridge failed with unhandled exception: %s", str(e))
        finally:
            self.stop()

    def stop(self):
        """Perform graceful teardown of all components."""
        if not self._running:
            return

        logger.info("Shutting down Bridge components...")
        self._running = False

        # 1. Stop receiving new data
        self._mqtt_client.stop()

        # 2. Flush and close Kafka producer (ensures no data loss)
        self._producer.close()

        logger.info("Bridge stopped successfully")


def setup_logging(level: str = "INFO"):
    """Configure standardized logging for the project."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    """Main Entry Point."""
    try:
        settings = get_settings()
        setup_logging(settings.log_level)

        bridge = Bridge(settings)
        bridge.start()
    except Exception as e:
        print(f"Failed to initialize Bridge: {e}")
        exit(1)


if __name__ == "__main__":
    main()
