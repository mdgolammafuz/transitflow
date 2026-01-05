"""
MQTT-to-Kafka bridge orchestrator.

Ties together MQTT client, Kafka producer, and metrics.
Handles graceful shutdown and statistics tracking.
"""

import logging
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

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
        self._vehicles_seen: dict[int, float] = {}  # vehicle_id -> last_seen_time
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
        """Handle validated vehicle position."""
        process_start = time.time()
        
        # Produce to Kafka
        self._producer.produce(position)
        
        # Track statistics
        with self._stats_lock:
            self._message_count += 1
            self._vehicles_seen[position.vehicle_id] = time.time()
        
        # Record processing latency
        latency = time.time() - process_start
        self.metrics.observe_processing_latency(latency)

    def _handle_invalid(self, invalid: InvalidEvent):
        """Handle invalid message - send to DLQ."""
        self._producer.produce_dlq(invalid)

    def _update_stats(self):
        """Periodic stats update for gauges."""
        while self._running:
            time.sleep(10)  # Update every 10 seconds
            
            now = time.time()
            with self._stats_lock:
                # Calculate message rate
                elapsed = now - self._last_count_time
                if elapsed > 0:
                    rate = self._message_count / elapsed
                    self.metrics.set_message_rate(rate)
                
                self._message_count = 0
                self._last_count_time = now
                
                # Count active vehicles (seen in last 60 seconds)
                cutoff = now - 60
                active = sum(1 for t in self._vehicles_seen.values() if t > cutoff)
                self.metrics.set_active_vehicles(active)
                
                # Prune old entries
                self._vehicles_seen = {
                    vid: t for vid, t in self._vehicles_seen.items() if t > cutoff
                }

    def _setup_signal_handlers(self):
        """Setup graceful shutdown on SIGINT/SIGTERM."""
        def handler(signum, frame):
            logger.info("Received signal %d, shutting down...", signum)
            self.stop()
        
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def start(self):
        """Start the bridge."""
        self._running = True
        self._setup_signal_handlers()
        
        # Start metrics server
        if self.settings.metrics.enabled:
            self.metrics.start_server(self.settings.metrics.port)
            logger.info("Metrics server started on port %d", self.settings.metrics.port)
        
        # Start stats updater thread
        stats_thread = threading.Thread(target=self._update_stats, daemon=True)
        stats_thread.start()
        
        # Log configuration
        logger.info("Starting MQTT-to-Kafka bridge")
        logger.info("MQTT broker: %s:%d", self.settings.mqtt.host, self.settings.mqtt.port)
        logger.info("Kafka bootstrap: %s", self.settings.kafka.bootstrap_servers)
        if self.settings.filter_line:
            logger.info("Filtering to line: %s", self.settings.filter_line)
        
        # Start MQTT client (blocking)
        try:
            self._mqtt_client.start()
        except KeyboardInterrupt:
            logger.info("Interrupted")
        finally:
            self.stop()

    def stop(self):
        """Stop the bridge gracefully."""
        logger.info("Stopping bridge...")
        self._running = False
        self._mqtt_client.stop()
        self._producer.close()
        logger.info("Bridge stopped")


def setup_logging(level: str = "INFO"):
    """Configure structured logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    """Entry point."""
    settings = get_settings()
    setup_logging(settings.log_level)
    
    bridge = Bridge(settings)
    bridge.start()


if __name__ == "__main__":
    main()
