"""
MQTT client for HSL vehicle position feed.
Pattern: Event-Driven Ingestion
Aligned: Uses UTC-aware latency tracking and string-based ID filtering.
"""

import json
import logging
import ssl
import time
from datetime import datetime, timezone
from typing import Callable, Optional

import paho.mqtt.client as mqtt
from pydantic import ValidationError

from .config import MQTTConfig
from .metrics import get_metrics
from .models import InvalidEvent, RawHSLPayload, VehiclePosition

logger = logging.getLogger(__name__)


class HSLMQTTClient:
    """
    MQTT client for HSL real-time vehicle positions.
    Handles high-throughput subscription to the /hfp/v2/ feed.
    """

    def __init__(
        self,
        config: MQTTConfig,
        on_message: Callable[[VehiclePosition], None],
        on_invalid: Callable[[InvalidEvent], None],
        filter_line: str = "",
    ):
        self.config = config
        self.on_message = on_message
        self.on_invalid = on_invalid
        self.filter_line = str(filter_line)  # Aligned: Treat as string for comparison
        self.metrics = get_metrics()

        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._reconnect_delay = config.reconnect_delay_min

    def _create_client(self) -> mqtt.Client:
        """Create and configure the MQTT client with V5 support intent via V2 callback API."""
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"transitflow-ingest-{int(time.time())}",
        )

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        if self.config.use_tls:
            # HSL uses TLS 1.2+ but is a public broker
            # ssl.CERT_NONE used because HSL doesn't require client certs
            client.tls_set(cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)

        return client

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Handle successful connection to HSL."""
        if rc == 0:
            logger.info("Connection established: %s:%d", self.config.host, self.config.port)
            self._connected = True
            self.metrics.set_mqtt_connected(True)
            self._reconnect_delay = self.config.reconnect_delay_min

            # Subscribe to the HSL HFP Topic
            client.subscribe(self.config.topic)
            logger.info("Subscription active: %s", self.config.topic)
        else:
            logger.error("MQTT connection failed with return code: %d", rc)
            self._connected = False
            self.metrics.set_mqtt_connected(False)

    def _on_disconnect(self, client, userdata, rc, properties=None, reason_code=None):
        """Handle disconnection with logging for circuit breaker visibility."""
        logger.warning("Disconnected from HSL broker. Return code: %s", rc)
        self._connected = False
        self.metrics.set_mqtt_connected(False)

    def _on_message(self, client, userdata, msg):
        """
        Ingest raw bytes, validate against contract, and calculate ingest-lag.
        Cleaned: Handles GPS dropouts without noisy stack traces.
        """
        receive_time = time.time()
        self.metrics.record_received()

        try:
            payload = json.loads(msg.payload.decode("utf-8"))

            # Extract the 'Vehicle Position' (VP) wrapper from HSL JSON
            vp_data = payload.get("VP")
            if not vp_data:
                self.metrics.record_invalid(reason="missing_vp_wrapper")
                return

            # Apply line filter before heavy validation to save CPU
            # desi (designation) is the user-facing line ID
            if self.filter_line and str(vp_data.get("desi")) != self.filter_line:
                return

            # Aligned: Parse raw payload and convert to UTC-forced Model
            raw = RawHSLPayload.model_validate(vp_data)
            position = raw.to_vehicle_position()

            self.metrics.record_validated()

            # Record Ingest Latency (How 'old' is the data when we touch it?)
            event_time = position.timestamp.timestamp()
            latency = receive_time - event_time
            if latency > 0:
                self.metrics.observe_mqtt_latency(latency)

            # Pass to Bridge Orchestrator
            self.on_message(position)

        except json.JSONDecodeError as e:
            self.metrics.record_invalid(reason="corrupt_json")
            self._send_to_dlq(msg.payload.decode("utf-8", errors="replace"), str(e), msg.topic)

        except ValidationError as e:
            self.metrics.record_invalid(reason="contract_violation")
            self._send_to_dlq(msg.payload.decode("utf-8", errors="replace"), str(e), msg.topic)

        except Exception as e:
            # Check if this is a known data quality issue (GPS Dropout)
            if "GPS dropout detected" in str(e):
                self.metrics.record_invalid(reason="gps_dropout")
                logger.warning("Data Quality Issue: %s", str(e))
            else:
                self.metrics.record_invalid(reason="runtime_error")
                logger.exception("Internal Bridge Error: %s", str(e))
            
            # Still route to DLQ so we don't lose the record of the failure
            self._send_to_dlq(msg.payload.decode("utf-8", errors="replace"), str(e), msg.topic)

    def _send_to_dlq(self, raw_payload: str, error: str, topic: str):
        """Helper to format and route failures to the DLQ handler."""
        invalid = InvalidEvent(
            raw_payload=raw_payload[:5000],  # Guard against massive payloads
            error_message=error,
            topic=topic,
            received_at=datetime.now(timezone.utc),  # Aligned: UTC audit time
        )
        self.on_invalid(invalid)

    def connect(self):
        """Initialize the client and attempt connection."""
        self._client = self._create_client()
        logger.info("Connecting to HSL MQTT at %s...", self.config.host)
        self._client.connect(
            self.config.host,
            self.config.port,
            keepalive=self.config.keepalive,
        )

    def start(self):
        """Blocking loop for foreground execution (Standard)."""
        if not self._client:
            self.connect()
        logger.info("MQTT Loop started (Blocking mode)")
        self._client.loop_forever(retry_first_connection=True)

    def start_background(self):
        """Non-blocking loop for multi-threaded testing."""
        if not self._client:
            self.connect()
        self._client.loop_start()

    def stop(self):
        """Graceful shutdown of the MQTT listener."""
        if self._client:
            logger.info("Disconnecting MQTT client...")
            self._client.loop_stop()
            self._client.disconnect()

    @property
    def connected(self) -> bool:
        return self._connected
