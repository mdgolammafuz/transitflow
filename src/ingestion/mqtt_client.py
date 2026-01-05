"""
MQTT client for HSL vehicle position feed.

Handles connection, subscription, reconnection, and message parsing.
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
    
    Connects to mqtt.hsl.fi, subscribes to vehicle position topics,
    and invokes callback for each validated message.
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
        self.filter_line = filter_line
        self.metrics = get_metrics()
        
        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._reconnect_delay = config.reconnect_delay_min

    def _create_client(self) -> mqtt.Client:
        """Create and configure MQTT client."""
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"transit-ingest-{int(time.time())}",
        )
        
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        
        if self.config.use_tls:
            # HSL uses TLS but doesn't require client certs
            client.tls_set(cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)
        
        return client

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Handle MQTT connection."""
        if rc == 0:
            logger.info("Connected to MQTT broker: %s:%d", self.config.host, self.config.port)
            self._connected = True
            self.metrics.set_mqtt_connected(True)
            self._reconnect_delay = self.config.reconnect_delay_min
            
            # Subscribe to topic
            client.subscribe(self.config.topic)
            logger.info("Subscribed to: %s", self.config.topic)
        else:
            logger.error("MQTT connection failed with code: %d", rc)
            self._connected = False
            self.metrics.set_mqtt_connected(False)

    def _on_disconnect(self, client, userdata, rc, properties=None, reason_code=None):
        """Handle MQTT disconnection."""
        logger.warning("Disconnected from MQTT broker (rc=%s)", rc)
        self._connected = False
        self.metrics.set_mqtt_connected(False)

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT message."""
        receive_time = time.time()
        self.metrics.record_received()
        
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            
            # HSL wraps data in "VP" object
            vp_data = payload.get("VP")
            if not vp_data:
                self.metrics.record_invalid(reason="missing_vp")
                return
            
            # Optional line filtering
            if self.filter_line and vp_data.get("desi") != self.filter_line:
                return
            
            # Parse raw payload
            raw = RawHSLPayload.model_validate(vp_data)
            
            # Convert to validated model
            position = raw.to_vehicle_position()
            
            self.metrics.record_validated()
            
            # Track latency from event time to now
            event_time = position.timestamp.timestamp()
            latency = receive_time - event_time
            if latency > 0:
                self.metrics.observe_mqtt_latency(latency)
            
            # Invoke callback
            self.on_message(position)
            
        except json.JSONDecodeError as e:
            self.metrics.record_invalid(reason="json_decode")
            self._send_to_dlq(msg.payload.decode("utf-8", errors="replace"), str(e), msg.topic)
            
        except ValidationError as e:
            self.metrics.record_invalid(reason="validation")
            self._send_to_dlq(msg.payload.decode("utf-8", errors="replace"), str(e), msg.topic)
            
        except Exception as e:
            self.metrics.record_invalid(reason="unknown")
            logger.exception("Unexpected error processing message")
            self._send_to_dlq(msg.payload.decode("utf-8", errors="replace"), str(e), msg.topic)

    def _send_to_dlq(self, raw_payload: str, error: str, topic: str):
        """Send invalid message to dead letter queue handler."""
        invalid = InvalidEvent(
            raw_payload=raw_payload[:10000],  # Truncate if huge
            error_message=error[:1000],
            topic=topic,
            received_at=datetime.now(timezone.utc),
        )
        self.on_invalid(invalid)

    def connect(self):
        """Connect to MQTT broker."""
        self._client = self._create_client()
        
        logger.info("Connecting to %s:%d...", self.config.host, self.config.port)
        self._client.connect(
            self.config.host,
            self.config.port,
            keepalive=self.config.keepalive,
        )

    def start(self):
        """Start the MQTT client loop (blocking)."""
        if not self._client:
            self.connect()
        
        logger.info("Starting MQTT loop...")
        self._client.loop_forever()

    def start_background(self):
        """Start MQTT client loop in background thread."""
        if not self._client:
            self.connect()
        
        self._client.loop_start()

    def stop(self):
        """Stop the MQTT client."""
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            logger.info("MQTT client stopped")

    @property
    def connected(self) -> bool:
        return self._connected
