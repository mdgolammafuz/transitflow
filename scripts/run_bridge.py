#!/usr/bin/env python3
"""
Run the MQTT-to-Kafka bridge.

Usage:
    python scripts/run_bridge.py                    # Default settings
    python scripts/run_bridge.py --line 600         # Filter to line 600
    python scripts/run_bridge.py --log-level DEBUG  # Verbose logging

Environment variables:
    MQTT_HOST           MQTT broker host (default: mqtt.hsl.fi)
    MQTT_PORT           MQTT broker port (default: 8883)
    KAFKA_BOOTSTRAP_SERVERS  Kafka brokers (default: localhost:9092)
    APP_FILTER_LINE     Line to filter (default: all)
    METRICS_PORT        Prometheus port (default: 8000)
"""

import argparse
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ingestion.bridge import Bridge, setup_logging
from src.ingestion.config import Settings


def main():
    parser = argparse.ArgumentParser(description="Transit MQTT-to-Kafka Bridge")
    parser.add_argument("--line", type=str, default="", help="Filter to specific line (e.g., 600)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level")
    parser.add_argument("--kafka", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--metrics-port", type=int, default=8000, help="Prometheus metrics port")
    args = parser.parse_args()
    
    # Override settings via environment
    if args.line:
        os.environ["APP_FILTER_LINE"] = args.line
    if args.kafka:
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = args.kafka
    if args.metrics_port:
        os.environ["METRICS_PORT"] = str(args.metrics_port)
    
    setup_logging(args.log_level)
    
    settings = Settings()
    bridge = Bridge(settings)
    bridge.start()


if __name__ == "__main__":
    main()
