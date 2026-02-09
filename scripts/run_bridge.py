#!/usr/bin/env python3
"""
Run the MQTT-to-Kafka bridge.
"""

import argparse
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.bridge import Bridge, setup_logging

# Import get_settings here to keep the top-level clean
from ingestion.config import get_settings


def main():
    parser = argparse.ArgumentParser(description="Transit MQTT-to-Kafka Bridge")
    parser.add_argument("--line", type=str, default="", help="Filter to specific line (e.g., 600)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level")
    parser.add_argument("--kafka", type=str, help="Kafka bootstrap servers")
    parser.add_argument("--metrics-port", type=int, help="Prometheus metrics port")
    args = parser.parse_args()

    # Priority 1: CLI Flags (Overwrites environment for this session)
    if args.line:
        os.environ["APP_FILTER_LINE"] = args.line
    if args.kafka:
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = args.kafka
    if args.metrics_port:
        # We set both to handle nested Pydantic prefixing variations
        os.environ["METRICS_PORT"] = str(args.metrics_port)
        os.environ["APP_METRICS_PORT"] = str(args.metrics_port)

    # Priority 2/3: Pydantic loads from Environment Variables or Config Defaults
    settings = get_settings()

    setup_logging(args.log_level)

    # Verification log to be absolutely sure before starting
    print(f"Operational Check: Metrics Port set to {settings.metrics.port}")

    bridge = Bridge(settings)
    bridge.start()


if __name__ == "__main__":
    main()
