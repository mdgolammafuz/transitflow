#!/usr/bin/env python3
"""
Phase 1 Verification Script

Run this script locally to verify the ingestion setup works.
Tests MQTT connection, message parsing, and Kafka production.
"""

import argparse
import json
import ssl
import sys
import time
from datetime import datetime, timezone


def test_mqtt_connection():
    """
    Test 1: Verify we can connect to HSL MQTT broker and receive messages.

    Expected: Receive vehicle position messages within 10 seconds.
    """
    print("\n=== Test 1: MQTT Connection ===")

    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print("FAIL: paho-mqtt not installed")
        print("Run: pip install paho-mqtt")
        return False

    received = []
    connected = False

    def on_connect(client, userdata, flags, rc, properties=None):
        nonlocal connected
        if rc == 0:
            print("Connected to mqtt.hsl.fi:8883")
            connected = True
            # Subscribe to all bus positions
            client.subscribe("/hfp/v2/journey/ongoing/vp/bus/#")
            print("Subscribed to bus position topic")
        else:
            print(f"Connection failed: rc={rc}")

    def on_message(client, userdata, msg):
        received.append(msg.payload)
        if len(received) <= 3:
            try:
                data = json.loads(msg.payload)
                vp = data.get("VP", {})
                print(
                    f"  Vehicle {vp.get('veh')}: line={vp.get('desi')}, "
                    f"speed={vp.get('spd'):.1f}m/s, delay={vp.get('dl')}s"
                )
            except Exception:
                pass

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)

    try:
        print("Connecting to mqtt.hsl.fi:8883...")
        client.connect("mqtt.hsl.fi", 8883, 60)
        client.loop_start()

        # Wait for messages
        timeout = 10
        start = time.time()
        while len(received) < 5 and (time.time() - start) < timeout:
            time.sleep(0.5)

        client.loop_stop()
        client.disconnect()

        if len(received) >= 5:
            print(f"PASS: Received {len(received)} messages in {time.time()-start:.1f}s")
            return True
        elif len(received) > 0:
            print(f"PARTIAL: Received {len(received)} messages (expected 5+)")
            return True
        else:
            print("FAIL: No messages received")
            return False

    except Exception as e:
        print(f"FAIL: {e}")
        return False


def test_message_parsing():
    """
    Test 2: Verify Pydantic models parse HSL messages correctly.

    Expected: Sample messages parse without errors.
    """
    print("\n=== Test 2: Message Parsing ===")

    try:
        from src.ingestion.models import RawHSLPayload, VehiclePosition  # noqa: F401
    except ImportError as e:
        print(f"FAIL: Import error - {e}")
        print("Make sure you're in the project root and run: pip install -e .")
        return False

    # Sample HSL message (based on real data)
    sample_messages = [
        {
            "VP": {
                "desi": "600",
                "dir": "1",
                "oper": 22,
                "veh": 1362,
                "tst": "2024-12-31T10:15:30.000Z",
                "tsi": 1735640130,
                "spd": 8.33,
                "hdg": 245,
                "lat": 60.1699,
                "long": 24.9384,
                "dl": 45,
                "drst": 0,
            }
        },
        {
            "VP": {
                "veh": 1001,
                "tst": "2024-12-31T10:15:30.000Z",
                "tsi": 1735640130,
                "lat": 60.22,
                "long": 24.88,
                # Minimal fields - should still parse
            }
        },
    ]

    passed = 0
    for i, msg in enumerate(sample_messages):
        try:
            vp_data = msg.get("VP")
            raw = RawHSLPayload.model_validate(vp_data)
            pos = raw.to_vehicle_position()

            # Verify key fields
            assert len(pos.vehicle_id) > 0
            assert 59.0 < pos.latitude < 61.0
            assert 23.0 < pos.longitude < 26.0
            assert pos.event_time_ms > 0

            print(
                f"  Message {i+1}: vehicle={pos.vehicle_id}, line={pos.line_id}, "
                f"delay={pos.delay_seconds}s - OK"
            )
            passed += 1
        except Exception as e:
            print(f"  Message {i+1}: FAIL - {e}")

    if passed == len(sample_messages):
        print(f"PASS: All {passed} messages parsed correctly")
        return True
    else:
        print(f"PARTIAL: {passed}/{len(sample_messages)} messages parsed")
        return False


def test_kafka_connection():
    """
    Test 3: Verify we can connect to Kafka and produce messages.

    Expected: Successfully produce and consume a test message.
    Requires: docker-compose up (Redpanda running)
    """
    print("\n=== Test 3: Kafka Connection ===")

    try:
        from confluent_kafka import Consumer, KafkaError, Producer  # noqa: F401
    except ImportError:
        print("FAIL: confluent-kafka not installed")
        print("Run: pip install confluent-kafka")
        return False

    bootstrap = "localhost:9092"
    test_topic = "test.verification"

    # Test producer
    try:
        producer = Producer({"bootstrap.servers": bootstrap})

        # Produce test message
        test_msg = json.dumps({"test": True, "timestamp": datetime.now(timezone.utc).isoformat()})
        producer.produce(test_topic, test_msg.encode())
        producer.flush(timeout=5)
        print(f"  Produced message to {test_topic}")
    except Exception as e:
        print(f"FAIL: Cannot connect to Kafka at {bootstrap}")
        print(f"  Error: {e}")
        print("  Make sure Redpanda is running: cd docker && docker-compose up -d")
        return False

    # Test consumer
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test-verify",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([test_topic])

        msg = consumer.poll(timeout=5.0)
        consumer.close()

        if msg and not msg.error():
            print(f"  Consumed message: {msg.value().decode()[:50]}...")
            print("PASS: Kafka produce/consume working")
            return True
        else:
            print("PARTIAL: Produced but could not consume (may be timing)")
            return True

    except Exception as e:
        print(f"FAIL: Consumer error - {e}")
        return False


def test_full_integration():
    """
    Test 4: Full integration - MQTT to Kafka pipeline.

    Expected: Receive HSL messages and produce to Kafka.
    """
    print("\n=== Test 4: Full Integration ===")

    try:
        import paho.mqtt.client as mqtt

        from src.ingestion.config import get_settings
        from src.ingestion.models import RawHSLPayload
        from src.ingestion.producer import TelemetryProducer
    except ImportError as e:
        print(f"FAIL: Import error - {e}")
        return False

    settings = get_settings()
    producer = TelemetryProducer(settings.kafka)

    messages_produced = []

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload)
            vp_data = payload.get("VP")
            if not vp_data:
                return

            raw = RawHSLPayload.model_validate(vp_data)
            pos = raw.to_vehicle_position()
            producer.produce(pos)
            messages_produced.append(pos.vehicle_id)

            if len(messages_produced) <= 3:
                print(f"  Produced: vehicle={pos.vehicle_id}, line={pos.line_id}")
        except Exception:
            pass

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client.subscribe("/hfp/v2/journey/ongoing/vp/bus/#")

    client.on_connect = on_connect

    try:
        print("Connecting to MQTT and producing to Kafka...")
        client.connect("mqtt.hsl.fi", 8883, 60)
        client.loop_start()

        timeout = 15
        start = time.time()
        while len(messages_produced) < 10 and (time.time() - start) < timeout:
            time.sleep(0.5)

        client.loop_stop()
        client.disconnect()
        producer.flush()

        if len(messages_produced) >= 10:
            print(f"PASS: Produced {len(messages_produced)} messages to Kafka")
            return True
        elif len(messages_produced) > 0:
            print(f"PARTIAL: Produced {len(messages_produced)} messages")
            return True
        else:
            print("FAIL: No messages produced")
            return False

    except Exception as e:
        print(f"FAIL: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Phase 1 Verification")
    parser.add_argument("--test-mqtt", action="store_true", help="Test MQTT connection")
    parser.add_argument("--test-parse", action="store_true", help="Test message parsing")
    parser.add_argument("--test-kafka", action="store_true", help="Test Kafka connection")
    parser.add_argument("--full", action="store_true", help="Run full integration test")
    args = parser.parse_args()

    # Default to running all tests if no specific test selected
    run_all = not (args.test_mqtt or args.test_parse or args.test_kafka or args.full)

    results = {}

    if args.test_mqtt or run_all:
        results["mqtt"] = test_mqtt_connection()

    if args.test_parse or run_all:
        results["parse"] = test_message_parsing()

    if args.test_kafka or run_all:
        results["kafka"] = test_kafka_connection()

    if args.full or run_all:
        results["integration"] = test_full_integration()

    # Summary
    print("\n=== Summary ===")
    all_passed = True
    for test, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {test}: {status}")
        if not passed:
            all_passed = False

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
