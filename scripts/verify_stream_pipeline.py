#!/usr/bin/env python3
"""
Final Consolidated Verification Script for TransitFlow.
Checks Flink health, Kafka throughput, and Redis Feature Store integrity.
"""
import json
import os
import sys
import time
from urllib.request import urlopen

# Environment Configuration
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def check_flink():
    print("\n=== Check 1 & 2: Flink Status ===")
    try:
        overview = json.loads(urlopen("http://localhost:8082/overview", timeout=5).read().decode())
        jobs = json.loads(urlopen("http://localhost:8082/jobs", timeout=5).read().decode()).get(
            "jobs", []
        )
        running = [j for j in jobs if j.get("status") == "RUNNING"]

        if overview.get("taskmanagers", 0) > 0 and running:
            print(f"  PASS: Flink OK. Job '{running[0].get('name')}' is RUNNING.")
            return True
        print("  FAIL: Flink cluster or job is not in the correct state.")
    except Exception as e:
        print(f"  FAIL: Flink API Error: {e}")
    return False


def check_kafka():
    print("\n=== Check 3: Kafka Throughput ===")
    try:
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer(
            {
                "bootstrap.servers": KAFKA_BROKER,
                "group.id": f"v-{int(time.time())}",
                "auto.offset.reset": "earliest",
            }
        )

        # We require enriched data; stop_events are optional for low-volume runs
        topic = "fleet.enriched"
        meta = c.list_topics(topic, timeout=5)
        if topic not in meta.topics:
            print(f"  FAIL: Topic {topic} missing.")
            return False

        partitions = meta.topics[topic].partitions
        total = 0
        for p in partitions:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p))
            total += high - low

        print(f"  PASS: {topic} contains {total} messages.")
        c.close()
        return total > 0
    except Exception as e:
        print(f"  FAIL: Kafka check error: {e}")
        return False


def check_redis():
    print("\n=== Check 4: Redis Feature Mapping ===")
    try:
        import redis

        r = redis.Redis(host="localhost", port=6379, password=REDIS_PASSWORD, decode_responses=True)
        keys = r.keys("features:vehicle:*")

        if not keys:
            print("  FAIL: No vehicle features found in Redis.")
            return False

        sample = r.hgetall(keys[0])
        print(f"  PASS: Found {len(keys)} keys. Sample (ID {sample.get('vehicle_id')}):")

        # Explicit validation of keys written by RedisSink.java
        speed_trend = sample.get("speed_trend")
        is_stopped = sample.get("is_stopped")

        print(f"    - speed_trend: {speed_trend}")
        print(f"    - is_stopped:  {is_stopped}")

        if speed_trend is not None and is_stopped is not None:
            return True
        print("  FAIL: One or more feature keys are missing (None).")
    except Exception as e:
        print(f"  FAIL: Redis check error: {e}")
    return False


def check_checkpoints():
    print("\n=== Check 5: Fault Tolerance ===")
    try:
        jobs = json.loads(urlopen("http://localhost:8082/jobs", timeout=5).read().decode()).get(
            "jobs", []
        )
        job_id = [j for j in jobs if j.get("status") == "RUNNING"][0]["id"]
        cp = json.loads(
            urlopen(f"http://localhost:8082/jobs/{job_id}/checkpoints", timeout=5).read().decode()
        )
        completed = cp.get("counts", {}).get("completed", 0)
        print(f"  PASS: {completed} checkpoints completed successfully.")
        return completed > 0
    except Exception:
        return False


if __name__ == "__main__":
    results = [check_flink(), check_kafka(), check_redis(), check_checkpoints()]
    print("\n" + "=" * 20 + "\nSUMMARY: " + ("PASS" if all(results) else "FAIL") + "\n" + "=" * 20)
    sys.exit(0 if all(results) else 1)
