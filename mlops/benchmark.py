"""
Inference Latency Benchmark.
Pattern: Performance Testing

Stresses the model with concurrent requests to verify SLA compliance.
Aligned with: ml_pipeline.config.py (Feature Schema)
"""

import argparse
import json
import logging
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import mlflow
from ml_pipeline.config import MLConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

config = MLConfig.from_env()

def generate_test_batch(n_samples: int):
    """Generate synthetic data matching the training schema."""
    # We use the length of feature_columns to determine input shape
    n_features = len(config.feature_columns)
    return np.random.rand(n_samples, n_features)

def benchmark(run_id: str, concurrency: int = 10, requests: int = 1000):
    model_uri = f"runs:/{run_id}/model"
    logger.info(f"Loading model from {model_uri}...")
    model = mlflow.sklearn.load_model(model_uri)
    
    # Warmup
    warmup_data = generate_test_batch(10)
    model.predict(warmup_data)

    logger.info(f"Starting benchmark: {requests} reqs, {concurrency} threads")
    
    latencies = []

    def _predict(_):
        # Generate single sample
        data = generate_test_batch(1)
        start = time.perf_counter()
        model.predict(data)
        return (time.perf_counter() - start) * 1000

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = executor.map(_predict, range(requests))
        latencies = list(results)

    stats = {
        "p50": round(statistics.median(latencies), 2),
        "p95": round(np.percentile(latencies, 95), 2),
        "p99": round(np.percentile(latencies, 99), 2),
        "max": round(max(latencies), 2),
        "throughput_rps": round(requests / (sum(latencies)/1000/concurrency), 2)
    }
    
    return stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--requests", type=int, default=500)
    args = parser.parse_args()

    stats = benchmark(args.run_id, args.concurrency, args.requests)
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    main()