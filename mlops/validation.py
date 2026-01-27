"""
Model Validation Gate.
Pattern: Quality Assurance

Validates trained models against business thresholds before they reach Staging.
Aligned with: ml_pipeline.training.py (Metrics)
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

import numpy as np
import mlflow
from mlflow.tracking import MlflowClient

from ml_pipeline.config import MLConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load project config for thresholds and feature definitions
config = MLConfig.from_env()

def validate_metrics(run_id: str, min_r2: float, max_mae: float) -> Dict[str, Any]:
    """Validate metrics logged during training."""
    client = MlflowClient()
    run = client.get_run(run_id)
    metrics = run.data.metrics

    # Mapped to ml_pipeline/training.py logging keys
    actual_r2 = metrics.get("r2_score", 0.0)
    actual_mae = metrics.get("test_mae", float("inf"))

    r2_pass = actual_r2 >= min_r2
    mae_pass = actual_mae <= max_mae

    if not r2_pass:
        logger.warning(f"R2 Failure: {actual_r2:.4f} < {min_r2}")
    if not mae_pass:
        logger.warning(f"MAE Failure: {actual_mae:.2f} > {max_mae}")

    return {
        "passed": r2_pass and mae_pass,
        "details": {
            "r2_score": {"actual": actual_r2, "threshold": min_r2, "passed": r2_pass},
            "test_mae": {"actual": actual_mae, "threshold": max_mae, "passed": mae_pass},
        }
    }

def validate_latency(run_id: str, max_latency_ms: float) -> Dict[str, Any]:
    """Validate inference speed using synthetic data matching config schema."""
    try:
        model_uri = f"runs:/{run_id}/model"
        model = mlflow.sklearn.load_model(model_uri)
        
        # Generate synthetic data based on actual feature config
        # This prevents shape mismatch errors during validation
        n_features = len(config.feature_columns)
        n_samples = 100
        X_test = np.random.rand(n_samples, n_features)

        # Warmup
        model.predict(X_test[:5])

        latencies = []
        for i in range(n_samples):
            start = time.perf_counter()
            model.predict(X_test[i:i+1])
            latencies.append((time.perf_counter() - start) * 1000)

        p99 = np.percentile(latencies, 99)
        passed = p99 <= max_latency_ms

        if not passed:
            logger.warning(f"Latency Failure: P99 {p99:.2f}ms > {max_latency_ms}ms")

        return {
            "passed": bool(passed),
            "details": {
                "p99_ms": round(p99, 2),
                "threshold_ms": max_latency_ms
            }
        }
    except Exception as e:
        logger.error(f"Latency check crashed: {e}")
        return {"passed": False, "error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="TransitFlow Model Validator")
    parser.add_argument("--run-id", required=True, help="MLflow Run ID")
    parser.add_argument("--min-r2", type=float, default=0.75)
    parser.add_argument("--max-mae", type=float, default=120.0)
    parser.add_argument("--max-latency", type=float, default=50.0)
    parser.add_argument("--output", help="JSON output path")
    
    args = parser.parse_args()

    results = {
        "run_id": args.run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": {}
    }

    # 1. Metric Validation
    metric_res = validate_metrics(args.run_id, args.min_r2, args.max_mae)
    results["checks"]["metrics"] = metric_res

    # 2. Latency Validation
    latency_res = validate_latency(args.run_id, args.max_latency)
    results["checks"]["latency"] = latency_res

    # 3. Aggregation
    overall_pass = metric_res["passed"] and latency_res["passed"]
    results["overall_passed"] = overall_pass

    # Output
    print(json.dumps(results, indent=2))
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)

    sys.exit(0 if overall_pass else 1)

if __name__ == "__main__":
    main()