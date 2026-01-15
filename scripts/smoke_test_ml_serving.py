#!/usr/bin/env python3
"""
Verification Script

Verifies:
1. ML Pipeline - training works
2. Model Registry - MLflow connection (optional)
3. Circuit Breaker - state transitions work
4. Tracing - spans are created
5. Serving API - endpoints respond
6. Prediction - can make predictions
"""

import argparse
import json
import os
import sys
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

import numpy as np


def check_ml_training():
    """Check 1: Verify ML training works."""
    print("\n=== Check 1: ML Training ===")

    try:
        from ml_pipeline.config import MLConfig
        from ml_pipeline.training import train_model

        config = MLConfig.default()

        X = np.random.randn(500, len(config.feature_columns))
        y = np.random.randn(500) * 60 + 30

        predictor, result = train_model(config, X, y, delta_version=1)

        print("  [OK] Model trained successfully")
        print(f"  [OK] Train MAE: {result.train_mae:.2f}s")
        print(f"  [OK] Test MAE: {result.test_mae:.2f}s")
        print(f"  [OK] R2 Score: {result.r2_score:.3f}")
        print(f"  [OK] Training samples: {result.training_samples}")

        features = {col: 0.0 for col in config.feature_columns}
        features["current_delay"] = 45.0
        prediction = predictor.predict_single(features)
        print(f"  [OK] Sample prediction: {prediction:.1f}s")

        print("PASS: ML training works")
        return True

    except Exception as e:
        print(f"  [FAIL] Training error: {e}")
        print("FAIL: ML training failed")
        return False


def check_model_save_load():
    """Check 2: Verify model save/load."""
    print("\n=== Check 2: Model Save/Load ===")

    try:
        import tempfile

        from ml_pipeline.config import MLConfig
        from ml_pipeline.training import DelayPredictor

        config = MLConfig.default()

        X = np.random.randn(100, len(config.feature_columns))
        y = np.random.randn(100) * 60

        predictor = DelayPredictor(config)
        predictor.train(X, y)

        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            model_path = f.name

        predictor.save(model_path)
        print(f"  [OK] Model saved to {model_path}")

        new_predictor = DelayPredictor(config)
        new_predictor.load(model_path)
        print(f"  [OK] Model loaded from {model_path}")

        assert new_predictor.is_trained
        print("  [OK] Loaded model is trained")

        os.unlink(model_path)

        print("PASS: Model save/load works")
        return True

    except Exception as e:
        print(f"  [FAIL] Save/load error: {e}")
        print("FAIL: Model save/load failed")
        return False


def check_circuit_breaker():
    """Check 3: Verify circuit breaker works."""
    print("\n=== Check 3: Circuit Breaker ===")

    try:
        from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState

        config = CircuitBreakerConfig(
            failure_threshold=3,
            timeout_seconds=0.1,
        )
        cb = CircuitBreaker("test", config)

        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.state == CircuitState.CLOSED
        print("  [OK] Successful call keeps circuit closed")

        def failing():
            raise Exception("test error")

        for i in range(3):
            try:
                cb.call(failing)
            except Exception:
                pass

        assert cb.state == CircuitState.OPEN
        print(f"  [OK] Circuit opens after {config.failure_threshold} failures")

        fallback_result = cb.call(failing, fallback=lambda: "fallback")
        assert fallback_result == "fallback"
        print("  [OK] Open circuit uses fallback")

        time.sleep(0.15)
        cb.call(lambda: "success")
        print("  [OK] Circuit recovers after timeout")

        print("PASS: Circuit breaker works")
        return True

    except Exception as e:
        print(f"  [FAIL] Circuit breaker error: {e}")
        print("FAIL: Circuit breaker failed")
        return False


def check_tracing():
    """Check 4: Verify tracing works."""
    print("\n=== Check 4: Tracing ===")

    try:
        from serving.tracing import Tracer

        tracer = Tracer()
        tracer.clear_local_spans()

        with tracer.span("test_operation", {"key": "value"}) as ctx:
            time.sleep(0.01)
            assert ctx.trace_id is not None
            assert ctx.span_id is not None

        assert ctx.end_time is not None
        duration_ms = (ctx.end_time - ctx.start_time) * 1000
        print(f"  [OK] Span created with trace_id={ctx.trace_id[:8]}...")
        print(f"  [OK] Span duration: {duration_ms:.2f}ms")
        print(f"  [OK] Span attributes: {ctx.attributes}")

        spans = tracer.get_local_spans()
        assert len(spans) >= 1
        print(f"  [OK] {len(spans)} span(s) recorded")

        print("PASS: Tracing works")
        return True

    except Exception as e:
        print(f"  [FAIL] Tracing error: {e}")
        print("FAIL: Tracing failed")
        return False


def check_serving_api_health():
    """Check 5: Verify Serving API health."""
    print("\n=== Check 5: Serving API Health ===")

    api_url = os.environ.get("SERVING_API_URL", "http://localhost:8001")

    try:
        response = urlopen(f"{api_url}/health", timeout=5)
        data = json.loads(response.read().decode())

        status = data.get("status", "unknown")
        model_loaded = data.get("model_loaded", False)
        model_version = data.get("model_version")
        circuits = data.get("circuit_breakers", {})

        print("  [OK] API responding")
        print(f"  [OK] Status: {status}")
        print(f"  [OK] Model loaded: {model_loaded}")
        print(f"  [OK] Model version: {model_version}")
        print(f"  [OK] Circuit breakers: {circuits}")

        print("PASS: Serving API healthy")
        return True

    except URLError as e:
        print(f"  [FAIL] API error: {e}")
        print("FAIL: Serving API not responding")
        print("  Hint: Start API with 'make serving-api'")
        return False


def check_prediction():
    """Check 6: Verify prediction works."""
    print("\n=== Check 6: Prediction ===")

    api_url = os.environ.get("SERVING_API_URL", "http://localhost:8001")

    try:
        request_data = json.dumps(
            {
                "vehicle_id": 1362,
                "stop_id": 12345,
                "line_id": "600",
            }
        ).encode()

        req = Request(
            f"{api_url}/predict",
            data=request_data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        response = urlopen(req, timeout=5)
        data = json.loads(response.read().decode())

        predicted = data.get("predicted_delay_seconds")
        latency = data.get("latency_ms")
        model_version = data.get("model_version")
        trace_id = data.get("trace_id")

        print("  [OK] Prediction successful")
        print(f"  [OK] Predicted delay: {predicted}s")
        print(f"  [OK] Latency: {latency}ms")
        print(f"  [OK] Model version: {model_version}")
        if trace_id:
            print(f"  [OK] Trace ID: {trace_id[:16]}...")

        print("PASS: Prediction works")
        return True

    except URLError as e:
        print(f"  [FAIL] Prediction error: {e}")
        print("FAIL: Prediction failed")
        return False


def check_circuits_endpoint():
    """Check 7: Verify circuits endpoint."""
    print("\n=== Check 7: Circuits Endpoint ===")

    api_url = os.environ.get("SERVING_API_URL", "http://localhost:8001")

    try:
        response = urlopen(f"{api_url}/circuits", timeout=5)
        data = json.loads(response.read().decode())

        print("  [OK] Circuits endpoint responding")
        for name, stats in data.items():
            state = stats.get("state", "unknown")
            calls = stats.get("total_calls", 0)
            print(f"  [OK] {name}: state={state}, calls={calls}")

        print("PASS: Circuits endpoint works")
        return True

    except URLError as e:
        print(f"  [FAIL] Circuits error: {e}")
        print("FAIL: Circuits endpoint failed")
        return False


def main():
    parser = argparse.ArgumentParser(description="Phase 6 Verification")
    parser.add_argument("--check-all", action="store_true", help="Run all checks")
    parser.add_argument("--check-ml", action="store_true", help="Check ML only")
    parser.add_argument("--check-serving", action="store_true", help="Check serving only")
    args = parser.parse_args()

    run_all = args.check_all or not any([args.check_ml, args.check_serving])

    results = {}

    print("=" * 50)
    print("PHASE 6 VERIFICATION: ML Pipeline + Serving")
    print("=" * 50)

    if args.check_ml or run_all:
        results["ml_training"] = check_ml_training()
        results["model_save_load"] = check_model_save_load()
        results["circuit_breaker"] = check_circuit_breaker()
        results["tracing"] = check_tracing()

    if args.check_serving or run_all:
        results["serving_health"] = check_serving_api_health()

        if results.get("serving_health"):
            results["prediction"] = check_prediction()
            results["circuits"] = check_circuits_endpoint()

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    passed = failed = skipped = 0

    for check, result in results.items():
        if result is True:
            status = "[PASS]"
            passed += 1
        elif result is False:
            status = "[FAIL]"
            failed += 1
        else:
            status = "[SKIP]"
            skipped += 1
        print(f"  {check}: {status}")

    print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")

    if failed == 0 and passed > 0:
        print("\n" + "=" * 50)
        print("PHASE 6 VERIFICATION: ALL CHECKS PASSED")
        print("=" * 50)
    else:
        print("\n" + "=" * 50)
        print("PHASE 6 VERIFICATION: INCOMPLETE")
        print("=" * 50)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
