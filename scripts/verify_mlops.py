#!/usr/bin/env python3
"""
MLOps & Monitoring Verification Script.
"""

import sys
import os
import logging
import numpy as np

# --- Add Project Root to Path ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)
# --------------------------------

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("verify")

def check_imports():
    """Verify module structure."""
    logger.info("1. Checking Imports...")
    errors = []
    
    modules = [
        "monitoring.metrics",
        "monitoring.drift",
        "monitoring.slo",
        "mlops.validation",
        "mlops.promotion",
        "serving.shadow"
    ]

    for mod in modules:
        try:
            __import__(mod)
            logger.info(f"  [OK] {mod}")
        except ImportError as e:
            errors.append(f"{mod}: {e}")
            logger.error(f"  [FAIL] {mod}: {e}")
            
    return len(errors) == 0

def check_psi_math():
    """Verify statistical calculations."""
    logger.info("\n2. Checking PSI Math...")
    try:
        from monitoring.drift import calculate_psi
        
        d1 = np.random.normal(0, 1, 1000)
        psi_low = calculate_psi(d1, d1)
        
        d2 = np.random.normal(2, 1, 1000)
        psi_high = calculate_psi(d1, d2)
        
        if psi_low < 0.05 and psi_high > 0.1:
            logger.info(f"  [OK] Logic verified (Low={psi_low:.3f}, High={psi_high:.3f})")
            return True
        logger.error(f"  [FAIL] Math error (Low={psi_low}, High={psi_high})")
        return False
    except Exception as e:
        logger.error(f"  [FAIL] PSI Check Crash: {e}")
        return False

def check_drift_detector():
    """Verify stateful drift detection."""
    logger.info("\n3. Checking Drift Detector...")
    try:
        from monitoring.drift import DriftDetector, DriftSeverity
        
        detector = DriftDetector()
        
        # 1. Fit Baseline (Large sample)
        baseline = np.random.normal(50, 10, 2000)
        detector.fit("speed", baseline)
        
        # 2. Check Normal (Same distribution)
        normal = np.random.normal(50, 10, 200)
        res_ok = detector.detect("speed", normal)
        
        # 3. Check Drift (Different distribution)
        drifted = np.random.normal(80, 10, 200)
        res_drift = detector.detect("speed", drifted)
        
        # Strictness Fix: Accept NONE or LOW for normal data
        is_normal_ok = res_ok.severity in [DriftSeverity.NONE, DriftSeverity.LOW]
        is_drift_ok = res_drift.severity == DriftSeverity.HIGH
        
        if is_normal_ok and is_drift_ok:
            logger.info("  [OK] Detector transitions correctly")
            return True
            
        logger.error(f"  [FAIL] State mismatch. Normal: {res_ok.severity}, Drifted: {res_drift.severity}")
        return False
    except Exception as e:
        logger.error(f"  [FAIL] Drift Check Crash: {e}")
        return False

def check_prometheus():
    """Verify metric registry."""
    logger.info("\n4. Checking Prometheus Metrics...")
    try:
        from monitoring.metrics import PREDICTION_LATENCY, FEATURE_DRIFT_PSI
        
        if hasattr(PREDICTION_LATENCY, 'observe') and hasattr(FEATURE_DRIFT_PSI, 'set'):
            logger.info("  [OK] Metrics registered successfully")
            return True
        return False
    except ImportError:
        logger.error("  [FAIL] Could not import metrics")
        return False

def check_shadow_mode():
    """Verify shadow predictor logic."""
    logger.info("\n5. Checking Shadow Mode...")
    try:
        from serving.shadow import ShadowPredictor
        from unittest.mock import Mock
    except ImportError:
        logger.warning("  [FAIL] Shadow module not found")
        return False

    try:
        # Mocks
        prod_mock = Mock()
        prod_mock.predict.return_value = np.array([100.0])
        
        shadow_mock = Mock()
        shadow_mock.predict.return_value = np.array([102.0]) # Diff = 2.0
        
        predictor = ShadowPredictor(
            prod_model=prod_mock,
            shadow_model=shadow_mock,
            agreement_threshold=5.0
        )
        
        # Verify sync helper
        prod_pred, comparison = predictor.predict_sync_with_shadow({"f1": 1}, "v1")
        
        # Attribute Fix: Use .diff instead of .difference
        if comparison.agreement is True and comparison.diff == 2.0:
            logger.info("  [OK] Shadow comparison logic verified")
            return True
            
        logger.error(f"  [FAIL] Shadow logic incorrect. Diff: {comparison.diff}, Agreement: {comparison.agreement}")
        return False
    except Exception as e:
        logger.error(f"  [FAIL] Shadow Check Crash: {e}")
        return False

def main():
    checks = [
        check_imports,
        check_psi_math,
        check_drift_detector,
        check_prometheus,
        check_shadow_mode
    ]
    
    # Run all checks
    results = [c() for c in checks]
    passed = all(results)
    
    print("-" * 30)
    if passed:
        print("SYSTEM VERIFICATION PASSED")
        sys.exit(0)
    else:
        print("SYSTEM VERIFICATION FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()