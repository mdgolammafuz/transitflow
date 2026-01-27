"""
Unit Tests for Monitoring Module.
Aligned with: monitoring/drift.py, monitoring/slo.py
"""

import pytest
import numpy as np
from monitoring.drift import DriftDetector, calculate_psi, DriftSeverity
from monitoring.slo import SLODefinition, SLOStatus

class TestPSICalculation:
    """Tests for PSI math logic."""

    def test_psi_identical(self):
        """PSI should be ~0 for identical distributions."""
        np.random.seed(42)
        data = np.random.normal(100, 15, 1000)
        psi = calculate_psi(data, data)
        assert psi < 0.01

    def test_psi_drifted(self):
        """PSI should detect shift."""
        np.random.seed(42)
        baseline = np.random.normal(100, 15, 1000)
        current = np.random.normal(150, 20, 1000) # Big shift
        psi = calculate_psi(baseline, current)
        assert psi > 0.2

class TestDriftDetector:
    """Tests for DriftDetector state management."""

    def test_workflow(self):
        """Test the Fit -> Detect workflow."""
        np.random.seed(42)
        detector = DriftDetector()
        
        # 1. Fit Baseline
        baseline = np.random.normal(50, 10, 2000)
        detector.fit("speed", baseline)
        
        # 2. Detect Normal Data (No Drift)
        # Increased sample size to 1000 to reduce statistical noise
        current_ok = np.random.normal(50, 10, 1000)
        result_ok = detector.detect("speed", current_ok)
        
        assert result_ok.feature == "speed"
        # Now it will reliably be NONE or LOW
        assert result_ok.severity in [DriftSeverity.NONE, DriftSeverity.LOW]

        # 3. Detect Drifted Data
        current_drift = np.random.normal(80, 10, 1000)
        result_drift = detector.detect("speed", current_drift)
        assert result_drift.severity == DriftSeverity.HIGH

    def test_missing_feature(self):
        """Should raise error if detecting on unknown feature."""
        detector = DriftDetector()
        with pytest.raises(ValueError):
            detector.detect("unknown_feature", np.array([1, 2]))

class TestSLO:
    """Tests for Service Level Objectives."""

    def test_check_less_than(self):
        """Test 'lt' operator."""
        slo = SLODefinition(name="lag", metric="m", target=100, op="lt", description="...")
        
        assert slo.check(50) == SLOStatus.OK       # 50 < 100
        assert slo.check(90) == SLOStatus.WARNING  # 90 > 80% of 100
        assert slo.check(110) == SLOStatus.BREACHED # 110 > 100

    def test_check_greater_than(self):
        """Test 'gt' operator."""
        slo = SLODefinition(name="uptime", metric="m", target=99.0, op="gt", description="...")
        
        assert slo.check(99.9) == SLOStatus.OK
        assert slo.check(98.0) == SLOStatus.BREACHED

class TestMetricsRegistry:
    """Smoke test for Prometheus definitions."""
    
    def test_metrics_export(self):
        """Ensure global metrics are defined correctly."""
        from monitoring.metrics import PREDICTION_LATENCY, FEATURE_DRIFT_PSI
        
        # Verify types
        assert hasattr(PREDICTION_LATENCY, 'observe')
        assert hasattr(FEATURE_DRIFT_PSI, 'set')
        
        # Verify names
        assert "transitflow_prediction_latency_seconds" in PREDICTION_LATENCY._name