"""
Feature Drift Detection.
Pattern: Observability (PSI)

Calculates Population Stability Index (PSI) to detect distribution shifts.
Aligned with: mlops/retraining.py
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any

import numpy as np

logger = logging.getLogger(__name__)

class DriftSeverity(Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate" # PSI 0.1 - 0.2
    HIGH = "high"         # PSI > 0.2

@dataclass
class DriftResult:
    feature: str
    psi: float
    severity: DriftSeverity
    timestamp: str

def calculate_psi(
    expected: np.ndarray,
    actual: np.ndarray,
    n_buckets: int = 10,
    eps: float = 1e-4
) -> float:
    """
    Calculate PSI between two distributions.
    Stateless function useful for ad-hoc checks.
    """
    # Defensive casting
    expected = np.asarray(expected).flatten()
    actual = np.asarray(actual).flatten()
    
    # Remove NaNs
    expected = expected[~np.isnan(expected)]
    actual = actual[~np.isnan(actual)]

    if len(expected) == 0 or len(actual) == 0:
        return 0.0

    # Define buckets based on the baseline (expected) distribution
    try:
        percentiles = np.linspace(0, 100, n_buckets + 1)
        bucket_edges = np.percentile(expected, percentiles)
        
        # Handle edge case where data is constant (all percentiles same)
        if bucket_edges[0] == bucket_edges[-1]:
            # If distributions are identical constants, PSI is 0
            if np.array_equal(np.unique(expected), np.unique(actual)):
                return 0.0
            return float('inf') # Distinct constants = infinite drift

        bucket_edges[0] = -np.inf
        bucket_edges[-1] = np.inf

        # Calculate frequencies
        expected_counts, _ = np.histogram(expected, bins=bucket_edges)
        actual_counts, _ = np.histogram(actual, bins=bucket_edges)

        # Normalize to probabilities
        expected_pct = expected_counts / len(expected)
        actual_pct = actual_counts / len(actual)

        # Add epsilon to avoid divide-by-zero or log(0)
        expected_pct = np.clip(expected_pct, eps, None)
        actual_pct = np.clip(actual_pct, eps, None)

        psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
        return float(psi)
        
    except Exception as e:
        logger.error(f"PSI calculation failed: {e}")
        return 0.0

class DriftDetector:
    """Stateful detector holding baseline distributions."""

    PSI_LOW = 0.1
    PSI_HIGH = 0.2
    MIN_SAMPLES = 100

    def __init__(self):
        # Stores (bucket_edges, expected_distribution)
        self._baselines: Dict[str, np.ndarray] = {}

    def fit(self, feature: str, values: np.ndarray) -> None:
        """Fit baseline from training data."""
        self._baselines[feature] = np.asarray(values)
        logger.info(f"Fitted baseline for {feature}: {len(values)} samples")

    def detect(self, feature: str, current_values: np.ndarray) -> DriftResult:
        """Check current data against fitted baseline."""
        from datetime import datetime, timezone
        
        if feature not in self._baselines:
            raise ValueError(f"No baseline found for {feature}")

        baseline = self._baselines[feature]
        psi = calculate_psi(baseline, current_values)

        if psi < self.PSI_LOW:
            severity = DriftSeverity.NONE
        elif psi < self.PSI_HIGH:
            severity = DriftSeverity.MODERATE
        else:
            severity = DriftSeverity.HIGH

        return DriftResult(
            feature=feature,
            psi=psi,
            severity=severity,
            timestamp=datetime.now(timezone.utc).isoformat()
        )