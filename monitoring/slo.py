"""
Service Level Objectives (SLO) Definitions.
Pattern: Observability

Defines the business contracts for system reliability.
These thresholds align with mlops/validation.py but enforce runtime health.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any

logger = logging.getLogger(__name__)

class SLOStatus(Enum):
    OK = "ok"
    WARNING = "warning"
    BREACHED = "breached"

@dataclass
class SLODefinition:
    name: str
    metric: str
    target: float
    op: str # 'lt' (less than) or 'gt' (greater than)
    description: str

    def check(self, current_value: float) -> SLOStatus:
        """Evaluates value against target."""
        is_breached = False
        
        if self.op == "lt":
            is_breached = current_value > self.target
        elif self.op == "gt":
            is_breached = current_value < self.target
            
        if is_breached:
            return SLOStatus.BREACHED
        
        # Simple warning logic: 80% of threshold for 'lt'
        # This is a simplification; production systems often use burn rates.
        if self.op == "lt" and current_value > (self.target * 0.8):
            return SLOStatus.WARNING
            
        return SLOStatus.OK

# Central Definition of Truth
SLO_REGISTRY = {
    "latency_p99": SLODefinition(
        name="p99_latency",
        metric="transitflow_prediction_latency_seconds",
        target=0.050, # 50ms
        op="lt",
        description="99th percentile inference latency"
    ),
    "data_freshness": SLODefinition(
        name="telemetry_freshness",
        metric="transitflow_feature_freshness_seconds",
        target=60.0, # 1 minute
        op="lt",
        description="Age of real-time features from Redis"
    ),
    "shadow_agreement": SLODefinition(
        name="shadow_model_accuracy",
        metric="transitflow_shadow_agreement_rate",
        target=0.90, # 90%
        op="gt",
        description="Agreement rate between Staging and Production models"
    ),
    "drift_psi": SLODefinition(
        name="feature_drift",
        metric="transitflow_feature_drift_psi",
        target=0.2,
        op="lt",
        description="Feature stability index"
    )
}

def check_all_slos(metrics_snapshot: Dict[str, float]) -> Dict[str, Any]:
    """
    Check current metric values against SLO definitions.
    Args:
        metrics_snapshot: Dict of {metric_name: value}
    """
    results = {}
    for key, slo in SLO_REGISTRY.items():
        # Map internal SLO name to prometheus metric name if needed
        # For simplicity, assuming snapshot keys match SLO metric fields
        val = metrics_snapshot.get(slo.metric)
        
        if val is not None:
            status = slo.check(val)
            results[key] = {
                "status": status.value,
                "current": val,
                "target": slo.target
            }
    return results