"""
ML Serving module.

Patterns:
- Distributed Tracing
- Circuit Breaker
- Shadow Deployment [New]

Provides:
- Prediction API with resiliency
- OpenTelemetry tracing
- Shadow model validation
"""

from serving.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    circuit_registry,
)
from serving.tracing import Tracer, get_tracer, init_tracer
from serving.shadow import ShadowPredictor

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CircuitOpenError",
    "circuit_registry",
    "Tracer",
    "get_tracer",
    "init_tracer",
    "ShadowPredictor",
]