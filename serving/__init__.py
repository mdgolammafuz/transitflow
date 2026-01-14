"""
ML Serving module.

Pattern: 
Distributed Tracing
Circuit Breaker

Provides:
- Prediction API with circuit breaker protection
- OpenTelemetry distributed tracing
- Model caching and hot reload
- Prediction logging to Kafka
"""

from serving.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitOpenError,
    circuit_registry,
)
from serving.tracing import Tracer, get_tracer, init_tracer

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CircuitOpenError",
    "circuit_registry",
    "Tracer",
    "get_tracer",
    "init_tracer",
]
