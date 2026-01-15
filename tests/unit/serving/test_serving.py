"""
Unit tests for Serving components.

Patterns:
Distributed Tracing
Circuit Breaker
"""

import time

import pytest


class TestCircuitBreakerConfig:
    """Tests for CircuitBreakerConfig."""

    def test_default_config(self):
        """Default configuration values."""
        from serving.circuit_breaker import CircuitBreakerConfig

        config = CircuitBreakerConfig()

        assert config.failure_threshold == 5
        assert config.success_threshold == 2
        assert config.timeout_seconds == 30.0
        assert config.half_open_max_calls == 3

    def test_custom_config(self):
        """Custom configuration values."""
        from serving.circuit_breaker import CircuitBreakerConfig

        config = CircuitBreakerConfig(
            failure_threshold=3,
            timeout_seconds=10.0,
        )

        assert config.failure_threshold == 3
        assert config.timeout_seconds == 10.0


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def test_initial_state_closed(self):
        """Circuit starts in closed state."""
        from serving.circuit_breaker import CircuitBreaker, CircuitState

        cb = CircuitBreaker("test")

        assert cb.state == CircuitState.CLOSED

    def test_success_keeps_closed(self):
        """Successful calls keep circuit closed."""
        from serving.circuit_breaker import CircuitBreaker, CircuitState

        cb = CircuitBreaker("test")

        result = cb.call(lambda: "success")

        assert result == "success"
        assert cb.state == CircuitState.CLOSED
        assert cb.stats.failure_count == 0

    def test_failure_increments_count(self):
        """Failed calls increment failure count."""
        from serving.circuit_breaker import CircuitBreaker, CircuitState

        cb = CircuitBreaker("test")

        def failing():
            raise Exception("error")

        with pytest.raises(Exception):
            cb.call(failing)

        assert cb.stats.failure_count == 1
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold(self):
        """Circuit opens after failure threshold."""
        from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState

        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker("test", config)

        def failing():
            raise Exception("error")

        for _ in range(3):
            with pytest.raises(Exception):
                cb.call(failing)

        assert cb.state == CircuitState.OPEN

    def test_open_uses_fallback(self):
        """Open circuit uses fallback."""
        from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState

        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker("test", config)

        def failing():
            raise Exception("error")

        with pytest.raises(Exception):
            cb.call(failing)

        assert cb.state == CircuitState.OPEN

        result = cb.call(failing, fallback=lambda: "fallback")

        assert result == "fallback"
        assert cb.stats.total_short_circuits == 1

    def test_open_raises_without_fallback(self):
        """Open circuit raises without fallback."""
        from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitOpenError

        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker("test", config)

        def failing():
            raise Exception("error")

        with pytest.raises(Exception):
            cb.call(failing)

        with pytest.raises(CircuitOpenError):
            cb.call(failing)

    def test_half_open_after_timeout(self):
        """Circuit becomes half-open after timeout."""
        from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState

        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=0.1,
        )
        cb = CircuitBreaker("test", config)

        def failing():
            raise Exception("error")

        with pytest.raises(Exception):
            cb.call(failing)

        assert cb.state == CircuitState.OPEN

        time.sleep(0.15)

        cb.call(lambda: "success")

        assert cb.state in [CircuitState.HALF_OPEN, CircuitState.CLOSED]

    def test_closes_after_success_threshold(self):
        """Circuit closes after success threshold in half-open."""
        from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState

        config = CircuitBreakerConfig(
            failure_threshold=1,
            success_threshold=2,
            timeout_seconds=0.01,
        )
        cb = CircuitBreaker("test", config)

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("error")))

        time.sleep(0.02)

        cb.call(lambda: "success")
        cb.call(lambda: "success")

        assert cb.state == CircuitState.CLOSED

    def test_reset(self):
        """Manual reset closes circuit."""
        from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState

        config = CircuitBreakerConfig(failure_threshold=1)
        cb = CircuitBreaker("test", config)

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception()))

        assert cb.state == CircuitState.OPEN

        cb.reset()

        assert cb.state == CircuitState.CLOSED

    def test_stats_tracking(self):
        """Stats are tracked correctly."""
        from serving.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker("test")

        cb.call(lambda: "ok")
        cb.call(lambda: "ok")

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception()))

        stats = cb.stats

        assert stats.total_calls == 3
        assert stats.total_failures == 1


class TestCircuitBreakerRegistry:
    """Tests for CircuitBreakerRegistry."""

    def test_get_or_create(self):
        """Registry creates and caches breakers."""
        from serving.circuit_breaker import CircuitBreakerRegistry

        registry = CircuitBreakerRegistry()

        cb1 = registry.get_or_create("test")
        cb2 = registry.get_or_create("test")

        assert cb1 is cb2

    def test_get_all_stats(self):
        """Registry returns all stats."""
        from serving.circuit_breaker import CircuitBreakerRegistry

        registry = CircuitBreakerRegistry()

        registry.get_or_create("cb1")
        registry.get_or_create("cb2")

        stats = registry.get_all_stats()

        assert "cb1" in stats
        assert "cb2" in stats

    def test_reset_all(self):
        """Registry resets all breakers."""
        from serving.circuit_breaker import (
            CircuitBreakerConfig,
            CircuitBreakerRegistry,
            CircuitState,
        )

        registry = CircuitBreakerRegistry()

        config = CircuitBreakerConfig(failure_threshold=1)
        cb = registry.get_or_create("test", config)

        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception()))

        assert cb.state == CircuitState.OPEN

        registry.reset_all()

        assert cb.state == CircuitState.CLOSED


class TestTracer:
    """Tests for Tracer."""

    def test_tracer_span_context(self):
        """Span creates context."""
        from serving.tracing import Tracer

        tracer = Tracer()

        with tracer.span("test_span") as ctx:
            assert ctx.name == "test_span"
            assert ctx.trace_id is not None
            assert ctx.span_id is not None

    def test_tracer_span_attributes(self):
        """Span stores attributes."""
        from serving.tracing import Tracer

        tracer = Tracer()

        with tracer.span("test", {"key": "value"}) as ctx:
            assert ctx.attributes.get("key") == "value"

    def test_tracer_span_timing(self):
        """Span records timing."""
        from serving.tracing import Tracer

        tracer = Tracer()

        with tracer.span("test") as ctx:
            time.sleep(0.01)

        assert ctx.end_time is not None
        assert ctx.end_time > ctx.start_time

    def test_tracer_span_error(self):
        """Span records errors."""
        from serving.tracing import Tracer

        tracer = Tracer()

        with pytest.raises(ValueError):
            with tracer.span("test") as ctx:
                raise ValueError("test error")

        assert ctx.status == "ERROR"

    def test_tracer_local_spans(self):
        """Local spans are recorded."""
        from serving.tracing import Tracer

        tracer = Tracer()
        tracer.clear_local_spans()

        with tracer.span("span1"):
            pass

        with tracer.span("span2"):
            pass

        spans = tracer.get_local_spans()

        assert len(spans) == 2
        assert spans[0].name == "span1"
        assert spans[1].name == "span2"

    def test_tracer_decorator(self):
        """Traced decorator works."""
        from serving.tracing import Tracer

        tracer = Tracer()
        tracer.clear_local_spans()

        @tracer.traced("my_function")
        def my_function():
            return 42

        result = my_function()

        assert result == 42

        spans = tracer.get_local_spans()
        assert len(spans) == 1
        assert spans[0].name == "my_function"


class TestTracerGlobals:
    """Tests for global tracer functions."""

    def test_get_tracer(self):
        """get_tracer returns singleton."""
        from serving.tracing import get_tracer

        t1 = get_tracer()
        t2 = get_tracer()

        assert t1 is t2

    def test_init_tracer(self):
        """init_tracer creates new tracer."""
        from serving.tracing import get_tracer, init_tracer

        tracer = init_tracer("test-service")

        assert get_tracer() is tracer
