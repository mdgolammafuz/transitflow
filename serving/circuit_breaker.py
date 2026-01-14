"""
Circuit Breaker Pattern.

Pattern: Circuit Breaker

Prevents cascading failures by failing fast when downstream services
are unavailable. Implements three states: CLOSED, OPEN, HALF_OPEN.

Interview talking point:
"The Circuit Breaker prevents cascading failures. If Redis is down,
we don't want every request to wait for a timeout. Instead, after
5 failures, the circuit opens and we immediately return cached or
default features. After 30 seconds, we try one request to check
if the service recovered."
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Callable, TypeVar, Optional, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    half_open_max_calls: int = 3


@dataclass
class CircuitStats:
    """Circuit breaker statistics."""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    total_calls: int = 0
    total_failures: int = 0
    total_short_circuits: int = 0


class CircuitBreaker:
    """
    Circuit breaker for external service calls.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Circuit tripped, requests fail fast
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        self._name = name
        self._config = config or CircuitBreakerConfig()
        self._stats = CircuitStats()
        self._lock = Lock()
        self._half_open_calls = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._stats.state

    @property
    def stats(self) -> CircuitStats:
        with self._lock:
            return CircuitStats(
                state=self._stats.state,
                failure_count=self._stats.failure_count,
                success_count=self._stats.success_count,
                last_failure_time=self._stats.last_failure_time,
                total_calls=self._stats.total_calls,
                total_failures=self._stats.total_failures,
                total_short_circuits=self._stats.total_short_circuits,
            )

    def call(
        self,
        func: Callable[[], T],
        fallback: Optional[Callable[[], T]] = None,
    ) -> T:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Function to execute
            fallback: Optional fallback function if circuit is open

        Returns:
            Function result or fallback result

        Raises:
            CircuitOpenError: If circuit is open and no fallback provided
        """
        with self._lock:
            self._stats.total_calls += 1

            if self._should_allow_request():
                pass
            elif fallback:
                self._stats.total_short_circuits += 1
                logger.debug("Circuit %s open, using fallback", self._name)
                return fallback()
            else:
                self._stats.total_short_circuits += 1
                raise CircuitOpenError(
                    f"Circuit {self._name} is open, failing fast"
                )

        try:
            result = func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            if fallback:
                logger.warning(
                    "Circuit %s call failed, using fallback: %s",
                    self._name,
                    e,
                )
                return fallback()
            raise

    def _should_allow_request(self) -> bool:
        """Check if request should be allowed."""
        if self._stats.state == CircuitState.CLOSED:
            return True

        if self._stats.state == CircuitState.OPEN:
            if self._timeout_elapsed():
                self._transition_to_half_open()
                return True
            return False

        if self._stats.state == CircuitState.HALF_OPEN:
            if self._half_open_calls < self._config.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

        return False

    def _timeout_elapsed(self) -> bool:
        """Check if timeout has elapsed since last failure."""
        return (
            time.time() - self._stats.last_failure_time
            >= self._config.timeout_seconds
        )

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            if self._stats.state == CircuitState.HALF_OPEN:
                self._stats.success_count += 1
                if self._stats.success_count >= self._config.success_threshold:
                    self._transition_to_closed()
            else:
                self._stats.failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._stats.failure_count += 1
            self._stats.total_failures += 1
            self._stats.last_failure_time = time.time()

            if self._stats.state == CircuitState.HALF_OPEN:
                self._transition_to_open()
            elif self._stats.failure_count >= self._config.failure_threshold:
                self._transition_to_open()

    def _transition_to_open(self) -> None:
        """Transition to OPEN state."""
        self._stats.state = CircuitState.OPEN
        logger.warning(
            "Circuit %s OPENED after %d failures",
            self._name,
            self._stats.failure_count,
        )

    def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state."""
        self._stats.state = CircuitState.HALF_OPEN
        self._stats.success_count = 0
        self._half_open_calls = 0
        logger.info("Circuit %s HALF_OPEN, testing recovery", self._name)

    def _transition_to_closed(self) -> None:
        """Transition to CLOSED state."""
        self._stats.state = CircuitState.CLOSED
        self._stats.failure_count = 0
        self._stats.success_count = 0
        self._half_open_calls = 0
        logger.info("Circuit %s CLOSED, service recovered", self._name)

    def reset(self) -> None:
        """Manually reset circuit to closed state."""
        with self._lock:
            self._transition_to_closed()


class CircuitOpenError(Exception):
    """Raised when circuit is open and no fallback available."""

    pass


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    def __init__(self):
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = Lock()

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> CircuitBreaker:
        """Get existing or create new circuit breaker."""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, config)
            return self._breakers[name]

    def get_all_stats(self) -> dict[str, CircuitStats]:
        """Get stats for all circuit breakers."""
        with self._lock:
            return {name: cb.stats for name, cb in self._breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._lock:
            for cb in self._breakers.values():
                cb.reset()


circuit_registry = CircuitBreakerRegistry()
