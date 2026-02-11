"""
OpenTelemetry Distributed Tracing.

Pattern: Distributed Tracing

Provides request tracing across services for debugging and performance
analysis.

"OpenTelemetry gives us distributed tracing. Each prediction request
gets a trace ID that follows through Feature Store calls, model
inference, and Kafka logging. When latency spikes, we can see exactly
which component is slow. This is critical for debugging production
issues in a distributed system."
"""

import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Dict, Generator, Optional

logger = logging.getLogger(__name__)

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.trace import Status, StatusCode

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    logger.info("OpenTelemetry not installed, using local tracing")


@dataclass
class SpanContext:
    """Local span context for when OpenTelemetry is not available."""

    trace_id: str
    span_id: str
    name: str
    start_time: float
    end_time: Optional[float] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    status: str = "OK"
    events: list = field(default_factory=list)


class Tracer:
    """Wrapper around OpenTelemetry tracer with fallback."""

    def __init__(
        self,
        service_name: str = "transitflow-serving",
        otlp_endpoint: Optional[str] = None,
    ):
        self._service_name = service_name
        self._tracer: Optional[Any] = None
        self._local_spans: list[SpanContext] = []

        if OTEL_AVAILABLE and otlp_endpoint:
            self._init_otel(otlp_endpoint)
        else:
            logger.info("Using local tracing (no OTLP endpoint)")

    def _init_otel(self, endpoint: str) -> None:
        """Initialize OpenTelemetry."""
        try:
            resource = Resource.create({"service.name": self._service_name})
            provider = TracerProvider(resource=resource)

            exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
            processor = BatchSpanProcessor(exporter)
            provider.add_span_processor(processor)

            trace.set_tracer_provider(provider)
            self._tracer = trace.get_tracer(self._service_name)

            logger.info("OpenTelemetry initialized with endpoint %s", endpoint)
        except Exception as e:
            logger.warning("Failed to initialize OpenTelemetry: %s", e)
            self._tracer = None

    @contextmanager
    def span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Generator[SpanContext, None, None]:
        """
        Create a traced span.

        Args:
            name: Span name
            attributes: Optional span attributes

        Yields:
            SpanContext for adding events/attributes
        """
        if self._tracer and OTEL_AVAILABLE:
            with self._tracer.start_as_current_span(name) as otel_span:
                if attributes:
                    for key, value in attributes.items():
                        otel_span.set_attribute(key, value)

                ctx = SpanContext(
                    trace_id=format(otel_span.get_span_context().trace_id, "032x"),
                    span_id=format(otel_span.get_span_context().span_id, "016x"),
                    name=name,
                    start_time=time.time(),
                    attributes=attributes or {},
                )
                try:
                    yield ctx
                    otel_span.set_status(Status(StatusCode.OK))
                    ctx.status = "OK"
                except Exception as e:
                    otel_span.set_status(Status(StatusCode.ERROR, str(e)))
                    otel_span.record_exception(e)
                    ctx.status = "ERROR"
                    raise
                finally:
                    ctx.end_time = time.time()
        else:
            ctx = SpanContext(
                trace_id=uuid.uuid4().hex,
                span_id=uuid.uuid4().hex[:16],
                name=name,
                start_time=time.time(),
                attributes=attributes or {},
            )
            try:
                yield ctx
                ctx.status = "OK"
            except Exception:
                ctx.status = "ERROR"
                raise
            finally:
                ctx.end_time = time.time()
                self._local_spans.append(ctx)

    def traced(self, name: Optional[str] = None):
        """Decorator to trace a function."""

        def decorator(func):
            span_name = name or f"{func.__module__}.{func.__name__}"

            @wraps(func)
            def wrapper(*args, **kwargs):
                with self.span(span_name):
                    return func(*args, **kwargs)

            return wrapper

        return decorator

    def get_current_trace_id(self) -> Optional[str]:
        """Get current trace ID if available."""
        if self._tracer and OTEL_AVAILABLE:
            current_span = trace.get_current_span()
            if current_span:
                return format(current_span.get_span_context().trace_id, "032x")
        return None

    def get_local_spans(self) -> list[SpanContext]:
        """Get locally recorded spans (for testing/debugging)."""
        return list(self._local_spans)

    def clear_local_spans(self) -> None:
        """Clear local span storage."""
        self._local_spans.clear()


_default_tracer: Optional[Tracer] = None


def get_tracer() -> Tracer:
    """Get the default tracer instance."""
    global _default_tracer
    if _default_tracer is None:
        _default_tracer = Tracer()
    return _default_tracer


def init_tracer(
    service_name: str = "transitflow-serving",
    otlp_endpoint: Optional[str] = None,
) -> Tracer:
    """Initialize the default tracer."""
    global _default_tracer
    _default_tracer = Tracer(service_name, otlp_endpoint)
    return _default_tracer
