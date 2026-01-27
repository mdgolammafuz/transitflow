"""
Prometheus Metrics Registry.
Pattern: Observability

Central definition of all application metrics.
Uses official prometheus_client for thread-safe collection.
"""

from prometheus_client import Counter, Gauge, Histogram

# Namespace ensures metrics don't collide with other system metrics
NAMESPACE = "transitflow"

# --- Prediction Metrics ---
PREDICTION_LATENCY = Histogram(
    f"{NAMESPACE}_prediction_latency_seconds",
    "Time spent processing prediction requests",
    ["model_version", "status"], # status: success, error
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)

PREDICTION_COUNT = Counter(
    f"{NAMESPACE}_predictions_total",
    "Total number of predictions made",
    ["model_version", "status"]
)

# --- Feature Store Metrics ---
FEATURE_FRESHNESS = Gauge(
    f"{NAMESPACE}_feature_freshness_seconds",
    "Age of the telemetry data used for prediction",
    ["source"] # source: redis, postgres
)

FEATURE_LOOKUP_LATENCY = Histogram(
    f"{NAMESPACE}_feature_lookup_seconds",
    "Latency of feature store lookups",
    ["store_type"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1]
)

# --- Shadow Deployment Metrics ---
SHADOW_PREDICTION_DIFF = Histogram(
    f"{NAMESPACE}_shadow_prediction_diff_seconds",
    "Absolute difference between Production and Shadow predictions",
    ["feature"], # e.g., 'delay'
    buckets=[1, 5, 10, 30, 60, 120, 300]
)

SHADOW_AGREEMENT_RATE = Gauge(
    f"{NAMESPACE}_shadow_agreement_rate",
    "Percentage of shadow predictions within tolerance of production",
)

# --- Infrastructure ---
CIRCUIT_BREAKER_STATE = Gauge(
    f"{NAMESPACE}_circuit_breaker_state",
    "State of circuit breakers (0=Closed, 1=Open, 2=Half-Open)",
    ["circuit"]
)

# --- ML Health ---
FEATURE_DRIFT_PSI = Gauge(
    f"{NAMESPACE}_feature_drift_psi",
    "Population Stability Index (PSI) for features",
    ["feature_name"]
)