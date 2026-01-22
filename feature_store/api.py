"""
Feature Store API - FastAPI Application.

Pattern: Semantic Interface & Strict Schema Enforcement
Robustness: Aligned with Java Flink Sink and validated dbt Marts.
Performance: Uses native FastAPI serialization to minimize latency.
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from feature_store.config import FeatureStoreConfig
from feature_store.feature_service import FeatureService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

config = FeatureStoreConfig.from_env()
feature_service: Optional[FeatureService] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage service lifecycle with robust connection handling."""
    global feature_service
    feature_service = FeatureService(config)
    try:
        feature_service.connect()
        logger.info("Feature service initialized successfully")
    except Exception as e:
        logger.error(f"Feature service started in DEGRADED mode: {e}")

    yield

    if feature_service:
        feature_service.close()


app = FastAPI(
    title="TransitFlow Feature Store",
    description="Unified feature access for ML prediction",
    version="1.1.0",
    lifespan=lifespan,
)


class HealthResponse(BaseModel):
    status: str
    online_store: str
    offline_store: str
    active_vehicles: int
    metrics: Dict[str, Any]


class OnlineFeaturesResponse(BaseModel):
    """Synchronized with Java RedisSink and Python OnlineStore."""

    vehicle_id: int
    line_id: Optional[str]
    current_delay: int
    delay_trend: float
    current_speed: float
    speed_trend: float
    is_stopped: bool
    stopped_duration_ms: int
    latitude: float
    longitude: float
    next_stop_id: Optional[str]  # Forced to String for reliable Handshake
    updated_at: int
    feature_age_ms: int


class OfflineFeaturesResponse(BaseModel):
    """Synchronized with validated Phase 5 dbt Gold Marts."""

    stop_id: str
    line_id: str
    hour_of_day: int
    day_of_week: int
    latitude: float
    longitude: float
    historical_avg_delay: float
    historical_stddev_delay: float
    avg_dwell_time_ms: float
    historical_arrival_count: int


class CombinedFeaturesResponse(BaseModel):
    vehicle_id: int
    request_timestamp: int
    online_available: bool
    offline_available: bool
    online_features: Optional[OnlineFeaturesResponse] = None
    offline_features: Optional[OfflineFeaturesResponse] = None
    latency_ms: float


@app.get("/health", response_model=HealthResponse)
async def health_check():
    if not feature_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    health = feature_service.health_check()
    active = feature_service.get_active_vehicles()
    return {**health, "active_vehicles": active}


@app.get("/features/{vehicle_id}", response_model=CombinedFeaturesResponse)
async def get_features(
    vehicle_id: int,
    stop_id: Optional[str] = Query(None, description="Force specific Stop ID"),
    line_id: Optional[str] = Query(None, description="Force specific Line ID"),
    hour_of_day: Optional[int] = Query(None, ge=0, le=23),
    day_of_week: Optional[int] = Query(None, ge=0, le=7),
):
    """
    Primary endpoint for ML Serving layer.
    Joins Online (Redis) and Offline (Postgres) data using next_stop_id as the key.
    """
    if not feature_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    start = time.perf_counter()
    combined = feature_service.get_features(
        vehicle_id=vehicle_id,
        stop_id=stop_id,
        line_id=line_id,
        hour_of_day=hour_of_day,
        day_of_week=day_of_week,
    )
    latency_ms = (time.perf_counter() - start) * 1000

    # Returning a dict allows Pydantic to handle the
    # complex serialization/type-checking automatically.
    return {
        "vehicle_id": combined.vehicle_id,
        "request_timestamp": combined.request_timestamp,
        "online_available": combined.online_available,
        "offline_available": combined.offline_available,
        "latency_ms": round(latency_ms, 2),
        "online_features": combined.online.to_dict() if combined.online else None,
        "offline_features": combined.offline.to_dict() if combined.offline else None,
    }


@app.get("/metrics")
async def get_metrics():
    if not feature_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    metrics = feature_service.get_metrics()
    return {
        "online": {
            "requests": metrics.online_requests,
            "hits": metrics.online_hits,
            "hit_rate": round(metrics.online_hit_rate, 3),
        },
        "offline": {
            "requests": metrics.offline_requests,
            "hits": metrics.offline_hits,
            "hit_rate": round(metrics.offline_hit_rate, 3),
        },
    }


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("API_HOST", "127.0.0.1")
    port = int(os.environ.get("API_PORT", 8000))
    uvicorn.run(app, host=host, port=port)
