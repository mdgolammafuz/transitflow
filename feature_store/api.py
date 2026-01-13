"""
Feature Store API - FastAPI Application.

Pattern: Semantic Interface
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
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
    """Manage service lifecycle."""
    global feature_service
    feature_service = FeatureService(config)
    try:
        feature_service.connect()
        logger.info("Feature service started")
        yield
    finally:
        if feature_service:
            feature_service.close()
        logger.info("Feature service stopped")


app = FastAPI(
    title="TransitFlow Feature Store",
    description="Unified feature access for ML prediction",
    version="1.0.0",
    lifespan=lifespan,
)


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    online_store: str
    offline_store: str
    active_vehicles: int
    metrics: Dict[str, Any]


class OnlineFeaturesResponse(BaseModel):
    """Online features from Redis."""
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
    next_stop_id: Optional[int]
    updated_at: int
    feature_age_ms: int


class OfflineFeaturesResponse(BaseModel):
    """Offline features from PostgreSQL."""
    stop_id: str  # Changed from int to str
    line_id: str
    hour_of_day: int
    day_of_week: int
    avg_delay_seconds: float
    stddev_delay_seconds: float
    avg_dwell_time_seconds: float
    p90_delay_seconds: float
    sample_count: int


class CombinedFeaturesResponse(BaseModel):
    """Combined feature response."""
    vehicle_id: int
    request_timestamp: int
    online_available: bool
    offline_available: bool
    online_features: Optional[OnlineFeaturesResponse] = None
    offline_features: Optional[OfflineFeaturesResponse] = None
    latency_ms: float


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Check service health."""
    if not feature_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    health = feature_service.health_check()
    active = feature_service.get_active_vehicles()

    return HealthResponse(
        status=health["status"],
        online_store=health["online_store"],
        offline_store=health["offline_store"],
        active_vehicles=active,
        metrics=health["metrics"],
    )


@app.get("/features/{vehicle_id}", response_model=CombinedFeaturesResponse)
async def get_features(
    vehicle_id: int,
    # stop_id must be str to match DB VARCHAR and prevent casting errors
    stop_id: Optional[str] = Query(None, description="Stop ID for offline features"),
    line_id: Optional[str] = Query(None, description="Line ID for offline features"),
    hour_of_day: Optional[int] = Query(None, ge=0, le=23, description="Hour (0-23)"),
    day_of_week: Optional[int] = Query(None, ge=1, le=7, description="Day (1=Mon, 7=Sun)"),
):
    """
    Get combined features for a vehicle.
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

    response = {
        "vehicle_id": combined.vehicle_id,
        "request_timestamp": combined.request_timestamp,
        "online_available": combined.online_available,
        "offline_available": combined.offline_available,
        "latency_ms": round(latency_ms, 2),
    }

    if combined.online:
        response["online_features"] = combined.online.to_dict()

    if combined.offline:
        response["offline_features"] = combined.offline.to_dict()

    return JSONResponse(content=response)


@app.get("/metrics")
async def get_metrics():
    """Get service metrics for monitoring."""
    if not feature_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    metrics = feature_service.get_metrics()
    active = feature_service.get_active_vehicles()

    return {
        "active_vehicles": active,
        "online": {
            "requests": metrics.online_requests,
            "hits": metrics.online_hits,
            "errors": metrics.online_errors,
            "hit_rate": round(metrics.online_hit_rate, 3),
        },
        "offline": {
            "requests": metrics.offline_requests,
            "hits": metrics.offline_hits,
            "errors": metrics.offline_errors,
            "hit_rate": round(metrics.offline_hit_rate, 3),
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)