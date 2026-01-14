"""
Feature Store API - FastAPI Application.

Pattern: Semantic Interface
Robustness: Handles partial store availability and supplemental data placeholders.
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

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
    """
    Manage service lifecycle robustly.
    Ensures the API stays up even if one store (like Redis) fails to connect.
    """
    global feature_service
    feature_service = FeatureService(config)
    try:
        # Hardened: Attempt connections but catch errors to prevent startup crash
        feature_service.connect()
        logger.info("Feature service initialized successfully")
    except Exception as e:
        # This is where we handle the "Authentication required" or "Connection refused" errors
        logger.error(f"Feature service started in DEGRADED mode. Connection error: {e}")
        logger.warning("Online features may be unavailable, but Offline lookups will proceed.")
    
    yield  # The API runs here
    
    # Cleanup on shutdown
    if feature_service:
        try:
            feature_service.close()
            logger.info("Feature service connections closed")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

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

    @field_validator('latitude', 'longitude')
    @classmethod
    def validate_coordinates(cls, v: float) -> float:
        """
        Robustness: Allows 0.0 coordinates. 
        Supplemental seed data uses 0.0 as a placeholder; we must not crash the API.
        """
        return v


class OfflineFeaturesResponse(BaseModel):
    """Offline features from PostgreSQL - Aligned with dbt marts."""
    stop_id: str
    line_id: str
    hour_of_day: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=0, le=7)
    avg_delay_seconds: float
    stddev_delay_seconds: float
    avg_dwell_time_seconds: float
    p90_delay_seconds: float = 0.0
    sample_count: int

    @field_validator('sample_count')
    @classmethod
    def validate_sample_size(cls, v: int) -> int:
        """Robustness: Ensure we have at least one sample to provide context."""
        if v < 1:
            raise ValueError('No historical samples available for this context')
        return v


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
    """Check service health across both stores."""
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
    stop_id: Optional[str] = Query(None, description="Stop ID for offline features"),
    line_id: Optional[str] = Query(None, description="Line ID for offline features"),
    hour_of_day: Optional[int] = Query(None, ge=0, le=23, description="Hour context"),
    day_of_week: Optional[int] = Query(None, ge=0, le=7, description="Day context (0-6 or 1-7)"),
):
    """
    Unified endpoint for real-time and historical features.
    If Redis/Flink is down, it relies on Query parameters for Offline context.
    """
    if not feature_service:
        raise HTTPException(status_code=503, detail="Service not initialized")

    start = time.perf_counter()

    # The FeatureService handles the orchestration and default system time context
    combined = feature_service.get_features(
        vehicle_id=vehicle_id,
        stop_id=stop_id,
        line_id=line_id,
        hour_of_day=hour_of_day,
        day_of_week=day_of_week,
    )

    latency_ms = (time.perf_counter() - start) * 1000

    # Build response dictionary
    res_data = {
        "vehicle_id": combined.vehicle_id,
        "request_timestamp": combined.request_timestamp,
        "online_available": combined.online_available,
        "offline_available": combined.offline_available,
        "latency_ms": round(latency_ms, 2),
        "online_features": None,
        "offline_features": None
    }

    # Populate online features if available and valid
    if combined.online:
        try:
            res_data["online_features"] = combined.online.to_dict()
        except Exception as e:
            logger.warning(f"Online validation failed for vehicle {vehicle_id}: {e}")
            res_data["online_available"] = False

    # Populate offline features if available and valid
    if combined.offline:
        try:
            res_data["offline_features"] = combined.offline.to_dict()
        except Exception as e:
            logger.warning(f"Offline validation failed for stop {stop_id}: {e}")
            res_data["offline_available"] = False

    return JSONResponse(content=res_data)


@app.get("/metrics")
async def get_metrics():
    """Monitoring metrics for both stores."""
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
    logger.info("Starting TransitFlow Feature API on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)