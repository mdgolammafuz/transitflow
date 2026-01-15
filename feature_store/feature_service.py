"""
Feature Service - Unified Feature Access.

Pattern:
Semantic Interface
ML Reproducibility

Combines online (Redis) and offline (PostgreSQL) features into a unified
interface. Ensures training-serving consistency by using the same feature
computation logic and default values.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from feature_store.config import FeatureStoreConfig
from feature_store.offline_store import OfflineStore, StopFeatures
from feature_store.online_store import OnlineFeatures, OnlineStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CombinedFeatures:
    """Combined online + offline features for prediction."""

    vehicle_id: int
    request_timestamp: int
    online: Optional[OnlineFeatures] = None
    offline: Optional[StopFeatures] = None
    online_available: bool = False
    offline_available: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        result = {
            "vehicle_id": self.vehicle_id,
            "request_timestamp": self.request_timestamp,
            "online_available": self.online_available,
            "offline_available": self.offline_available,
        }

        if self.online:
            result["online_features"] = self.online.to_dict()
        if self.offline:
            result["offline_features"] = self.offline.to_dict()

        return result

    def to_feature_vector(self) -> Dict[str, Any]:
        """
        Convert to flat feature vector for ML model.
        Hardened: Uses None/Null for missing coordinates instead of 0.0 to prevent
        data corruption in the prediction layer.
        """
        features: Dict[str, Any] = {
            "vehicle_id": self.vehicle_id,
        }

        # Online (Real-time) Logic
        if self.online:
            features.update(
                {
                    "current_delay": self.online.current_delay,
                    "delay_trend": self.online.delay_trend,
                    "current_speed": self.online.current_speed,
                    "speed_trend": self.online.speed_trend,
                    "is_stopped": 1 if self.online.is_stopped else 0,
                    "stopped_duration_ms": self.online.stopped_duration_ms,
                    "latitude": self.online.latitude,
                    "longitude": self.online.longitude,
                    "feature_age_ms": self.online.feature_age_ms,
                }
            )
        else:
            features.update(
                {
                    "current_delay": 0,
                    "delay_trend": 0.0,
                    "current_speed": 0.0,
                    "speed_trend": 0.0,
                    "is_stopped": 0,
                    "stopped_duration_ms": 0,
                    "latitude": None,
                    "longitude": None,
                    "feature_age_ms": -1,
                }
            )

        # Offline (Historical) Logic
        if self.offline:
            features.update(
                {
                    "historical_avg_delay": self.offline.avg_delay_seconds,
                    "historical_stddev_delay": self.offline.stddev_delay_seconds,
                    "historical_p90_delay": self.offline.p90_delay_seconds,
                    "historical_sample_count": self.offline.sample_count,
                }
            )
        else:
            features.update(
                {
                    "historical_avg_delay": 0.0,
                    "historical_stddev_delay": 0.0,
                    "historical_p90_delay": 0.0,
                    "historical_sample_count": 0,
                }
            )

        return features


@dataclass
class FeatureServiceMetrics:
    """Metrics for feature service health monitoring."""

    online_requests: int = 0
    online_hits: int = 0
    online_errors: int = 0
    offline_requests: int = 0
    offline_hits: int = 0
    offline_errors: int = 0

    @property
    def online_hit_rate(self) -> float:
        return self.online_hits / self.online_requests if self.online_requests > 0 else 0.0

    @property
    def offline_hit_rate(self) -> float:
        return self.offline_hits / self.offline_requests if self.offline_requests > 0 else 0.0


class FeatureService:
    """Unified feature service combining online and offline stores."""

    def __init__(self, config: FeatureStoreConfig):
        self._config = config
        self._online_store = OnlineStore(config)
        self._offline_store = OfflineStore(config)
        self._metrics = FeatureServiceMetrics()

    def connect(self) -> None:
        self._online_store.connect()
        self._offline_store.connect()

    def close(self) -> None:
        self._online_store.close()
        self._offline_store.close()

    def health_check(self) -> Dict[str, Any]:
        online_healthy = self._online_store.is_healthy()
        offline_healthy = self._offline_store.is_healthy()

        return {
            "status": "healthy" if (online_healthy and offline_healthy) else "degraded",
            "online_store": "healthy" if online_healthy else "unhealthy",
            "offline_store": "healthy" if offline_healthy else "unhealthy",
            "metrics": {
                "online_hit_rate": round(self._metrics.online_hit_rate, 3),
                "offline_hit_rate": round(self._metrics.offline_hit_rate, 3),
                "online_errors": self._metrics.online_errors,
                "offline_errors": self._metrics.offline_errors,
            },
        }

    def get_features(
        self,
        vehicle_id: int,
        stop_id: Optional[str] = None,
        line_id: Optional[str] = None,
        hour_of_day: Optional[int] = None,
        day_of_week: Optional[int] = None,
    ) -> CombinedFeatures:
        """
        Orchestrates retrieval with context inference and fallback.
        """
        now = datetime.now(timezone.utc)
        request_timestamp = int(now.timestamp() * 1000)

        # Consistent context inference
        h_ctx = hour_of_day if hour_of_day is not None else now.hour

        # Align with SQL/DBT: Python's weekday() is 0-6 (Mon-Sun)
        # We pass this to the offline_store which uses fallback logic for 0-7 compatibility
        d_ctx = day_of_week if day_of_week is not None else now.weekday()

        online_data = None
        offline_data = None

        # 1. Fetch Online Features
        self._metrics.online_requests += 1
        try:
            online_data = self._online_store.get_features(vehicle_id)
            if online_data:
                self._metrics.online_hits += 1
                # Context propagation
                if stop_id is None and online_data.next_stop_id:
                    stop_id = str(online_data.next_stop_id)
                if line_id is None:
                    line_id = online_data.line_id
        except Exception as e:
            self._metrics.online_errors += 1
            logger.error(f"Error fetching online features for vehicle {vehicle_id}: {e}")

        # 2. Fetch Offline Features
        if stop_id:
            self._metrics.offline_requests += 1
            try:
                offline_data = self._offline_store.get_stop_features(
                    stop_id=stop_id, hour_of_day=h_ctx, day_of_week=d_ctx
                )
                if offline_data:
                    self._metrics.offline_hits += 1
            except Exception as e:
                self._metrics.offline_errors += 1
                logger.error(f"Error fetching offline features for stop {stop_id}: {e}")

        return CombinedFeatures(
            vehicle_id=vehicle_id,
            request_timestamp=request_timestamp,
            online=online_data,
            offline=offline_data,
            online_available=(online_data is not None),
            offline_available=(offline_data is not None),
        )

    def get_active_vehicles(self) -> int:
        return self._online_store.get_active_vehicle_count()

    def get_metrics(self) -> FeatureServiceMetrics:
        return self._metrics

    def reset_metrics(self) -> None:
        self._metrics = FeatureServiceMetrics()
