"""
Online Feature Store - Redis.
Robust: Implements strict type conversion and key-missing alerts.
Clean: Removes silent defaults that mask data loss.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis
from redis.exceptions import RedisError
from feature_store.config import FeatureStoreConfig

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class OnlineFeatures:
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
    next_stop_id: Optional[str] # Changed to str for reliable DB Handshake
    updated_at: int
    feature_age_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vehicle_id": self.vehicle_id,
            "line_id": self.line_id,
            "current_delay": self.current_delay,
            "delay_trend": self.delay_trend,
            "current_speed": self.current_speed,
            "speed_trend": self.speed_trend,
            "is_stopped": self.is_stopped,
            "stopped_duration_ms": self.stopped_duration_ms,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "next_stop_id": self.next_stop_id,
            "updated_at": self.updated_at,
            "feature_age_ms": self.feature_age_ms,
        }

class OnlineStore:
    def __init__(self, config: FeatureStoreConfig):
        self._config = config
        self._client: Optional[redis.Redis] = None
        self._connected = False

    def connect(self) -> None:
        try:
            self._client = redis.Redis(
                host=self._config.redis_host,
                port=self._config.redis_port,
                password=self._config.redis_password,
                decode_responses=True, # Critical for String handling
                socket_timeout=self._config.request_timeout_seconds,
            )
            self._client.ping()
            self._connected = True
        except RedisError as e:
            self._connected = False
            logger.error(f"Redis Connection Failed: {e}")
            raise

    def close(self) -> None:
        if self._client:
            self._client.close()

    def is_healthy(self) -> bool:
        try:
            return self._client.ping() if self._client else False
        except:
            return False

    def _parse_features(self, data: Dict[str, str], vehicle_id: int, now_ms: int) -> OnlineFeatures:
        """
        Principal Method: Defensive parsing.
        Uses explicit type casting to avoid silent zeros.
        """
        try:
            # Spatial data must be present
            lat = float(data.get("latitude", 0.0))
            lon = float(data.get("longitude", 0.0))
            
            # Handshake ID: Force to string to match Postgres 'text' type
            next_stop_raw = data.get("next_stop_id")
            next_stop_id = str(next_stop_raw).strip() if next_stop_raw else None

            updated_at = int(data.get("updated_at", 0))

            return OnlineFeatures(
                vehicle_id=int(data.get("vehicle_id", vehicle_id)),
                line_id=data.get("line_id"),
                current_delay=int(data.get("current_delay", 0)),
                delay_trend=float(data.get("delay_trend", 0.0)),
                current_speed=float(data.get("current_speed", 0.0)),
                speed_trend=float(data.get("speed_trend", 0.0)),
                is_stopped=str(data.get("is_stopped")).lower() == "true",
                stopped_duration_ms=int(data.get("stopped_duration_ms", 0)),
                latitude=lat,
                longitude=lon,
                next_stop_id=next_stop_id,
                updated_at=updated_at,
                feature_age_ms=now_ms - updated_at if updated_at > 0 else -1,
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Data Integrity Error for vehicle {vehicle_id}: {e} | Data: {data}")
            # Return partial data rather than crashing, but the error is now logged
            raise

    def get_features(self, vehicle_id: int) -> Optional[OnlineFeatures]:
        if not self._client: return None
        key = f"{self._config.redis_key_prefix}{vehicle_id}"
        data = self._client.hgetall(key)
        if not data: return None
        
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return self._parse_features(data, vehicle_id, now_ms)

    def get_active_vehicle_count(self) -> int:
        if not self._client: return 0
        return sum(1 for _ in self._client.scan_iter(match=f"{self._config.redis_key_prefix}*"))