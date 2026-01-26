"""
Online Feature Store - Redis.
Robust: Handles both Hash and JSON String formats from Flink.
Clean: Implements defensive parsing and field mapping.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import redis
from redis.exceptions import RedisError, ResponseError

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
    next_stop_id: Optional[str]
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
            params = self._config.get_redis_params()
            # CRITICAL: Ensure we get Strings, not Bytes
            params["decode_responses"] = True
            self._client = redis.Redis(**params)
            self._client.ping()
            self._connected = True
            logger.info(f"Connected to Online Store (Redis) at {self._config.redis_host}")
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

    def _parse_features(self, data: Dict[str, Any], vehicle_id: int, now_ms: int) -> OnlineFeatures:
        try:
            # 1. Field Mapping (Flink -> Python)
            current_delay = int(float(data.get("current_delay", data.get("delay_seconds", 0))))
            current_speed = float(data.get("current_speed", data.get("speed_ms", 0.0)))

            # 2. Type Safety
            lat = float(data.get("latitude", 0.0))
            lon = float(data.get("longitude", 0.0))
            
            is_stopped_raw = data.get("is_stopped", False)
            if isinstance(is_stopped_raw, str):
                is_stopped = is_stopped_raw.lower() == "true"
            else:
                is_stopped = bool(is_stopped_raw)

            next_stop_raw = data.get("next_stop_id")
            next_stop_id = str(next_stop_raw).strip() if next_stop_raw and str(next_stop_raw) != 'None' else None

            updated_at = int(data.get("updated_at", data.get("event_time_ms", 0)))

            return OnlineFeatures(
                vehicle_id=int(data.get("vehicle_id", vehicle_id)),
                line_id=str(data.get("line_id", "")),
                current_delay=current_delay,
                delay_trend=float(data.get("delay_trend", 0.0)),
                current_speed=current_speed,
                speed_trend=float(data.get("speed_trend", 0.0)),
                is_stopped=is_stopped,
                stopped_duration_ms=int(data.get("stopped_duration_ms", 0)),
                latitude=lat,
                longitude=lon,
                next_stop_id=next_stop_id,
                updated_at=updated_at,
                feature_age_ms=now_ms - updated_at if updated_at > 0 else -1,
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Data Integrity Error for vehicle {vehicle_id}: {e}")
            raise

    def get_features(self, vehicle_id: int) -> Optional[OnlineFeatures]:
        if not self._client:
            return None
            
        key = f"{self._config.redis_key_prefix}{vehicle_id}"
        
        # DEBUG LOGGING (To confirm we are using the right file)
        # print(f"DEBUG: Checking Key: {key}")

        try:
            # Try reading as Hash
            data = self._client.hgetall(key)
            if not data:
                # Try reading as JSON String
                payload = self._client.get(key)
                if payload:
                    data = json.loads(payload)
                else:
                    return None
            
        except ResponseError as e:
            if "WRONGTYPE" in str(e):
                # Fallback for JSON String
                payload = self._client.get(key)
                if payload:
                    data = json.loads(payload)
                else:
                    return None
            else:
                raise e

        if not data:
            return None

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return self._parse_features(data, vehicle_id, now_ms)

    def get_active_vehicle_count(self) -> int:
        if not self._client:
            return 0
        count = 0
        match_pattern = f"{self._config.redis_key_prefix}*"
        for _ in self._client.scan_iter(match=match_pattern, count=100):
            count += 1
        return count