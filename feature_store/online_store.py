"""
Online Feature Store - Redis.

Pattern: Semantic Interface
Real-time features written by Flink, read by serving API.
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
    """
    Real-time features for a vehicle.
    Frozen to ensure immutability during the request lifecycle.
    """

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

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
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
    """Redis-based online feature store."""

    def __init__(self, config: FeatureStoreConfig):
        self._config = config
        self._client: Optional[redis.Redis] = None
        self._connected = False

    def connect(self) -> None:
        """Establish Redis connection with timeouts and authentication."""
        try:
            # Uses the authenticated connection parameters from FeatureStoreConfig
            self._client = redis.Redis(
                host=self._config.redis_host,
                port=self._config.redis_port,
                password=self._config.redis_password,
                decode_responses=True,
                socket_timeout=self._config.request_timeout_seconds,
                socket_connect_timeout=self._config.request_timeout_seconds,
            )
            self._client.ping()
            self._connected = True
            logger.info(
                "Connected to Redis at %s:%d (Online Store)",
                self._config.redis_host,
                self._config.redis_port,
            )
        except RedisError as e:
            self._connected = False
            logger.error("Failed to connect to Redis: %s", e)
            raise

    def close(self) -> None:
        """Close Redis connection safely."""
        if self._client:
            self._client.close()
            self._connected = False

    def is_healthy(self) -> bool:
        """Check if Redis connection is healthy."""
        if not self._client or not self._connected:
            return False
        try:
            self._client.ping()
            return True
        except RedisError:
            return False

    def _parse_features(self, data: Dict[str, str], vehicle_id: int, now_ms: int) -> OnlineFeatures:
        """Helper to parse raw Redis hash data into OnlineFeatures object."""
        updated_at = int(data.get("updated_at", 0))
        lat = float(data.get("latitude", 0.0))
        lon = float(data.get("longitude", 0.0))

        # Hardening: Trace placeholder coordinates coming from the real-time stream
        if lat == 0.0 or lon == 0.0:
            logger.debug(f"Placeholder coordinates detected in Redis for vehicle {vehicle_id}")

        return OnlineFeatures(
            vehicle_id=int(data.get("vehicle_id", vehicle_id)),
            line_id=data.get("line_id"),
            current_delay=int(data.get("current_delay", 0)),
            delay_trend=float(data.get("delay_trend", 0.0)),
            current_speed=float(data.get("current_speed", 0.0)),
            speed_trend=float(data.get("speed_trend", 0.0)),
            is_stopped=data.get("is_stopped", "false").lower() == "true",
            stopped_duration_ms=int(data.get("stopped_duration_ms", 0)),
            latitude=lat,
            longitude=lon,
            next_stop_id=int(data["next_stop_id"]) if data.get("next_stop_id") else None,
            updated_at=updated_at,
            feature_age_ms=now_ms - updated_at,
        )

    def get_features(self, vehicle_id: int) -> Optional[OnlineFeatures]:
        """Get real-time features for a vehicle."""
        if not self._client:
            raise RuntimeError("Not connected to Redis (Online Store)")

        key = f"{self._config.redis_key_prefix}{vehicle_id}"
        try:
            data = self._client.hgetall(key)
            if not data:
                return None

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            return self._parse_features(data, vehicle_id, now_ms)
        except RedisError as e:
            logger.error("Redis error fetching vehicle %d: %s", vehicle_id, e)
            raise

    def get_features_batch(self, vehicle_ids: List[int]) -> Dict[int, Optional[OnlineFeatures]]:
        """Get features for multiple vehicles efficiently using Redis pipelines."""
        if not self._client:
            raise RuntimeError("Not connected to Redis")

        results: Dict[int, Optional[OnlineFeatures]] = {}
        try:
            pipe = self._client.pipeline()
            for vid in vehicle_ids:
                pipe.hgetall(f"{self._config.redis_key_prefix}{vid}")

            responses = pipe.execute()
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            for vid, data in zip(vehicle_ids, responses):
                results[vid] = self._parse_features(data, vid, now_ms) if data else None
            return results
        except RedisError as e:
            logger.error("Redis pipeline error: %s", e)
            raise

    def get_active_vehicle_count(self) -> int:
        """Get count of active vehicles using SCAN (O(N) but non-blocking)."""
        if not self._client:
            raise RuntimeError("Not connected to Redis")
        try:
            count = 0
            # Matches keys with the prefix defined in config
            for _ in self._client.scan_iter(match=f"{self._config.redis_key_prefix}*", count=100):
                count += 1
            return count
        except RedisError as e:
            logger.error("Redis scan error: %s", e)
            return 0
