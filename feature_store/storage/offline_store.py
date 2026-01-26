"""
Offline Feature Store - PostgreSQL.
Hardened: Implements internal Connection Pooling for low-latency API access.
Aligned: Strictly targets marts.fct_stop_arrivals (Phase 5).
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Optional

import psycopg2.pool
from psycopg2.extras import RealDictCursor

from feature_store.config import FeatureStoreConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StopFeatures:
    """Historical features for a stop. Aligned with fct_stop_arrivals schema."""

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

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stop_id": self.stop_id,
            "line_id": self.line_id,
            "hour_of_day": self.hour_of_day,
            "day_of_week": self.day_of_week,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "historical_avg_delay": self.historical_avg_delay,
            "historical_stddev_delay": self.historical_stddev_delay,
            "avg_dwell_time_ms": self.avg_dwell_time_ms,
            "historical_arrival_count": self.historical_arrival_count,
        }


class OfflineStore:
    """PostgreSQL-based offline feature store targeting the 'marts' schema."""

    def __init__(self, config: FeatureStoreConfig):
        self._config = config
        self._pool: Optional[psycopg2.pool.SimpleConnectionPool] = None

    def connect(self) -> None:
        """Initializes the connection pool."""
        try:
            params = self._config.get_postgres_params()
            # Initialize a pool with min 1 and max 10 connections
            self._pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                **params
            )
            logger.info("Connected to Offline Store (Postgres Pool initialized)")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def close(self):
        if self._pool:
            self._pool.closeall()
            logger.info("Closed Postgres connection pool")

    def is_healthy(self) -> bool:
        if not self._pool:
            return False
        try:
            # Checkout a connection just to check health
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Robust check: Verify we can actually read the table
                    cur.execute("SELECT 1 FROM marts.fct_stop_arrivals LIMIT 1")
                return True
            finally:
                self._pool.putconn(conn)
        except Exception:
            return False

    @contextmanager
    def _get_cursor(self):
        """Yields a cursor from a pooled connection and ensures return."""
        if not self._pool:
            raise RuntimeError("Offline store connection pool is not initialized")
        
        conn = self._pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
        finally:
            self._pool.putconn(conn)

    def get_stop_features(
        self,
        stop_id: str,
        line_id: Optional[str] = None,
        hour_of_day: Optional[int] = None,
        day_of_week: Optional[int] = None,
    ) -> Optional[StopFeatures]:
        """
        Retrieves historical features from the dbt mart with contextual fallback.
        """
        query = """
            SELECT
                stop_id,
                line_id,
                hour_of_day,
                day_of_week,
                stop_lat,
                stop_lon,
                historical_avg_delay,
                historical_stddev_delay,
                avg_dwell_time_ms,
                historical_arrival_count
            FROM marts.fct_stop_arrivals
            WHERE stop_id = %s
            AND (%s IS NULL OR line_id = %s)
            ORDER BY
                (hour_of_day = %s AND day_of_week = %s) DESC,
                ABS(hour_of_day - %s) ASC
            LIMIT 1
        """
        try:
            with self._get_cursor() as cur:
                # Params passed twice: once for IS NULL check, once for ORDER BY logic
                params = (str(stop_id), line_id, line_id, hour_of_day, day_of_week, hour_of_day)
                cur.execute(query, params)
                row = cur.fetchone()

                if not row:
                    return None

                return StopFeatures(
                    stop_id=str(row["stop_id"]),
                    line_id=str(row.get("line_id", "UNKNOWN")),
                    hour_of_day=int(row["hour_of_day"]),
                    day_of_week=int(row["day_of_week"]),
                    latitude=float(row.get("stop_lat") or 0.0),
                    longitude=float(row.get("stop_lon") or 0.0),
                    historical_avg_delay=float(row.get("historical_avg_delay") or 0.0),
                    historical_stddev_delay=float(row.get("historical_stddev_delay") or 0.0),
                    avg_dwell_time_ms=float(row.get("avg_dwell_time_ms") or 0.0),
                    historical_arrival_count=int(row.get("historical_arrival_count") or 0),
                )
        except Exception as e:
            logger.error(f"PostgreSQL fetch failure: {e}")
            raise