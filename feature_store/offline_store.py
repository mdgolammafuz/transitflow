"""
Offline Feature Store - PostgreSQL.
Hardened: Explicit schema search path and nearest-hour fallback.
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Optional

import psycopg2
from psycopg2 import DatabaseError, OperationalError
from psycopg2.extras import RealDictCursor

from feature_store.config import FeatureStoreConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StopFeatures:
    """Historical features for a stop at a specific time context."""

    stop_id: str
    line_id: str
    hour_of_day: int
    day_of_week: int
    latitude: float
    longitude: float
    historical_avg_delay: float
    historical_stddev_delay: float
    avg_dwell_time_seconds: float
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
            "avg_dwell_time_seconds": self.avg_dwell_time_seconds,
            "historical_arrival_count": self.historical_arrival_count,
        }


class OfflineStore:
    """PostgreSQL-based offline feature store."""

    def __init__(self, config: FeatureStoreConfig):
        self._config = config
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        try:
            self._conn = psycopg2.connect(
                host=self._config.postgres_host,
                port=self._config.postgres_port,
                user=self._config.postgres_user,
                password=self._config.postgres_password,
                dbname=self._config.postgres_db,
                connect_timeout=max(2, int(self._config.request_timeout_seconds)),
            )
            self._conn.autocommit = True

            with self._conn.cursor() as cur:
                cur.execute(f"SET search_path TO {self._config.postgres_schema}, public")

            logger.info(f"Connected to PostgreSQL. Schema: {self._config.postgres_schema}")
        except OperationalError as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def is_healthy(self) -> bool:
        if not self._conn or self._conn.closed:
            return False
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except DatabaseError:
            return False

    @contextmanager
    def _cursor(self):
        if not self._conn or self._conn.closed:
            raise RuntimeError("Offline store connection is closed")
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cur
        finally:
            cur.close()

    def get_stop_features(
        self, stop_id: str, hour_of_day: int, day_of_week: int
    ) -> Optional[StopFeatures]:
        """
        Retrieves historical features from the dbt mart.
        Matches stop_id and provides nearest-hour fallback for robustness.
        """
        # UPDATED QUERY: Included latitude and longitude
        query = """
            SELECT
                stop_id,
                line_id,
                hour_of_day,
                day_of_week,
                latitude,
                longitude,
                historical_avg_delay,
                historical_stddev_delay,
                avg_dwell_time_ms,
                historical_arrival_count
            FROM fct_stop_arrivals
            WHERE stop_id = %s
            ORDER BY
                (hour_of_day = %s AND day_of_week = %s) DESC,
                ABS(hour_of_day - %s) ASC
            LIMIT 1
        """
        try:
            with self._cursor() as cur:
                cur.execute(query, (str(stop_id), hour_of_day, day_of_week, hour_of_day))
                row = cur.fetchone()

                if not row:
                    logger.warning(f"No database record found for stop_id {stop_id}")
                    return None

                return StopFeatures(
                    stop_id=str(row["stop_id"]),
                    line_id=str(row.get("line_id", "UNKNOWN")),
                    hour_of_day=int(row["hour_of_day"]),
                    day_of_week=int(row["day_of_week"]),
                    latitude=float(row.get("latitude") or 0.0),
                    longitude=float(row.get("longitude") or 0.0),
                    historical_avg_delay=float(row.get("historical_avg_delay") or 0.0),
                    historical_stddev_delay=float(row.get("historical_stddev_delay") or 0.0),
                    avg_dwell_time_seconds=float((row.get("avg_dwell_time_ms") or 0.0) / 1000.0),
                    historical_arrival_count=int(row.get("historical_arrival_count") or 0),
                )
        except Exception as e:
            logger.error(f"PostgreSQL fetch crash: {e}")
            raise