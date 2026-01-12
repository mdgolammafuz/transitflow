"""
Offline Feature Store - PostgreSQL.

Pattern: 
Semantic Interface
ML Reproducibility

Historical aggregated features from dbt marts used to provide context 
to real-time predictions.
"""

import logging
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import OperationalError, DatabaseError

from feature_store.config import FeatureStoreConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StopFeatures:
    """Historical features for a stop at a specific time context."""
    stop_id: int
    line_id: str
    hour_of_day: int
    day_of_week: int
    avg_delay_seconds: float
    stddev_delay_seconds: float
    avg_dwell_time_seconds: float
    p90_delay_seconds: float
    sample_count: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "stop_id": self.stop_id,
            "line_id": self.line_id,
            "hour_of_day": self.hour_of_day,
            "day_of_week": self.day_of_week,
            "avg_delay_seconds": self.avg_delay_seconds,
            "stddev_delay_seconds": self.stddev_delay_seconds,
            "avg_dwell_time_seconds": self.avg_dwell_time_seconds,
            "p90_delay_seconds": self.p90_delay_seconds,
            "sample_count": self.sample_count,
        }


class OfflineStore:
    """PostgreSQL-based offline feature store."""

    def __init__(self, config: FeatureStoreConfig):
        self._config = config
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        """Establish PostgreSQL connection with timeout."""
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
            logger.info("Connected to PostgreSQL (Offline Store)")
        except OperationalError as e:
            logger.error("Failed to connect to PostgreSQL: %s", e)
            raise

    def close(self) -> None:
        """Close PostgreSQL connection safely."""
        if self._conn and not self._conn.closed:
            self._conn.close()

    def is_healthy(self) -> bool:
        """Check if PostgreSQL connection is alive."""
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
        """Context manager for database cursor using RealDictCursor."""
        if not self._conn or self._conn.closed:
            raise RuntimeError("Offline store connection is closed")
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cur
        finally:
            cur.close()

    def get_stop_features(
        self,
        stop_id: int,
        hour_of_day: int,
        day_of_week: int,
    ) -> Optional[StopFeatures]:
        """Fetch the most recent historical aggregates for a stop context."""
        query = """
            SELECT 
                stop_id, hour_of_day, day_of_week,
                avg_delay_seconds, stddev_delay_seconds,
                avg_dwell_time_seconds, p90_delay_seconds,
                sample_count
            FROM features.stop_historical
            WHERE stop_id = %s AND hour_of_day = %s AND day_of_week = %s
            ORDER BY computed_date DESC
            LIMIT 1
        """
        try:
            with self._cursor() as cur:
                cur.execute(query, (stop_id, hour_of_day, day_of_week))
                row = cur.fetchone()
                if not row:
                    return None
                
                return StopFeatures(
                    stop_id=row["stop_id"],
                    line_id="", # Line ID usually comes from the Online record
                    hour_of_day=row["hour_of_day"],
                    day_of_week=row["day_of_week"],
                    avg_delay_seconds=float(row["avg_delay_seconds"]),
                    stddev_delay_seconds=float(row["stddev_delay_seconds"]),
                    avg_dwell_time_seconds=float(row["avg_dwell_time_seconds"]),
                    p90_delay_seconds=float(row["p90_delay_seconds"]),
                    sample_count=int(row["sample_count"])
                )
        except DatabaseError as e:
            logger.error("PostgreSQL fetch error for stop %d: %s", stop_id, e)
            raise

    def get_line_average_delay(self) -> Optional[float]:
        """Fallback: Get global line average delay for the last week."""
        query = """
            SELECT AVG(avg_delay_seconds) as avg_delay
            FROM features.stop_historical
            WHERE computed_date >= CURRENT_DATE - INTERVAL '7 days'
        """
        try:
            with self._cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return float(row["avg_delay"]) if row and row["avg_delay"] else None
        except DatabaseError:
            return None