"""
ML Pipeline configuration.
Hardened for Phase 6 consistency with verified Postgres and Delta Lake schemas.
"""

import os
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class MLConfig:
    """Configuration for ML training and serving."""

    mlflow_tracking_uri: str
    mlflow_experiment_name: str
    model_name: str

    delta_lake_path: str
    training_table: str
    
    postgres_host: str
    postgres_db: str

    feature_columns: List[str]
    target_column: str

    test_size: float
    random_state: int

    xgb_n_estimators: int
    xgb_max_depth: int
    xgb_learning_rate: float

    model_cache_ttl_seconds: int

    @classmethod
    def from_env(cls) -> "MLConfig":
        """Load configuration from environment variables."""
        return cls(
            mlflow_tracking_uri=os.environ.get(
                "MLFLOW_TRACKING_URI", "http://localhost:5000"
            ),
            mlflow_experiment_name=os.environ.get(
                "MLFLOW_EXPERIMENT_NAME", "transitflow-delay-prediction"
            ),
            model_name=os.environ.get("MODEL_NAME", "delay-predictor"),
            delta_lake_path=os.environ.get(
                "DELTA_LAKE_PATH", "s3a://transitflow-lakehouse"
            ),
            # Path matches your spark/gold_aggregation.py output
            training_table=os.environ.get("TRAINING_TABLE", "gold/stop_performance"),
            postgres_host=os.environ.get("POSTGRES_HOST", "localhost"),
            postgres_db=os.environ.get("POSTGRES_DB", "transit"),
            # Optimized feature set: Only columns confirmed in dbt/Spark
            feature_columns=os.environ.get(
                "FEATURE_COLUMNS",
                "hour_of_day,day_of_week,latitude,longitude,"
                "historical_avg_delay,avg_dwell_time_ms,sample_count"
            ).split(","),
            target_column=os.environ.get("TARGET_COLUMN", "historical_avg_delay"),
            test_size=float(os.environ.get("TEST_SIZE", "0.2")),
            random_state=int(os.environ.get("RANDOM_STATE", "42")),
            xgb_n_estimators=int(os.environ.get("XGB_N_ESTIMATORS", "100")),
            xgb_max_depth=int(os.environ.get("XGB_MAX_DEPTH", "6")),
            xgb_learning_rate=float(os.environ.get("XGB_LEARNING_RATE", "0.1")),
            model_cache_ttl_seconds=int(
                os.environ.get("MODEL_CACHE_TTL_SECONDS", "300")
            ),
        )

    @classmethod
    def default(cls) -> "MLConfig":
        """Default configuration for local development."""
        return cls.from_env()