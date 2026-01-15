"""
ML Training Pipeline.

Pattern: DE#8 - ML Reproducibility
"""

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import structlog
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from ml_pipeline.config import MLConfig
from ml_pipeline.registry import ModelRegistry

logger = structlog.get_logger()

try:
    import xgboost as xgb

    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    logger.warning("xgboost_not_installed", action="using_mock_predictor")


@dataclass
class TrainingResult:
    """Detailed results of a training run."""

    model_version: str
    train_mae: float
    test_mae: float
    train_rmse: float
    test_rmse: float
    r2_score: float
    feature_importance: Dict[str, float]
    training_samples: int
    test_samples: int
    training_duration_seconds: float
    delta_version: Optional[int] = None


class MockPredictor:
    """Fallback predictor for environments without XGBoost."""

    def __init__(self):
        self._mean = 0.0
        self.is_trained = True

    def fit(self, X: np.ndarray, y: np.ndarray):
        self._mean = float(np.mean(y)) if len(y) > 0 else 0.0

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.full(len(X), self._mean)


class DelayPredictor:
    """Wrapper for XGBoost model with training and inference logic."""

    def __init__(self, config: MLConfig):
        self._config = config
        self._model: Optional[Any] = None
        self._feature_columns: List[str] = config.feature_columns
        self._registry = ModelRegistry(config)
        self._trained_at: Optional[datetime] = None

    @property
    def is_trained(self) -> bool:
        if XGB_AVAILABLE and self._model:
            try:
                self._model.get_booster()
                return True
            except Exception:
                return False
        return self._model is not None

    def _prepare_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
        """Validates columns and filters features based on what is available in the DF."""
        target = self._config.target_column

        if target not in df.columns:
            logger.error("target_missing", target=target, available=df.columns.tolist())
            raise ValueError(f"Target column {target} missing from data")

        available_features = [col for col in self._feature_columns if col in df.columns]

        if not available_features:
            logger.error(
                "no_features_available",
                expected=self._feature_columns,
                available=df.columns.tolist(),
            )
            raise ValueError("None of the configured feature columns were found in the dataset")

        X = df[available_features].copy()
        for col in X.columns:
            X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)

        y = pd.to_numeric(df[target], errors="coerce").fillna(0)

        return X, y, available_features

    def train(self, df: pd.DataFrame) -> TrainingResult:
        """Train the model using a Pandas DataFrame."""
        start_time = time.time()

        X_df, y_series, actual_features = self._prepare_data(df)
        self._feature_columns = actual_features

        X = X_df.values
        y = y_series.values

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=self._config.test_size,
            random_state=self._config.random_state,
        )

        if XGB_AVAILABLE:
            params = {
                "n_estimators": self._config.xgb_n_estimators,
                "max_depth": self._config.xgb_max_depth,
                "learning_rate": self._config.xgb_learning_rate,
                "random_state": self._config.random_state,
                "n_jobs": -1,
            }
            self._model = xgb.XGBRegressor(**params)
            self._model.fit(X_train, y_train)

            train_pred = self._model.predict(X_train)
            test_pred = self._model.predict(X_test)

            importance = dict(
                zip(
                    actual_features,
                    self._model.feature_importances_.tolist(),
                )
            )
        else:
            self._model = MockPredictor()
            self._model.fit(X_train, y_train)
            train_pred = self._model.predict(X_train)
            test_pred = self._model.predict(X_test)
            params = {"model": "mock"}
            importance = {col: 0.0 for col in actual_features}

        self._trained_at = datetime.utcnow()

        result = TrainingResult(
            model_version=self._trained_at.strftime("%Y%m%d_%H%M%S"),
            train_mae=float(mean_absolute_error(y_train, train_pred)),
            test_mae=float(mean_absolute_error(y_test, test_pred)),
            train_rmse=float(np.sqrt(mean_squared_error(y_train, train_pred))),
            test_rmse=float(np.sqrt(mean_squared_error(y_test, test_pred))),
            r2_score=float(r2_score(y_test, test_pred)),
            feature_importance=importance,
            training_samples=len(X_train),
            test_samples=len(X_test),
            training_duration_seconds=time.time() - start_time,
        )

        metrics = {
            "test_mae": result.test_mae,
            "test_rmse": result.test_rmse,
            "r2_score": result.r2_score,
        }

        self._registry.log_model(
            model=self._model,
            metrics=metrics,
            params=params if XGB_AVAILABLE else {"model_type": "mock"},
            X_sample=X_train[:5],
            y_sample=train_pred[:5],
        )

        return result


def train_model(
    config: MLConfig, df: pd.DataFrame, delta_version: int = 0
) -> Tuple[DelayPredictor, TrainingResult]:
    predictor = DelayPredictor(config)
    result = predictor.train(df)
    result.delta_version = delta_version
    return predictor, result
