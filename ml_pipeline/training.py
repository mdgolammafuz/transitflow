import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from ml_pipeline.config import MLConfig
from ml_pipeline.registry import ModelRegistry

logger = logging.getLogger(__name__)

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    logger.warning("XGBoost not installed, using mock predictor")

@dataclass
class TrainingResult:
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

class DelayPredictor:
    def __init__(self, config: MLConfig):
        self._config = config
        self._model: Optional[Any] = None
        self._feature_columns: List[str] = config.feature_columns
        self._registry = ModelRegistry(config)
        self._trained_at: Optional[datetime] = None

    def train(self, df: pd.DataFrame) -> TrainingResult:
        """
        Train the model using a Pandas DataFrame.
        Ensures Target is separated from Features.
        """
        start_time = time.time()

        # 1. Separate Features and Target
        X = df[self._feature_columns].values
        y = df[self._config.target_column].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=self._config.test_size,
            random_state=self._config.random_state,
        )

        if XGB_AVAILABLE:
            # Hyperparameters pulled from our hardened config.py
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
            
            importance = dict(zip(
                self._feature_columns,
                self._model.feature_importances_.tolist(),
            ))
        else:
            # Fallback for dev environments without XGB binaries
            self._model = MockPredictor()
            self._model.fit(X_train, y_train)
            train_pred = self._model.predict(X_train)
            test_pred = self._model.predict(X_test)
            params = {"model": "mock"}
            importance = {col: 0.0 for col in self._feature_columns}

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

        # 2. Log to MLflow Registry
        metrics = {
            "test_mae": result.test_mae,
            "test_rmse": result.test_rmse,
            "r2_score": result.r2_score
        }
        self._registry.log_model(self._model, metrics, params)

        return result

class MockPredictor:
    def __init__(self):
        self._mean = 0.0
    def fit(self, X, y):
        self._mean = float(np.mean(y)) if len(y) > 0 else 0.0
    def predict(self, X):
        return np.full(len(X), self._mean)