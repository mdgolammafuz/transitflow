"""
Unit tests for ML Pipeline.
Aligned with hardened MLConfig schema.
"""

import os
import pytest
import numpy as np
from unittest.mock import patch
from ml_pipeline.config import MLConfig
from ml_pipeline.training import DelayPredictor, train_model, TrainingResult

class TestMLConfig:
    def test_config_from_env_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            config = MLConfig.from_env()
            assert config.mlflow_tracking_uri == "http://localhost:5000"
            assert config.model_name == "delay-predictor"
            assert config.xgb_n_estimators == 100

    def test_config_default_factory(self):
        config = MLConfig.default()
        # Aligned with verified schema: 
        # [hour_of_day, day_of_week, latitude, longitude, 
        #  historical_avg_delay, avg_dwell_time_ms, sample_count]
        assert len(config.feature_columns) == 7
        assert config.target_column == "historical_avg_delay"
        assert config.test_size == 0.2

    def test_config_immutable(self):
        config = MLConfig.default()
        with pytest.raises(Exception):
            config.model_name = "new_name"


class TestDelayPredictor:
    def test_predictor_not_trained(self):
        config = MLConfig.default()
        predictor = DelayPredictor(config)
        assert predictor.is_trained is False
        with pytest.raises(RuntimeError, match="not trained"):
            predictor.predict(np.array([[1, 2, 3, 4, 5, 6, 7]]))

    def test_predictor_train(self):
        config = MLConfig.default()
        predictor = DelayPredictor(config)
        
        # Match feature count to config
        X = np.random.randn(100, len(config.feature_columns))
        y = np.random.randn(100) * 60

        result = predictor.train(X, y)
        assert predictor.is_trained is True
        assert result.training_samples == 80  # 100 * (1 - 0.2 test_size)

    def test_predictor_save_load(self, tmp_path):
        config = MLConfig.default()
        predictor = DelayPredictor(config)
        X = np.random.randn(100, len(config.feature_columns))
        y = np.random.randn(100) * 60
        predictor.train(X, y)

        model_path = tmp_path / "model.pkl"
        predictor.save(str(model_path))

        new_predictor = DelayPredictor(config)
        new_predictor.load(str(model_path))
        assert new_predictor.is_trained is True

    def test_predictor_feature_importance(self):
        config = MLConfig.default()
        predictor = DelayPredictor(config)
        X = np.random.randn(100, len(config.feature_columns))
        y = np.random.randn(100) * 60
        predictor.train(X, y)

        importance = predictor.get_feature_importance()
        assert len(importance) == len(config.feature_columns)