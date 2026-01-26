"""
Unit tests for ML Pipeline.
Hardened: Matches Phase 6 verified Pandas implementation.
Includes network mocking to prevent MLflow connection retries.
"""

import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from ml_pipeline.config import MLConfig
from ml_pipeline.training import DelayPredictor, TrainingResult


class TestMLConfig:
    def test_config_from_env_defaults(self):
        """Verify defaults match the Phase 5 schema alignment."""
        with patch.dict(os.environ, {}, clear=True):
            config = MLConfig.from_env()
            # In Docker, we default to the service name 'mlflow'
            assert config.mlflow_tracking_uri == "http://mlflow:5000"
            assert config.model_name == "delay-predictor"
            # We updated the default features list to match dbt Gold Marts
            assert "historical_avg_delay" in config.feature_columns
            assert "avg_dwell_time_ms" in config.feature_columns

    def test_target_column_alignment(self):
        """Ensure the target column is set correctly."""
        config = MLConfig.default()
        assert config.target_column == "historical_avg_delay"


class TestDelayPredictor:
    
    @pytest.fixture(autouse=True)
    def mock_mlflow(self):
        """
        CRITICAL FIX: Globally mock mlflow for all tests in this class.
        This prevents the code from trying to connect to 'http://mock:5000'
        and hanging with connection retries.
        """
        with patch("ml_pipeline.registry.mlflow") as mock:
            yield mock

    @pytest.fixture
    def mock_config(self):
        """Create a config with a small feature set for testing."""
        return MLConfig(
            mlflow_tracking_uri="http://mock:5000",
            mlflow_experiment_name="test_exp",
            model_name="test_model",
            delta_lake_path="s3a://bucket",
            training_table="gold/test",
            postgres_host="localhost",
            postgres_db="test",
            feature_columns=["f1", "f2"],
            target_column="target",
            test_size=0.2,
            random_state=42,
            xgb_n_estimators=10,
            xgb_max_depth=2,
            xgb_learning_rate=0.1,
            model_cache_ttl_seconds=60,
        )

    @pytest.fixture
    def valid_data(self):
        """Create a valid Pandas DataFrame matching the config."""
        return pd.DataFrame({
            "f1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "f2": [0.1, 0.2, 0.3, 0.4, 0.5],
            "target": [10, 20, 30, 40, 50],
            "irrelevant_col": ["a", "b", "c", "d", "e"]
        })

    def test_predictor_initialization(self, mock_config):
        predictor = DelayPredictor(mock_config)
        assert predictor.is_trained is False

    @patch("ml_pipeline.training.ModelRegistry")
    def test_train_success_path(self, mock_registry_cls, mock_config, valid_data):
        """Verify the happy path with valid Pandas data."""
        # Setup Mocks
        mock_registry = mock_registry_cls.return_value
        
        predictor = DelayPredictor(mock_config)
        result = predictor.train(valid_data)

        # Assertions
        assert isinstance(result, TrainingResult)
        assert predictor.is_trained is True
        assert result.training_samples == 4  # 5 rows * 0.8 split
        
        # Ensure registry was called to log the model
        mock_registry.log_model.assert_called_once()

    def test_train_fail_fast_on_corrupt_data(self, mock_config):
        """
        ROBUSTNESS CHECK: Ensure the pipeline CRASHES on bad types.
        We explicitly do NOT want silent failures here.
        """
        predictor = DelayPredictor(mock_config)
        
        # Data with a string "error" in a numeric feature column
        bad_data = pd.DataFrame({
            "f1": [1.0, "error", 3.0], 
            "f2": [0.1, 0.2, 0.3],
            "target": [10, 20, 30]
        })

        with pytest.raises(ValueError) as excinfo:
            predictor.train(bad_data)
        
        # Verify it's our specific error message
        assert "contains non-numeric data" in str(excinfo.value)

    def test_train_missing_target(self, mock_config):
        predictor = DelayPredictor(mock_config)
        df = pd.DataFrame({"f1": [1], "f2": [2]}) # No target column

        with pytest.raises(ValueError, match="Target column.*missing"):
            predictor.train(df)

    def test_train_missing_features(self, mock_config):
        predictor = DelayPredictor(mock_config)
        # DataFrame has target but NONE of the configured features
        df = pd.DataFrame({"target": [1], "other": [2]})

        with pytest.raises(ValueError, match="None of the configured feature columns"):
            predictor.train(df)