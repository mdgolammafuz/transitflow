"""
Unit Tests for MLOps Module
Aligned with: mlops/validation.py, mlops/promotion.py, mlops/retraining.py
"""

import pytest
import numpy as np
from unittest.mock import Mock, patch
from ml_pipeline.config import MLConfig

@pytest.fixture(autouse=True)
def mock_config():
    """Global config mock to prevent env var dependency."""
    with patch("ml_pipeline.config.MLConfig.from_env") as mock:
        mock.return_value = Mock(
            feature_columns=["speed", "delay"], 
            model_name="test-model"
        )
        yield mock

class TestValidation:
    """Tests for mlops/validation.py"""

    def test_validate_metrics_pass(self):
        # FIXED: Patching the client where it is imported in the module
        with patch("mlops.validation.MlflowClient") as mock_client_cls:
            mock_run = Mock()
            mock_run.data.metrics = {"r2_score": 0.80, "test_mae": 100.0}
            
            # Configure the instance returned by the class
            mock_client_instance = mock_client_cls.return_value
            mock_client_instance.get_run.return_value = mock_run

            from mlops.validation import validate_metrics
            result = validate_metrics("run_123", min_r2=0.75, max_mae=120)
            
            assert result["passed"] is True
            assert result["details"]["r2_score"]["passed"] is True

    def test_validate_metrics_fail(self):
        with patch("mlops.validation.MlflowClient") as mock_client_cls:
            mock_run = Mock()
            mock_run.data.metrics = {"r2_score": 0.60, "test_mae": 100.0}
            
            mock_client_instance = mock_client_cls.return_value
            mock_client_instance.get_run.return_value = mock_run

            from mlops.validation import validate_metrics
            result = validate_metrics("run_123", min_r2=0.75, max_mae=120)
            
            assert result["passed"] is False

    def test_validate_latency(self):
        mock_model = Mock()
        mock_model.predict.return_value = np.array([0])
        
        with patch("mlflow.sklearn.load_model", return_value=mock_model):
            from mlops.validation import validate_latency
            result = validate_latency("run_123", max_latency_ms=1000)
            assert result["passed"] is True


class TestPromotion:
    """Tests for mlops/promotion.py"""

    def test_promote_to_stage_success(self):
        with patch("mlops.promotion.MlflowClient") as mock_client_cls:
            mock_client_instance = mock_client_cls.return_value
            
            # Setup mocks
            mock_client_instance.get_registered_model.return_value = True
            
            mock_version = Mock()
            mock_version.version = "5"
            mock_client_instance.create_model_version.return_value = mock_version
            
            from mlops.promotion import promote_to_stage
            result = promote_to_stage("run_123", "Staging")
            
            assert result["success"] is True
            assert result["version"] == "5"
            mock_client_instance.transition_model_version_stage.assert_called()

    def test_rollback_success(self):
        with patch("mlops.promotion.MlflowClient") as mock_client_cls:
            mock_client_instance = mock_client_cls.return_value
            
            # Mock successful retrieval
            mock_client_instance.get_model_version.return_value = Mock(version="2")
            mock_client_instance.get_latest_versions.return_value = [Mock(version="3")]
            
            from mlops.promotion import rollback_to_version
            result = rollback_to_version("test-model", "2")
            
            assert result["success"] is True
            assert result["rolled_back_to"] == "2"
            
            # Verify transition call
            mock_client_instance.transition_model_version_stage.assert_any_call(
                name="test-model", version="3", stage="Archived"
            )


class TestRetraining:
    """Tests for mlops/retraining.py"""

    def test_drift_decision_retrain(self):
        from mlops.retraining import check_drift_and_decide
        
        metrics = {"speed": 0.25, "delay": 0.05}
        result = check_drift_and_decide(metrics, threshold=0.2, dry_run=False)
        
        assert result.decision == "retrain"
        assert result.action == "trigger_pipeline"

    def test_drift_decision_skip(self):
        from mlops.retraining import check_drift_and_decide
        
        metrics = {"speed": 0.10, "delay": 0.05}
        result = check_drift_and_decide(metrics, threshold=0.2, dry_run=False)
        
        assert result.decision == "skip"
        assert result.action == "log_only"