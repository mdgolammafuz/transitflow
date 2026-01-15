"""
ML Pipeline module.

Pattern: DE#8 - ML Reproducibility

Training pipeline for delay prediction using:
- Delta Lake time-travel for reproducible training data
- XGBoost for fast, interpretable predictions
- MLflow for experiment tracking and model registry
"""

from ml_pipeline.config import MLConfig
from ml_pipeline.registry import ModelRegistry
from ml_pipeline.training import DelayPredictor, TrainingResult, train_model

__all__ = [
    "MLConfig",
    "DelayPredictor",
    "train_model",
    "TrainingResult",
    "ModelRegistry",
]
