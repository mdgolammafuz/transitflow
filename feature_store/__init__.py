"""
Feature Store module.

Pattern: 
Semantic Interface
ML Reproducibility

Provides unified access to online (Redis) and offline (PostgreSQL) features
with training-serving consistency guarantees.
"""

from feature_store.config import FeatureStoreConfig
from feature_store.online_store import OnlineStore
from feature_store.offline_store import OfflineStore
from feature_store.feature_service import FeatureService

__all__ = [
    "FeatureStoreConfig",
    "OnlineStore",
    "OfflineStore",
    "FeatureService",
]