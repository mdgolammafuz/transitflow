"""
Shadow Deployment Engine.
Safe Deployment
Runs a candidate model asynchronously alongside production.
"""

import asyncio
import logging
import time
import numpy as np
from typing import Any, Optional, Dict
from dataclasses import dataclass

# Project Imports
from monitoring.metrics import (
    SHADOW_PREDICTION_DIFF,
    SHADOW_AGREEMENT_RATE,
    PREDICTION_COUNT
)
from ml_pipeline.config import MLConfig

logger = logging.getLogger(__name__)

@dataclass
class Comparison:
    prod: float
    shadow: float
    diff: float
    agreement: bool

class ShadowPredictor:
    """
    Wraps Production and Shadow models.
    Returns Production result immediately; logs Shadow result asynchronously.
    """
    
    def __init__(self, prod_model: Any, shadow_model: Optional[Any], agreement_threshold: float = 60.0):
        self.prod_model = prod_model
        self.shadow_model = shadow_model
        self.threshold = agreement_threshold
        self.enabled = shadow_model is not None
        
        # Load feature schema from config to ensure shape alignment
        self.features = MLConfig.from_env().feature_columns
        
        # Internal stats
        self._total = 0
        self._agreed = 0

    def _prepare_vector(self, feature_map: Dict[str, Any]) -> np.ndarray:
        """Convert dict to array in correct order defined by MLConfig."""
        return np.array([[feature_map.get(f, 0.0) for f in self.features]])

    def _predict_single(self, model: Any, X: np.ndarray) -> float:
        """Helper to run a single model safely."""
        try:
            # We assume the model has a .predict() method
            # It might return a Float (scalar) OR a List/Array
            pred = model.predict(X)
            # If it's a list/array, extract the item.
            # If it's already a float/int, use it directly.
            if isinstance(pred, (list, np.ndarray)):
                if len(pred) > 0:
                    return float(pred[0])
                return 0.0
            return float(pred)     
        except Exception as e:
            logger.error(f"Shadow prediction failed: {e}")
            return 0.0
        
    async def predict(self, feature_map: Dict[str, Any], vehicle_id: str) -> float:
        """
        Main entry point.
        Returns: Production Prediction (float)
        """
        X = self._prepare_vector(feature_map)
        
        # 1. Critical Path: Production Prediction
        prod_val = self._predict_single(self.prod_model, X)

        # 2. Async Path: Shadow Prediction (Fire & Forget)
        if self.enabled:
            asyncio.create_task(self._compare_async(X, prod_val, vehicle_id))
            
        return prod_val

    async def _compare_async(self, X: np.ndarray, prod_val: float, vehicle_id: str):
        """Run shadow model and record metrics."""
        try:
            loop = asyncio.get_running_loop()
            # Run CPU-bound model inference in thread pool
            shadow_val = await loop.run_in_executor(None, self._predict_single, self.shadow_model, X)
            
            diff = abs(prod_val - shadow_val)
            is_agreed = diff <= self.threshold
            
            # Metrics
            SHADOW_PREDICTION_DIFF.labels(feature="delay").observe(diff)
            
            # Update internal agreement rate
            self._total += 1
            if is_agreed: 
                self._agreed += 1
            
            # Update Gauge every 10 requests to reduce noise
            if self._total % 10 == 0:
                SHADOW_AGREEMENT_RATE.set(self._agreed / self._total)

            if not is_agreed and diff > (self.threshold * 2):
                logger.warning(f"Shadow Divergence | Veh: {vehicle_id} | Prod: {prod_val:.1f} | Shadow: {shadow_val:.1f}")

        except Exception as e:
            logger.error(f"Shadow Inference Failed: {e}")
            PREDICTION_COUNT.labels(model_version="shadow", status="error").inc()

    def predict_sync_with_shadow(self, feature_map: Dict[str, Any], vehicle_id: str):
        """Synchronous helper for Unit Tests."""
        X = self._prepare_vector(feature_map)
        prod = self._predict_single(self.prod_model, X)
        
        comp = None
        if self.enabled:
            shadow = self._predict_single(self.shadow_model, X)
            diff = abs(prod - shadow)
            comp = Comparison(prod, shadow, diff, diff <= self.threshold)
            
        return prod, comp