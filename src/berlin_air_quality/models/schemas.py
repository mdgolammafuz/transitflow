
"""
Core data schemas for Berlin Air Quality Predictor.

Defines the prediction target and feature schemas.
"""

from enum import Enum
from datetime import date as date_type
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class PM25Category(str, Enum):
    """
    PM2.5 air quality categories based on WHO guidelines.
    
    Thresholds (daily mean in µg/m³):
    - Good: ≤ 15
    - Moderate: 16-35
    - Unhealthy: > 35
    """
    
    GOOD = "Good"
    MODERATE = "Moderate"
    UNHEALTHY = "Unhealthy"
    
    @classmethod
    def from_value(cls, pm25_ugm3: float) -> "PM25Category":
        """
        Classify PM2.5 value into category.
        
        Args:
            pm25_ugm3: PM2.5 daily mean in µg/m³
            
        Returns:
            PM25Category
        """
        if pm25_ugm3 <= 15:
            return cls.GOOD
        elif pm25_ugm3 <= 35:
            return cls.MODERATE
        else:
            return cls.UNHEALTHY
    
    @property
    def advisory(self) -> str:
        """Get health advisory for this category."""
        advisories = {
            PM25Category.GOOD: "Safe for everyone",
            PM25Category.MODERATE: "Sensitive groups should limit prolonged outdoor exertion",
            PM25Category.UNHEALTHY: "Everyone should reduce outdoor activity"
        }
        return advisories[self]
    
    @property
    def numeric(self) -> int:
        """Get numeric encoding for ML models."""
        return {
            PM25Category.GOOD: 0,
            PM25Category.MODERATE: 1,
            PM25Category.UNHEALTHY: 2
        }[self]


class DailyFeatures(BaseModel):
    """
    Daily feature set for prediction.
    
    All features available at prediction time (evening before).
    """
    
    # Identifier
    target_date: date_type = Field(..., description="Prediction target date")
    feature_version: str = Field(default="v1", description="Feature schema version")
    
    # PM2.5 features (from today/yesterday)
    today_pm25_mean: float = Field(..., ge=0, description="Today's mean PM2.5 µg/m³")
    today_pm25_max: float = Field(..., ge=0, description="Today's max PM2.5 µg/m³")
    yesterday_pm25_mean: float = Field(..., ge=0, description="Yesterday's mean PM2.5 µg/m³")
    
    # Weather forecast features (for tomorrow)
    wind_speed_forecast_ms: float = Field(..., ge=0, description="Forecasted wind speed m/s")
    precipitation_forecast_mm: float = Field(..., ge=0, description="Forecasted precipitation mm")
    temperature_forecast_c: float = Field(..., description="Forecasted temperature °C")
    
    # Calendar features
    day_of_week: int = Field(..., ge=0, le=6, description="0=Monday, 6=Sunday")
    month: int = Field(..., ge=1, le=12, description="Month of year")
    
    # Target (only for training, None for inference)
    tomorrow_pm25_category: Optional[PM25Category] = Field(
        None, 
        description="Actual next-day category (training only)"
    )
    
    # Configuration (Pydantic V2 style)
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "target_date": "2024-12-20",
                "feature_version": "v1",
                "today_pm25_mean": 22.5,
                "today_pm25_max": 35.2,
                "yesterday_pm25_mean": 18.3,
                "wind_speed_forecast_ms": 4.5,
                "precipitation_forecast_mm": 0.0,
                "temperature_forecast_c": 3.2,
                "day_of_week": 4,
                "month": 12,
                "tomorrow_pm25_category": "Moderate"
            }
        }
    )
    
    def to_feature_vector(self) -> list[float]:
        """
        Convert to numeric feature vector for ML models.
        
        Returns:
            List of 8 float features
        """
        return [
            self.today_pm25_mean,
            self.today_pm25_max,
            self.yesterday_pm25_mean,
            self.wind_speed_forecast_ms,
            self.precipitation_forecast_mm,
            self.temperature_forecast_c,
            float(self.day_of_week),
            float(self.month)
        ]
    
    @staticmethod
    def feature_names() -> list[str]:
        """Get ordered list of feature names."""
        return [
            "today_pm25_mean",
            "today_pm25_max",
            "yesterday_pm25_mean",
            "wind_speed_forecast_ms",
            "precipitation_forecast_mm",
            "temperature_forecast_c",
            "day_of_week",
            "month"
        ]


class Prediction(BaseModel):
    """Prediction response model."""
    
    target_date: date_type = Field(..., description="Predicted date")
    category: PM25Category = Field(..., description="Predicted category")
    confidence: float = Field(..., ge=0, le=1, description="Prediction confidence")
    probabilities: dict[str, float] = Field(..., description="Per-class probabilities")
    reason: str = Field(..., description="Human-readable explanation")
    model_version: str = Field(..., description="Model version used")
    
    # Configuration (Pydantic V2 style)
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "target_date": "2024-12-21",
                "category": "Moderate",
                "confidence": 0.72,
                "probabilities": {
                    "Good": 0.18,
                    "Moderate": 0.72,
                    "Unhealthy": 0.10
                },
                "reason": "Low wind speed (2.5 m/s) will limit pollution dispersion",
                "model_version": "1.0.0"
            }
        }
    )
    