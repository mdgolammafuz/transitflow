"""
Configuration management using Pydantic Settings.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment."""
    
    # Project paths
    project_root: Path = Path(__file__).parent.parent.parent.parent
    data_dir: Path = project_root / "data"
    # Berlin coordinates
    berlin_lat: float = 52.52
    berlin_lon: float = 13.405
    
    # API timeouts
    api_timeout_seconds: float = 30.0
    
    # Data settings
    pm25_good_threshold: float = 15.0  # µg/m³
    pm25_moderate_threshold: float = 35.0  # µg/m³
    
    # Logging
    log_level: str = "INFO"
    
    class Config:
        env_prefix = "BAQ_"  # Berlin Air Quality prefix
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
