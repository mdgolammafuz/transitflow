from typing import Any, Dict
from dotenv import load_dotenv
from pathlib import Path
import pytest

@pytest.fixture
def sample_enriched_event() -> Dict[str, Any]:
    """Sample enriched event aligned with 2026 Boolean schema."""
    return {
        "vehicle_id": 1362,
        "timestamp": "2026-01-07T10:15:30.000Z",
        "event_time_ms": 1767810000000,
        "latitude": 60.1699,
        "longitude": 24.9384,
        "speed_ms": 8.33,
        "heading": 245,
        "delay_seconds": 45,
        "door_status": False,  # Strict Boolean
        "line_id": "600",
        "direction_id": 1,
        "operator_id": 22,
        "next_stop_id": 1234567,
        "delay_trend": 5.0,
        "speed_trend": -0.5,
        "is_stopped": False,  # Strict Boolean
        "processing_time": 1767810000500,
    }


@pytest.fixture
def env_config(monkeypatch):
    """Matches your .env exactly to ensure config tests pass."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.setenv("POSTGRES_USER", "transit")
    monkeypatch.setenv("POSTGRES_PASSWORD", "transit_secure_local")
    monkeypatch.setenv("POSTGRES_DB", "transit")
    monkeypatch.setenv("POSTGRES_HOST", "postgres")


@pytest.fixture(scope="session", autouse=True)
def load_env():
    """
    Automated Industry Practice: 
    Load infra/.env into the process environment before any tests run.
    """
    env_path = Path(__file__).parent.parent / "infra" / "local" / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        pytest.fail(f"CRITICAL: .env file not found at {env_path}")
