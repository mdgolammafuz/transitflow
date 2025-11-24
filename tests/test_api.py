# tests/test_api.py
"""Minimal API tests for core endpoints."""

import os
import pytest
from fastapi.testclient import TestClient
from serving.api import app

client = TestClient(app)


def test_healthz():
    """Test health check endpoint."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_root_endpoint():
    """Test root endpoint returns service info."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "service" in data
    assert data["service"] == "IntelSent"


@pytest.mark.skipif(
    os.getenv("SKIP_CHAIN_INIT") == "1",
    reason="Requires chain initialization"
)
def test_query_requires_auth():
    """Test that query endpoint requires API key (skipped if chain not initialized)."""
    response = client.post(
        "/query",
        json={"text": "test", "company": "AAPL", "year": 2023}
    )
    # Should be 401 or 403 without API key
    assert response.status_code in [401, 403]


def test_query_without_chain():
    """Test query endpoint returns 503 when chain not initialized."""
    if os.getenv("SKIP_CHAIN_INIT") != "1":
        pytest.skip("Only relevant when SKIP_CHAIN_INIT=1")
    
    response = client.post(
        "/query",
        json={"text": "test", "company": "AAPL", "year": 2023}
    )
    # Should return 503 Service Unavailable when chain is not initialized
    assert response.status_code == 503
    assert "not initialized" in response.json()["detail"].lower()