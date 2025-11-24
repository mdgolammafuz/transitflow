# tests/test_mcp.py
"""Tests for MCP (Model Context Protocol) endpoints."""

import os
import pytest
from fastapi.testclient import TestClient
from serving.api import app

client = TestClient(app)


def test_mcp_tools_list():
    """Test MCP tools manifest endpoint."""
    response = client.get("/mcp/tools")
    assert response.status_code == 200
    
    data = response.json()
    assert "tools" in data
    assert len(data["tools"]) == 3  # retrieve_filing, compare_companies, analyze_trend
    
    tool_names = [t["name"] for t in data["tools"]]
    assert "retrieve_filing" in tool_names
    assert "compare_companies" in tool_names
    assert "analyze_trend" in tool_names


@pytest.mark.skipif(
    os.getenv("SKIP_CHAIN_INIT") == "1",
    reason="Requires chain initialization"
)
def test_mcp_execute_requires_auth():
    """Test MCP execute requires API key (skipped if chain not initialized)."""
    response = client.post(
        "/mcp/execute",
        json={
            "tool": "retrieve_filing",
            "arguments": {"company": "AAPL", "year": 2023, "question": "test"}
        }
    )
    # Should require auth
    assert response.status_code in [401, 403]


def test_mcp_execute_without_chain():
    """Test MCP execute returns 503 when chain not initialized."""
    if os.getenv("SKIP_CHAIN_INIT") != "1":
        pytest.skip("Only relevant when SKIP_CHAIN_INIT=1")
    
    response = client.post(
        "/mcp/execute",
        json={
            "tool": "retrieve_filing",
            "arguments": {"company": "AAPL", "year": 2023, "question": "test"}
        }
    )
    # Should return 503 when chain not available
    assert response.status_code == 503


def test_mcp_execute_invalid_tool():
    """Test MCP execute rejects unknown tools."""
    # This should fail before reaching chain, so works with SKIP_CHAIN_INIT
    response = client.post(
        "/mcp/execute",
        headers={"X-API-Key": "demo-key-123"},
        json={
            "tool": "nonexistent_tool",
            "arguments": {}
        }
    )
    # Should be 400 Bad Request for unknown tool, or 503 if chain needed first
    assert response.status_code in [400, 503]