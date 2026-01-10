"""
Unit tests for Schema Registry.
Consistent with fastavro implementation and strict security.
"""

import pytest
from unittest.mock import patch, MagicMock
from src.schema_registry.client import SchemaRegistryClient, SchemaRegistryError
from src.schema_registry.validator import SchemaValidator

class TestSchemaRegistryClient:
    """Tests for hardened SchemaRegistryClient."""

    def test_client_initialization_fails_without_url(self, monkeypatch):
        """Ensures security-first behavior: no URL = no client."""
        monkeypatch.delenv("SCHEMA_REGISTRY_URL", raising=False)
        with pytest.raises(SchemaRegistryError, match="environment variable is not set"):
            SchemaRegistryClient()

    def test_client_initialization_custom_url(self):
        client = SchemaRegistryClient("http://custom:9999")
        assert client.url == "http://custom:9999"

class TestSchemaValidator:
    """Tests for fastavro-based SchemaValidator."""

    @pytest.fixture
    def validator(self):
        # Initialize with a dummy URL to satisfy strict init
        return SchemaValidator("http://mock-registry:8081")

    def test_validate_logic_with_fastavro(self, validator):
        """
        Tests that validation correctly uses fastavro.
        We mock the internal parsed cache to avoid network calls.
        """
        from fastavro.schema import parse_schema
        
        raw_schema = {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "int"}]
        }
        
        # Consistent with our hardened validator internal naming
        validator._parsed_schema_cache["test-value"] = parse_schema(raw_schema)
        
        # Valid case
        is_valid, error = validator.validate("test-value", {"id": 123})
        assert is_valid is True
        
        # Invalid case (string instead of int)
        is_valid, error = validator.validate("test-value", {"id": "wrong"})
        assert is_valid is False
        assert "id" in error

    def test_cache_clear(self, validator):
        validator._parsed_schema_cache["test"] = {"dummy": "data"}
        validator.clear_cache()
        assert len(validator._parsed_schema_cache) == 0
