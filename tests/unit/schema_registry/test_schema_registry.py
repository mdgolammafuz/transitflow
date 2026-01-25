"""
Unit tests for Schema Registry.
Tests the fastavro integration without external dependencies.
"""

import pytest
from src.schema_registry.client import SchemaRegistryClient, SchemaRegistryError
from src.schema_registry.validator import SchemaValidator

class TestSchemaRegistryClient:
    """Tests for hardened SchemaRegistryClient."""

    def test_client_initialization_fails_without_url(self, monkeypatch):
        monkeypatch.delenv("SCHEMA_REGISTRY_URL", raising=False)
        with pytest.raises(SchemaRegistryError):
            SchemaRegistryClient()

    def test_client_initialization_custom_url(self):
        client = SchemaRegistryClient("http://custom:9999")
        assert client.url == "http://custom:9999"

class TestSchemaValidator:
    """Tests for fastavro-based SchemaValidator."""

    @pytest.fixture
    def validator(self):
        return SchemaValidator("http://mock-registry:8081")

    def test_validate_logic_with_fastavro(self, validator):
        """
        Tests that validation correctly uses fastavro.
        We mock the internal parsed cache to avoid network calls.
        """
        # We need to import fastavro to mock the parse result structure
        import fastavro

        raw_schema = {
            "type": "record", 
            "name": "Test", 
            "fields": [{"name": "id", "type": "int"}]
        }

        # Manually parse using fastavro to get the correct internal structure
        parsed_schema = fastavro.parse_schema(raw_schema)

        # Inject into cache
        validator._parsed_schema_cache["test-value"] = parsed_schema

        # 1. Valid case
        is_valid, error = validator.validate("test-value", {"id": 123})
        assert is_valid is True
        assert error is None

        # 2. Invalid case (String instead of Int)
        is_valid, error = validator.validate("test-value", {"id": "wrong"})
        assert is_valid is False
        assert error is not None

    def test_cache_clear(self, validator):
        validator._parsed_schema_cache["test"] = {"dummy": "data"}
        validator.clear_cache()
        assert len(validator._parsed_schema_cache) == 0