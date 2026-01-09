"""
Schema Registry client for Redpanda/Confluent Schema Registry.

Handles:
- Schema registration
- Schema retrieval
- Compatibility checks
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)


@dataclass
class SchemaInfo:
    """Information about a registered schema."""
    subject: str
    version: int
    schema_id: int
    schema: Dict[str, Any]


class SchemaRegistryError(Exception):
    """Error from Schema Registry operations."""
    pass


class SchemaRegistryClient:
    """
    Client for interacting with Schema Registry.
    
    Usage:
        client = SchemaRegistryClient("http://localhost:8081")
        client.register_schema("my-topic-value", avro_schema)
        schema = client.get_latest_schema("my-topic-value")
    """
    
    def __init__(self, url: Optional[str] = None):
        """
        Initialize client.
        
        Args:
            url: Schema Registry URL. Defaults to SCHEMA_REGISTRY_URL env var.
        """
        self.url = url or os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        self.url = self.url.rstrip("/")
        logger.info(f"Schema Registry client initialized: {self.url}")
    
    def _request(self, path: str, method: str = "GET", data: Optional[Dict] = None) -> Dict:
        """Make HTTP request to Schema Registry."""
        url = f"{self.url}{path}"
        headers = {
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Accept": "application/vnd.schemaregistry.v1+json",
        }
        
        body = None
        if data:
            body = json.dumps(data).encode("utf-8")
        
        request = Request(url, data=body, headers=headers, method=method)
        
        try:
            with urlopen(request, timeout=10) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            error_body = e.read().decode("utf-8")
            logger.error(f"Schema Registry error: {e.code} - {error_body}")
            raise SchemaRegistryError(f"HTTP {e.code}: {error_body}")
        except URLError as e:
            logger.error(f"Cannot connect to Schema Registry: {e}")
            raise SchemaRegistryError(f"Connection failed: {e}")
    
    def list_subjects(self) -> List[str]:
        """List all registered subjects."""
        return self._request("/subjects")
    
    def register_schema(
        self, 
        subject: str, 
        schema: Dict[str, Any],
        schema_type: str = "AVRO"
    ) -> int:
        """
        Register a new schema version.
        
        Args:
            subject: Subject name (e.g., "topic-value")
            schema: Avro schema as dict
            schema_type: Schema type (AVRO, JSON, PROTOBUF)
        
        Returns:
            Schema ID
        """
        data = {
            "schema": json.dumps(schema),
            "schemaType": schema_type,
        }
        
        result = self._request(f"/subjects/{subject}/versions", method="POST", data=data)
        schema_id = result["id"]
        
        logger.info(f"Registered schema {subject} with ID {schema_id}")
        return schema_id
    
    def get_latest_schema(self, subject: str) -> SchemaInfo:
        """Get the latest schema version for a subject."""
        result = self._request(f"/subjects/{subject}/versions/latest")
        
        return SchemaInfo(
            subject=result["subject"],
            version=result["version"],
            schema_id=result["id"],
            schema=json.loads(result["schema"]),
        )
    
    def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        """Get schema by its global ID."""
        result = self._request(f"/schemas/ids/{schema_id}")
        return json.loads(result["schema"])
    
    def check_compatibility(
        self, 
        subject: str, 
        schema: Dict[str, Any]
    ) -> bool:
        """
        Check if a schema is compatible with the latest version.
        
        Args:
            subject: Subject name
            schema: Schema to check
        
        Returns:
            True if compatible
        """
        data = {"schema": json.dumps(schema)}
        
        try:
            result = self._request(
                f"/compatibility/subjects/{subject}/versions/latest",
                method="POST",
                data=data
            )
            return result.get("is_compatible", False)
        except SchemaRegistryError:
            return False
    
    def set_compatibility(self, subject: str, level: str = "BACKWARD") -> None:
        """
        Set compatibility level for a subject.
        
        Levels:
        - BACKWARD: New schema can read old data
        - FORWARD: Old schema can read new data
        - FULL: Both directions
        - NONE: No compatibility checking
        """
        valid_levels = ["BACKWARD", "FORWARD", "FULL", "NONE", 
                       "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE"]
        
        if level not in valid_levels:
            raise ValueError(f"Invalid compatibility level: {level}")
        
        data = {"compatibility": level}
        self._request(f"/config/{subject}", method="PUT", data=data)
        
        logger.info(f"Set compatibility for {subject} to {level}")
    
    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        """Delete a subject (soft delete by default)."""
        path = f"/subjects/{subject}"
        if permanent:
            path += "?permanent=true"
        
        return self._request(path, method="DELETE")
    
    def is_healthy(self) -> bool:
        """Check if Schema Registry is healthy."""
        try:
            self.list_subjects()
            return True
        except SchemaRegistryError:
            return False