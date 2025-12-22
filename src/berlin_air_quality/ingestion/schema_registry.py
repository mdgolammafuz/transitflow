"""
Schema Registry client for managing Avro schemas.

Uses Redpanda's built-in Schema Registry (compatible with Confluent).
"""

import httpx
import json
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SchemaRegistryClient:
    """
    Client for Redpanda/Confluent Schema Registry.
    
    Handles schema registration, retrieval, and compatibility checks.
    
    Usage:
        client = SchemaRegistryClient()
        
        # Register a schema
        schema_id = await client.register_schema(
            subject="raw.berlin.pm25-value",
            schema_path="schemas/pm25_measurement.avsc"
        )
        
        # Get schema by ID
        schema = await client.get_schema(schema_id)
    """
    
    def __init__(
        self, 
        url: str = "http://localhost:8081",
        timeout: float = 10.0
    ):
        """
        Initialize Schema Registry client.
        
        Args:
            url: Schema Registry URL
            timeout: Request timeout in seconds
        """
        self.url = url.rstrip('/')
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
    
    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client
    
    async def register_schema(
        self, 
        subject: str, 
        schema_path: Optional[str] = None,
        schema_str: Optional[str] = None
    ) -> int:
        """
        Register a schema with the registry.
        
        Args:
            subject: Schema subject name (e.g., "raw.berlin.pm25-value")
            schema_path: Path to .avsc file
            schema_str: Schema as JSON string (alternative to path)
            
        Returns:
            Schema ID
        """
        if schema_path:
            schema_str = Path(schema_path).read_text()
        
        if not schema_str:
            raise ValueError("Either schema_path or schema_str required")
        
        # Validate JSON
        schema_dict = json.loads(schema_str)
        
        payload = {
            "schemaType": "AVRO",
            "schema": json.dumps(schema_dict)
        }
        
        response = await self.client.post(
            f"{self.url}/subjects/{subject}/versions",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        response.raise_for_status()
        
        result = response.json()
        schema_id = result["id"]
        logger.info(f"Registered schema '{subject}' with ID {schema_id}")
        return schema_id
    
    async def get_schema(self, schema_id: int) -> dict:
        """
        Get schema by ID.
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema as dictionary
        """
        response = await self.client.get(f"{self.url}/schemas/ids/{schema_id}")
        response.raise_for_status()
        
        result = response.json()
        return json.loads(result["schema"])
    
    async def get_latest_schema(self, subject: str) -> tuple[int, dict]:
        """
        Get latest schema version for a subject.
        
        Args:
            subject: Schema subject name
            
        Returns:
            Tuple of (schema_id, schema_dict)
        """
        response = await self.client.get(
            f"{self.url}/subjects/{subject}/versions/latest"
        )
        response.raise_for_status()
        
        result = response.json()
        return result["id"], json.loads(result["schema"])
    
    async def check_compatibility(
        self, 
        subject: str, 
        schema_str: str
    ) -> bool:
        """
        Check if a schema is compatible with the latest version.
        
        Args:
            subject: Schema subject name
            schema_str: New schema as JSON string
            
        Returns:
            True if compatible
        """
        payload = {
            "schemaType": "AVRO",
            "schema": schema_str
        }
        
        response = await self.client.post(
            f"{self.url}/compatibility/subjects/{subject}/versions/latest",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        
        if response.status_code == 404:
            # No existing schema, anything is compatible
            return True
        
        response.raise_for_status()
        result = response.json()
        return result.get("is_compatible", False)
    
    async def set_compatibility(
        self, 
        subject: str, 
        level: str = "BACKWARD"
    ) -> None:
        """
        Set compatibility level for a subject.
        
        Args:
            subject: Schema subject name
            level: Compatibility level (BACKWARD, FORWARD, FULL, NONE)
        """
        payload = {"compatibility": level}
        
        response = await self.client.put(
            f"{self.url}/config/{subject}",
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        response.raise_for_status()
        logger.info(f"Set compatibility for '{subject}' to {level}")
    
    async def list_subjects(self) -> list[str]:
        """List all registered subjects."""
        response = await self.client.get(f"{self.url}/subjects")
        response.raise_for_status()
        return response.json()
    
    async def delete_subject(self, subject: str, permanent: bool = False) -> None:
        """
        Delete a subject (for testing/cleanup).
        
        Args:
            subject: Subject to delete
            permanent: If True, permanently delete
        """
        response = await self.client.delete(
            f"{self.url}/subjects/{subject}",
            params={"permanent": str(permanent).lower()}
        )
        response.raise_for_status()
        logger.info(f"Deleted subject '{subject}'")


# =============================================================================
# Synchronous wrapper
# =============================================================================

def register_schemas_sync(
    schema_registry_url: str = "http://localhost:8081",
    schemas_dir: str = "schemas"
) -> dict[str, int]:
    """
    Register all schemas synchronously.
    
    Returns:
        Dict mapping subject names to schema IDs
    """
    import asyncio
    
    async def _register():
        results = {}
        schemas_path = Path(schemas_dir)
        
        async with SchemaRegistryClient(schema_registry_url) as client:
            # Define subject-to-file mapping
            schema_files = {
                "raw.berlin.pm25-value": "pm25_measurement.avsc",
                "raw.berlin.weather-value": "weather_observation.avsc",
                "dlq.ingestion-value": "dead_letter.avsc"
            }
            
            for subject, filename in schema_files.items():
                filepath = schemas_path / filename
                if filepath.exists():
                    # Set compatibility to BACKWARD
                    try:
                        await client.set_compatibility(subject, "BACKWARD")
                    except Exception:
                        pass  # Subject might not exist yet
                    
                    schema_id = await client.register_schema(
                        subject=subject,
                        schema_path=str(filepath)
                    )
                    results[subject] = schema_id
                else:
                    logger.warning(f"Schema file not found: {filepath}")
        
        return results
    
    return asyncio.run(_register())
