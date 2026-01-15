import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaInfo:
    """Information about a registered schema (Immutable)."""

    subject: str
    version: int
    schema_id: int
    schema: Dict[str, Any]


class SchemaRegistryError(Exception):
    """Base exception for Schema Registry operations."""

    pass


class SchemaRegistryClient:
    """
    Hardened client for interacting with Schema Registry.
    """

    def __init__(self, url: Optional[str] = None):
        # Strict configuration: fail fast if no URL is provided/found
        self.url = url or os.environ.get("SCHEMA_REGISTRY_URL")
        if not self.url:
            raise SchemaRegistryError("SCHEMA_REGISTRY_URL environment variable is not set")

        self.url = self.url.rstrip("/")
        self._timeout = int(os.environ.get("SCHEMA_REGISTRY_TIMEOUT", "10"))

    def _request(self, path: str, method: str = "GET", data: Optional[Dict] = None) -> Dict:
        url = f"{self.url}{path}"
        headers = {
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Accept": "application/vnd.schemaregistry.v1+json",
        }

        body = json.dumps(data).encode("utf-8") if data else None
        request = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(request, timeout=self._timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            # Masking potentially sensitive error details in production logs
            status = e.code
            logger.error(f"Schema Registry HTTP Error: {status}")
            raise SchemaRegistryError(f"Registry request failed with status {status}")
        except URLError as e:
            logger.critical(f"Network connectivity issue with Schema Registry: {e.reason}")
            raise SchemaRegistryError("Failed to connect to Schema Registry")

    def register_schema(
        self, subject: str, schema: Dict[str, Any], schema_type: str = "AVRO"
    ) -> int:
        data = {"schema": json.dumps(schema), "schemaType": schema_type}
        result = self._request(f"/subjects/{subject}/versions", method="POST", data=data)
        return result["id"]

    def get_latest_schema(self, subject: str) -> SchemaInfo:
        result = self._request(f"/subjects/{subject}/versions/latest")
        return SchemaInfo(
            subject=result["subject"],
            version=result["version"],
            schema_id=result["id"],
            schema=json.loads(result["schema"]),
        )

    def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        result = self._request(f"/schemas/ids/{schema_id}")
        return json.loads(result["schema"])

    def is_healthy(self) -> bool:
        try:
            # Use a lightweight endpoint for health checks
            self._request("/subjects")
            return True
        except SchemaRegistryError:
            return False

    def list_subjects(self) -> List[str]:
        """Returns the list of registered subjects."""
        # The _request method already handles the JSON parsing
        return self._request("/subjects")

    def is_compatible(self, subject: str, schema: Dict[str, Any]) -> bool:
        """Checks if a schema is compatible with the latest version in the registry."""
        data = {"schema": json.dumps(schema)}
        try:
            result = self._request(
                f"/compatibility/subjects/{subject}/versions/latest", method="POST", data=data
            )
            return result.get("is_compatible", False)
        except SchemaRegistryError:
            # If the subject doesn't exist yet, it's technically compatible
            return True

    def set_compatibility(self, subject: str, level: str = "BACKWARD") -> bool:
        """Sets the compatibility level for a subject."""
        data = {"compatibility": level}
        # Path for subject-level config
        self._request(f"/config/{subject}", method="PUT", data=data)
        return True
