import logging
from typing import Any, Dict, Optional, Tuple

from fastavro import validate as avro_validate
from fastavro.schema import parse_schema

from .client import SchemaRegistryClient, SchemaRegistryError

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validates messages against Schema Registry using audited Avro logic.
    """

    def __init__(self, registry_url: Optional[str] = None):
        self.client = SchemaRegistryClient(registry_url)
        # Using a parsed schema cache to improve performance
        self._parsed_schema_cache: Dict[str, Any] = {}

    def _get_parsed_schema(self, subject: str) -> Any:
        """Retrieves and parses schema, caching the parsed result."""
        if subject not in self._parsed_schema_cache:
            try:
                schema_info = self.client.get_latest_schema(subject)
                # parse_schema handles the internal Avro logic correctly
                self._parsed_schema_cache[subject] = parse_schema(schema_info.schema)
            except Exception as e:
                logger.error(f"Failed to resolve schema for subject {subject}: {str(e)}")
                raise SchemaRegistryError(f"Validation impossible: {subject} schema unavailable")

        return self._parsed_schema_cache[subject]

    def validate(self, subject: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validates message against the contract defined in the Registry.
        """
        try:
            schema = self._get_parsed_schema(subject)
            # fastavro.validate raises error if invalid, returns True if valid
            avro_validate(message, schema)
            return True, None
        except SchemaRegistryError as e:
            return False, str(e)
        except Exception as e:
            # This catches validation errors specifically
            error_msg = f"Contract violation on {subject}: {str(e)}"
            logger.warning(error_msg)
            return False, error_msg

    def clear_cache(self) -> None:
        self._parsed_schema_cache.clear()
        logger.info("Schema validator cache purged")
