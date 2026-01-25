"""
Schema validator for runtime message validation.
Hardened: Uses 'fastavro' for robust logical type handling (timestamp-millis).
Pattern: DE#1 - Ingestion is a Contract
"""

import logging
from typing import Dict, Any, Optional, Tuple
# fastavro is required for industry-standard validation
import fastavro 
from .client import SchemaRegistryClient, SchemaRegistryError

logger = logging.getLogger(__name__)

class SchemaValidator:
    """
    Validates messages using fastavro.
    
    Usage:
        validator = SchemaValidator()
        is_valid, error = validator.validate("vehicle_position-value", message)
    """
    
    def __init__(self, registry_url: Optional[str] = None):
        self.client = SchemaRegistryClient(registry_url)
        self._parsed_schema_cache: Dict[str, Any] = {}
    
    def _get_parsed_schema(self, subject: str) -> Any:
        """Get and parse schema from cache or registry."""
        if subject not in self._parsed_schema_cache:
            try:
                # 1. Fetch raw schema from Registry
                schema_info = self.client.get_latest_schema(subject)
                raw_schema = schema_info.schema
                
                # 2. Parse with fastavro (validates the schema definition itself)
                parsed_schema = fastavro.parse_schema(raw_schema)
                self._parsed_schema_cache[subject] = parsed_schema
                logger.debug(f"Cached parsed schema for {subject}")
                
            except SchemaRegistryError as e:
                logger.error(f"Cannot get schema for {subject}: {e}")
                raise
            except Exception as e:
                logger.error(f"Invalid Avro schema for {subject}: {e}")
                raise
        
        return self._parsed_schema_cache[subject]
    
    def validate(
        self, 
        subject: str, 
        message: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a message against a schema using fastavro.
        """
        try:
            parsed_schema = self._get_parsed_schema(subject)
        except Exception as e:
            return False, f"Schema error: {e}"
        
        # fastavro.validate returns True/False.
        # It handles Logical Types (timestamps) automatically.
        try:
            is_valid = fastavro.validate(message, parsed_schema)
            if not is_valid:
                # If invalid, we use schemaless_writer to force fastavro 
                # to throw the specific field error for debugging
                try:
                    fastavro.schemaless_writer(None, parsed_schema, message)
                except Exception as validation_err:
                    return False, str(validation_err)
                return False, "Unknown validation error"
            
            return True, None
        except Exception as e:
            return False, str(e)

    def clear_cache(self) -> None:
        self._parsed_schema_cache.clear()
        logger.info("Schema cache cleared")