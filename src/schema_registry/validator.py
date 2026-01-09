"""
Schema validator for runtime message validation.

Pattern: Ingestion is a Contract
- Validates messages against registered schemas
- Rejects incompatible messages to DLQ
"""

import logging
from typing import Dict, Any, Optional, Tuple

from .client import SchemaRegistryClient, SchemaRegistryError

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validates messages against Schema Registry schemas.
    
    Usage:
        validator = SchemaValidator()
        is_valid, error = validator.validate("vehicle_position-value", message)
        if not is_valid:
            send_to_dlq(message, error)
    """
    
    def __init__(self, registry_url: Optional[str] = None):
        """Initialize validator with Schema Registry client."""
        self.client = SchemaRegistryClient(registry_url)
        self._schema_cache: Dict[str, Dict[str, Any]] = {}
    
    def _get_schema(self, subject: str) -> Dict[str, Any]:
        """Get schema from cache or registry."""
        if subject not in self._schema_cache:
            try:
                schema_info = self.client.get_latest_schema(subject)
                self._schema_cache[subject] = schema_info.schema
                logger.debug(f"Cached schema for {subject}")
            except SchemaRegistryError as e:
                logger.error(f"Cannot get schema for {subject}: {e}")
                raise
        
        return self._schema_cache[subject]
    
    def validate(
        self, 
        subject: str, 
        message: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a message against a schema.
        
        Args:
            subject: Schema Registry subject name
            message: Message to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            schema = self._get_schema(subject)
        except SchemaRegistryError as e:
            return False, f"Schema not found: {e}"
        
        # Validate required fields
        errors = self._validate_fields(schema, message)
        
        if errors:
            return False, "; ".join(errors)
        
        return True, None
    
    def _validate_fields(
        self, 
        schema: Dict[str, Any], 
        message: Dict[str, Any]
    ) -> list:
        """Validate message fields against Avro schema."""
        errors = []
        
        if schema.get("type") != "record":
            return errors
        
        fields = schema.get("fields", [])
        
        for field in fields:
            field_name = field["name"]
            field_type = field["type"]
            
            # Check required fields (non-union types or unions without null)
            is_required = not self._is_nullable(field_type)
            
            if is_required and field_name not in message:
                errors.append(f"Missing required field: {field_name}")
                continue
            
            if field_name in message:
                value = message[field_name]
                
                # Skip null values for nullable fields
                if value is None and self._is_nullable(field_type):
                    continue
                
                # Type validation
                type_error = self._validate_type(field_name, field_type, value)
                if type_error:
                    errors.append(type_error)
        
        return errors
    
    def _is_nullable(self, field_type) -> bool:
        """Check if field type allows null."""
        if isinstance(field_type, list):
            return "null" in field_type
        return False
    
    def _validate_type(
        self, 
        field_name: str, 
        field_type, 
        value: Any
    ) -> Optional[str]:
        """Validate value against Avro type."""
        # Handle union types
        if isinstance(field_type, list):
            # Try each type in union
            for t in field_type:
                if t == "null" and value is None:
                    return None
                if self._matches_type(t, value):
                    return None
            return f"Field {field_name}: value {type(value).__name__} doesn't match union {field_type}"
        
        # Handle simple types
        if not self._matches_type(field_type, value):
            return f"Field {field_name}: expected {field_type}, got {type(value).__name__}"
        
        return None
    
    def _matches_type(self, avro_type, value: Any) -> bool:
        """Check if value matches Avro type."""
        if avro_type == "null":
            return value is None
        elif avro_type == "boolean":
            return isinstance(value, bool)
        elif avro_type == "int":
            return isinstance(value, int) and not isinstance(value, bool)
        elif avro_type == "long":
            return isinstance(value, int) and not isinstance(value, bool)
        elif avro_type == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif avro_type == "double":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif avro_type == "string":
            return isinstance(value, str)
        elif avro_type == "bytes":
            return isinstance(value, (bytes, str))
        elif isinstance(avro_type, dict):
            # Complex types (record, array, map, etc.)
            type_name = avro_type.get("type")
            if type_name == "array":
                return isinstance(value, list)
            elif type_name == "map":
                return isinstance(value, dict)
            elif type_name == "record":
                return isinstance(value, dict)
        
        return True  # Unknown types pass through
    
    def clear_cache(self) -> None:
        """Clear the schema cache."""
        self._schema_cache.clear()
        logger.info("Schema cache cleared")
    
    def refresh_schema(self, subject: str) -> None:
        """Refresh a specific schema from registry."""
        if subject in self._schema_cache:
            del self._schema_cache[subject]
        self._get_schema(subject)
        logger.info(f"Refreshed schema for {subject}")