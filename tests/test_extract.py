# tests/test_extract.py
"""Tests for rag.extract module."""

import pytest
from rag.extract import extract_driver


def test_extract_driver_finds_pattern():
    """Test extract_driver finds matching patterns in contexts."""
    contexts = [
        "The company saw growth in cloud services.",
        "Revenue was driven by iPhone sales increase.",
        "Services segment contributed significantly."
    ]
    # Patterns must have capture groups - extract_driver expects group(1)
    patterns = [
        r"driven by (.+?)(?:\.|$)",  # Captures what comes after "driven by"
        r"(iPhone|cloud|Services)",  # Captures the match itself
    ]
    
    result = extract_driver(contexts, patterns)
    
    # Should find one of the patterns
    assert result is not None
    assert isinstance(result, str)
    assert len(result) > 0


def test_extract_driver_no_match():
    """Test extract_driver returns None when no patterns match."""
    contexts = ["No relevant information here."]
    patterns = [r"(iPhone|cloud)"]
    
    result = extract_driver(contexts, patterns)
    
    # Should return None when nothing matches
    assert result is None


def test_extract_driver_empty_contexts():
    """Test extract_driver handles empty contexts gracefully."""
    contexts = []
    patterns = [r"(iPhone)"]
    
    result = extract_driver(contexts, patterns)
    
    assert result is None