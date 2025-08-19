"""Test for Lance schema blob encoding issue #4939."""

from __future__ import annotations


def test_lance_schema_blob_encoding():
    """Test for issue #4939: Lance schema does not work with blob encoding.

    This is a simple test that will initially fail.
    """
    # Simple failing assertion - this will fail
    assert 1 == 0, "Lance blob encoding not yet implemented"


def test_lance_schema_basic():
    """Another simple test to demonstrate the testing pattern."""
    # This one also fails initially
    result = False
    assert result is True, "Test intentionally failing"
