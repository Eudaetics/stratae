"""
Unit tests for initialization and configuration for the `AsyncLifecycle` class.

This test suite verifies the following behaviors:
- Proper initialization of the Lifecycle stack.
- Enforcement of unique scope names.
- Error handling for duplicate or missing scopes.
- Attribute and cache access for valid and invalid scopes.

Per-scope validation (identifier names, isolation values) raises at Scope
construction and is covered in tests/unit/lifecycle/test_scope.py.
"""

import pytest

from stratae.lifecycle import AsyncLifecycle, Scope
from stratae.lifecycle.exceptions import ScopeNotFoundError


def test_initialization(async_lifecycle: AsyncLifecycle):
    """
    Test that AsyncLifecycle initializes with an empty lifecycle stack.

    Given: A new instance of AsyncLifecycle
    When: The instance is created
    Then: The lifecycle stack should be empty
    """
    # Arrange & Act (done via fixture)

    # Assert
    assert async_lifecycle.is_empty()


def test_initialization_with_duplicate_scopes():
    """
    Test that initializing Lifecycle with duplicate scopes raises an error.

    Given: A list of lifecycle scopes with duplicates
    When: An attempt is made to create a Lifecycle instance
    Then: A ValueError should be raised
    """
    # Arrange
    scopes = [Scope(name, "context") for name in ["application", "request", "session", "request"]]

    # Act & Assert
    with pytest.raises(ValueError, match="All scopes must be unique."):
        AsyncLifecycle(scopes)


def test_initialization_with_no_scopes():
    """
    Test that initializing Lifecycle with no scopes raises an error.

    Given: An empty list of lifecycle scopes
    When: An attempt is made to create a Lifecycle instance
    Then: A ValueError should be raised
    """
    # Arrange
    scopes: list[Scope] = []

    # Act & Assert
    with pytest.raises(ValueError, match="At least one scope must be defined."):
        AsyncLifecycle(scopes)


def test_get_slots_invalid_scope(async_lifecycle: AsyncLifecycle):
    """
    Test that getting slots for an invalid scope raises an error.

    Given: An AsyncLifecycle instance
    When: An attempt is made to get slots for an invalid scope
    Then: A ValueError should be raised
    """
    # Arrange (done via fixture)

    # Act & Assert
    with pytest.raises(ValueError, match="Unknown scope: bad"):
        async_lifecycle.get_slots("bad")


def test_allocate_invalid(async_lifecycle: AsyncLifecycle):
    """
    Allocating a slot for an invalid scope raises.

    Given: An AsyncLifecycle instance,
    When: An attempt is made to allocate a slot for a non-existent scope,
    Then: A ScopeNotFoundError is raised.
    """
    with pytest.raises(ScopeNotFoundError, match="Unknown scope: bad"):
        async_lifecycle.allocate_slot("bad")
