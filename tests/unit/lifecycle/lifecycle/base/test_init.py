"""
Unit tests for initialization and configuration for BaseLifecycle, exercised via Lifecycle.

__init__, get_slots, and allocate_slot's error paths are inherited unchanged by
AsyncLifecycle, so testing them through Lifecycle covers both - there is no separate
async test module for this.

Per-scope validation (identifier names, isolation values) raises at Scope
construction and is covered in tests/unit/lifecycle/test_scope.py.
"""

import pytest

from stratae.lifecycle import Lifecycle, Scope
from stratae.lifecycle.exceptions import ScopeNotFoundError


def test_initialization(lifecycle: Lifecycle):
    """
    Test that Lifecycle initializes with an empty lifecycle stack.

    Given: A new instance of Lifecycle
    When: The instance is created
    Then: The lifecycle stack should be empty
    """
    assert lifecycle.is_empty()


def test_initialization_with_duplicate_scopes():
    """
    Test that initializing Lifecycle with duplicate scopes raises an error.

    Given: A list of lifecycle scopes with duplicates
    When: An attempt is made to create a Lifecycle instance
    Then: A ValueError should be raised
    """
    # Arrange
    scopes = [Scope(name, "shared") for name in ["application", "request", "session", "request"]]

    # Act & Assert
    with pytest.raises(ValueError, match="All scopes must be unique."):
        Lifecycle(scopes)


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
        Lifecycle(scopes)


def test_get_slots_invalid_scope(lifecycle: Lifecycle):
    """
    Test that getting slots for an invalid scope raises an error.

    Given: A Lifecycle instance
    When: An attempt is made to get slots for an invalid scope
    Then: A ValueError should be raised
    """
    # Act & Assert
    with pytest.raises(ValueError, match="Unknown scope: bad"):
        lifecycle.get_slots("bad")


def test_allocate_invalid(lifecycle: Lifecycle):
    """
    Allocating a slot for an invalid scope raises.

    Given: A Lifecycle instance,
    When: An attempt is made to allocate a slot for a non-existent scope,
    Then: A ScopeNotFoundError is raised.
    """
    with pytest.raises(ScopeNotFoundError, match="Unknown scope: bad"):
        lifecycle.allocate_slot("bad")
