"""
Unit tests for BaseScope construction and validation, exercised via Scope.

__init__'s validation is inherited unchanged by AsyncScope, so testing it through Scope
covers both - there is no separate async test module for this.
"""

import pytest

from stratae.lifecycle.exceptions import LifecycleConfigurationError
from stratae.lifecycle.scope import Scope


def test_scope_init_positional():
    """
    Construct Scope with positional args.

    Given: a name and isolation level,
    When: Scope is constructed positionally,
    Then: it should store the name and isolation.
    """
    # Arrange
    name = "request"
    isolation = "shared"

    # Act
    scope = Scope(name, isolation)

    # Assert
    assert scope.name == name
    assert scope.isolation == isolation


def test_scope_init_defaults():
    """
    Scope should default to context isolation, dense storage, and no required parent.

    Given: only a name,
    When: Scope is constructed without isolation, storage, or requires,
    Then: it should default isolation to "context", storage to "dense", and requires to None.
    """
    # Arrange
    name = "request"

    # Act
    scope = Scope(name)

    # Assert
    assert scope.isolation == "context"
    assert scope.storage == "dense"
    assert scope.requires is None


def test_scope_init_keyword():
    """
    Assign Scope fields via keyword arguments.

    Given: a name, isolation, storage, and requires,
    When: Scope is constructed using keyword arguments,
    Then: it should store each field correctly.
    """
    # Arrange
    name = "request"
    isolation = "context"
    parent = Scope("application", "shared")

    # Act
    scope = Scope(name=name, isolation=isolation, storage="sparse", requires=parent)

    # Assert
    assert scope.name == name
    assert scope.isolation == isolation
    assert scope.storage == "sparse"
    assert scope.requires is parent


def test_scope_init_with_invalid_isolation():
    """
    Constructing a Scope with an invalid isolation raises an error.

    Given: an isolation value that is neither "shared" nor "context",
    When: a Scope is constructed with that isolation,
    Then: a LifecycleConfigurationError should be raised.
    """
    # Act & Assert
    with pytest.raises(
        LifecycleConfigurationError, match="Invalid scope isolation given for application."
    ):
        Scope("application", "bogus")  # pyright: ignore[reportArgumentType]


def test_scope_init_with_invalid_storage():
    """
    Constructing a Scope with an invalid storage raises an error.

    Given: a storage value that is neither "dense" nor "sparse",
    When: a Scope is constructed with that storage,
    Then: a LifecycleConfigurationError should be raised.
    """
    # Act & Assert
    with pytest.raises(
        LifecycleConfigurationError, match="Invalid scope storage given for application."
    ):
        Scope("application", "shared", "bogus")  # pyright: ignore[reportArgumentType]


def test_scope_init_shared_requiring_context_raises():
    """
    A shared scope cannot require a context-isolated scope.

    Given: a context-isolated parent scope,
    When: a shared-isolation Scope is constructed requiring it,
    Then: a LifecycleConfigurationError should be raised, and the child scope should never
        be usable - the constructor raises before returning an instance.
    """
    # Arrange
    parent = Scope("application", "context")

    # Act & Assert
    with pytest.raises(
        LifecycleConfigurationError,
        match="Shared scope 'child' cannot require context-isolated scope 'application'.",
    ):
        Scope("child", "shared", requires=parent)


@pytest.mark.parametrize(
    "child_isolation,parent_isolation",
    [("context", "context"), ("context", "shared"), ("shared", "shared")],
)
def test_scope_init_requires_allowed_combinations(child_isolation: str, parent_isolation: str):
    """
    Every isolation combination other than shared-requiring-context is allowed.

    Given: a parent scope with a given isolation,
    When: a child scope requiring it is constructed with an allowed isolation combination,
    Then: construction succeeds and the child stores the parent as its requires.
    """
    # Arrange
    parent = Scope("application", parent_isolation)  # pyright: ignore[reportArgumentType]

    # Act
    child = Scope("child", child_isolation, requires=parent)  # pyright: ignore[reportArgumentType]

    # Assert
    assert child.requires is parent


def test_scope_isolation_is_read_only():
    """
    Scope.isolation has no setter - it's fixed at construction.

    Given: a constructed Scope,
    When: an attempt is made to reassign its isolation,
    Then: an AttributeError should be raised.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    with pytest.raises(AttributeError):
        scope.isolation = "context"  # pyright: ignore[reportAttributeAccessIssue]


def test_scope_storage_is_read_only():
    """
    Scope.storage has no setter - it's fixed at construction.

    Given: a constructed Scope,
    When: an attempt is made to reassign its storage,
    Then: an AttributeError should be raised.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    with pytest.raises(AttributeError):
        scope.storage = "sparse"  # pyright: ignore[reportAttributeAccessIssue]
