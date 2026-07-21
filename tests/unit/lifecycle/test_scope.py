"""Validation of Scope construction."""

import pytest

from stratae.lifecycle import Scope


def test_scope_init():
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
    Scope should default to context and dense options.

    Given: only a name,
    When: Scope is constructed without isolation or storage,
    Then: it should default isolation to "context" and storage to "dense".
    """
    # Arrange
    name = "request"

    # Act
    scope = Scope(name)

    # Assert
    assert scope.isolation == "context"
    assert scope.storage == "dense"


def test_scope_keyword_init():
    """
    Assign Scope fields via keyword arguments.

    Given: a name and isolation level,
    When: Scope is constructed using keyword arguments,
    Then: it should store each field correctly.
    """
    # Arrange
    name = "request"
    isolation = "context"

    # Act
    scope = Scope(name=name, isolation=isolation)

    # Assert
    assert scope.name == name
    assert scope.isolation == isolation


@pytest.mark.parametrize("invalid_name", ["app-1", "request scope", "123scope", "scope!"])
def test_scope_init_with_non_identifier_name(invalid_name: str):
    """
    Constructing a Scope with a non-identifier name raises an error.

    Given: a scope name that is not a valid Python identifier,
    When: a Scope is constructed with that name,
    Then: a ValueError should be raised.
    """
    # Act & Assert
    with pytest.raises(ValueError, match="All scopes must be valid Python identifiers."):
        Scope(invalid_name, "shared")


def test_scope_init_with_invalid_isolation():
    """
    Constructing a Scope with an invalid isolation raises an error.

    Given: an isolation value that is neither "shared" nor "context",
    When: a Scope is constructed with that isolation,
    Then: a ValueError should be raised.
    """
    # Act & Assert
    with pytest.raises(ValueError, match="Invalid scope isolation given for application."):
        Scope("application", "bogus")  # pyright: ignore[reportArgumentType]


def test_scope_init_with_invalid_storage():
    """
    Constructing a Scope with an invalid storage raises an error.

    Given: a storage value that is neither "dense" nor "sparse",
    When: a Scope is constructed with that storage,
    Then: a ValueError should be raised.
    """
    # Act & Assert
    with pytest.raises(ValueError, match="Invalid scope storage given for application."):
        Scope("application", "shared", "bogus")  # pyright: ignore[reportArgumentType]
