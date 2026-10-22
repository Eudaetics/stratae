"""Unit tests for using a Scope directly as a context manager, and token-free deactivate()."""

from contextvars import copy_context
from typing import Generator

import pytest

from stratae.lifecycle.exceptions import ScopeActivationError
from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope


def test_with_scope_activates_and_deactivates():
    """
    A scope used directly in a with block is active inside it and inactive after.

    Given: A Scope,
    When: It is entered with `with scope:`,
    Then: is_active() is True inside the block and False after.
    """
    # Arrange
    scope = Scope("request")

    # Act & Assert
    with scope:
        assert scope.is_active()
    assert not scope.is_active()


def test_with_scope_closes_entered_resources():
    """
    Leaving a `with scope:` block closes the resources entered within it.

    Given: A Scope caching a resource,
    When: The resource is entered inside `with scope:` and the block ends,
    Then: The resource's cleanup has run.
    """
    # Arrange
    scope = Scope("request")
    events: list[str] = []

    @scope.cache()
    @resource
    def get_value() -> Generator[int, None, None]:
        yield 1
        events.append("closed")

    # Act
    with scope:
        get_value()

    # Assert
    assert events == ["closed"]


def test_deactivate_without_token_ends_current_activation():
    """
    deactivate() with no token ends the scope's current activation.

    Given: A Scope activated with `__enter__`, without a token,
    When: deactivate() is called with no arguments,
    Then: is_active() returns False.
    """
    # Arrange
    scope = Scope("request")
    scope.__enter__()

    # Act
    scope.deactivate()

    # Assert
    assert not scope.is_active()


def test_deactivate_without_token_raises_when_inactive():
    """
    deactivate() with no token raises when the scope isn't active.

    Given: A Scope that isn't active,
    When: deactivate() is called with no arguments,
    Then: A ScopeActivationError is raised.
    """
    # Arrange
    scope = Scope("request")

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot deactivate 'request': not active."):
        scope.deactivate()


def test_deactivate_without_token_raises_in_copied_context():
    """
    deactivate() with no token raises in a copied context that only sees the activation.

    Given: A context Scope activated in one context, and a copy of that context,
    When: deactivate() is called with no arguments inside the copy,
    Then: A ScopeActivationError is raised, and the activation stays live where it began.
    """
    # Arrange
    scope = Scope("request")
    scope.__enter__()
    copied = copy_context()

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="outside the context that activated it"):
        copied.run(scope.deactivate)
    assert scope.is_active()
    scope.deactivate()


def test_deactivate_returns_whether_exception_was_suppressed():
    """
    deactivate() hands its exception to resource cleanup and reports whether it was suppressed.

    Given: A Scope with an entered resource whose cleanup suppresses any exception,
    When: The activation is ended with deactivate(activation, exc),
    Then: It returns True.
    """
    # Arrange
    scope = Scope("request")

    @scope.cache()
    @resource
    def get_value() -> Generator[int, None, None]:
        try:
            yield 1
        except ValueError:
            pass

    activation = scope.activate()
    get_value()

    # Act
    suppressed = scope.deactivate(activation, ValueError("boom"))

    # Assert
    assert suppressed
