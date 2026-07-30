"""Unit tests for AsyncScope.activate/deactivate, exercised via the async AsyncScope class."""

import pytest

from stratae.lifecycle._scope2 import AsyncScope
from stratae.lifecycle.exceptions import ScopeActivationError


async def test_activate_marks_scope_active():
    """
    activate() marks the scope active immediately, before entering it as a context manager.

    Given: An AsyncScope,
    When: activate() is called,
    Then: is_active() returns True.
    """
    # Arrange
    scope = AsyncScope("application", "shared")

    # Act
    scope.activate()

    # Assert
    assert scope.is_active()


async def test_async_with_block_activates_and_deactivates():
    """
    Using activate() as an async with block deactivates the scope on exit.

    Given: An AsyncScope,
    When: it is activated via `async with scope.activate():`,
    Then: is_active() is True inside the block and False after.
    """
    # Arrange
    scope = AsyncScope("application", "shared")

    # Act & Assert
    async with scope.activate():
        assert scope.is_active()
    assert not scope.is_active()


async def test_manual_deactivate_by_token():
    """
    deactivate() ends the activation identified by the given token.

    Given: An AsyncScope activated via activate(),
    When: deactivate() is awaited with the returned token,
    Then: is_active() returns False.
    """
    # Arrange
    scope = AsyncScope("application", "shared")
    activation = scope.activate()

    # Act
    await scope.deactivate(activation)

    # Assert
    assert not scope.is_active()


async def test_context_scope_can_reactivate_within_same_context():
    """
    A context-isolated scope can be activated again before the first activation ends.

    Given: A context-isolated AsyncScope activated once,
    When: it is activated a second time in the same execution context,
    Then: both activations succeed, and deactivating them in LIFO order cleanly restores
        the prior state.
    """
    # Arrange
    scope = AsyncScope("request", "context")
    outer = scope.activate()

    # Act
    inner = scope.activate()

    # Assert
    assert scope.is_active()
    await scope.deactivate(inner)
    assert scope.is_active()
    await scope.deactivate(outer)
    assert not scope.is_active()


async def test_shared_scope_reactivation_raises():
    """
    A shared scope cannot be activated again while already active.

    Given: A shared-isolation AsyncScope that is already active,
    When: activate() is called again,
    Then: A ScopeActivationError is raised.
    """
    # Arrange
    scope = AsyncScope("application", "shared")
    scope.activate()

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot activate shared scope 'application'"):
        scope.activate()


async def test_shared_scope_double_deactivate_raises():
    """
    Deactivating a shared scope's stale token raises.

    Given: A shared-isolation AsyncScope that was activated and already deactivated,
    When: deactivate() is awaited again with the same token,
    Then: A ScopeActivationError is raised.
    """
    # Arrange
    scope = AsyncScope("application", "shared")
    activation = scope.activate()
    await scope.deactivate(activation)

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot deactivate shared scope 'application'"):
        await scope.deactivate(activation)


async def test_context_scope_double_deactivate_raises():
    """
    Deactivating a context scope's stale token raises.

    Given: A context-isolation AsyncScope that was activated and already deactivated,
    When: deactivate() is awaited again with the same token,
    Then: A RuntimeError is raised (the underlying contextvars.Token has already been used).
    """
    # Arrange
    scope = AsyncScope("request", "context")
    activation = scope.activate()
    await scope.deactivate(activation)

    # Act & Assert
    with pytest.raises(RuntimeError):
        await scope.deactivate(activation)
