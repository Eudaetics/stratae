"""Unit tests for AsyncScope.activate/deactivate, exercised via the async AsyncScope class."""

import pytest

from stratae.lifecycle.exceptions import ScopeActivationError
from stratae.lifecycle.resource import async_resource
from stratae.lifecycle.scope import AsyncScope


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


async def test_exception_raised_while_handling_another_propagates_unchanged():
    """
    An exception raised while handling another propagates as-is through deactivation.

    Given: An active scope whose block raises a ValueError while handling a KeyError,
    When: the scope deactivates on the way out,
    Then: the ValueError itself propagates - not an ExceptionGroup bundling it with the
          KeyError it was raised during.
    """
    # Arrange
    scope = AsyncScope("request")
    raised = ValueError("translated")

    # Act
    with pytest.raises(ValueError) as exc_info:
        async with scope.activate():
            try:
                raise KeyError("missing")
            except KeyError:
                raise raised from None

    # Assert
    assert exc_info.value is raised


async def test_exception_raised_while_handling_another_propagates_unchanged_through_resource():
    """
    A resource passing an exception through doesn't regroup it with its __context__.

    Given: An active scope holding an entered resource that re-raises whatever reaches it,
    When: the block raises a ValueError while handling a KeyError,
    Then: the resource sees the ValueError, and the ValueError itself propagates.
    """
    # Arrange
    scope = AsyncScope("request")
    seen: list[BaseException] = []

    @scope.cache()
    @async_resource
    async def get_connection():
        try:
            yield object()
        except Exception as exc:
            seen.append(exc)
            raise

    raised = ValueError("translated")

    # Act
    with pytest.raises(ValueError) as exc_info:
        async with scope.activate():
            await get_connection()
            try:
                raise KeyError("missing")
            except KeyError:
                raise raised from None

    # Assert
    assert seen == [raised]
    assert exc_info.value is raised
