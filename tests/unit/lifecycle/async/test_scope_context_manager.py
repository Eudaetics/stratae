"""Unit tests for using an AsyncScope directly as a context manager, and token-free deactivate()."""

import asyncio
from contextvars import copy_context
from typing import AsyncGenerator

import pytest

from stratae.lifecycle.exceptions import ScopeActivationError
from stratae.lifecycle.resource import async_resource
from stratae.lifecycle.scope import AsyncScope


async def test_async_with_scope_activates_and_deactivates():
    """
    An async scope used directly in an async with block is active inside it and inactive after.

    Given: An AsyncScope,
    When: It is entered with `async with scope:`,
    Then: is_active() is True inside the block and False after.
    """
    # Arrange
    scope = AsyncScope("request")

    # Act & Assert
    async with scope:
        assert scope.is_active()
    assert not scope.is_active()


async def test_async_with_scope_closes_entered_resources():
    """
    Leaving an `async with scope:` block closes the resources entered within it.

    Given: An AsyncScope caching an async resource,
    When: The resource is entered inside `async with scope:` and the block ends,
    Then: The resource's cleanup has run.
    """
    # Arrange
    scope = AsyncScope("request")
    events: list[str] = []

    @scope.cache()
    @async_resource
    async def get_value() -> AsyncGenerator[int, None]:
        yield 1
        events.append("closed")

    # Act
    async with scope:
        await get_value()

    # Assert
    assert events == ["closed"]


async def test_deactivate_without_token_ends_current_activation():
    """
    deactivate() with no token ends the scope's current activation.

    Given: An AsyncScope activated with `__aenter__`, without a token,
    When: deactivate() is awaited with no arguments,
    Then: is_active() returns False.
    """
    # Arrange
    scope = AsyncScope("request")
    await scope.__aenter__()

    # Act
    await scope.deactivate()

    # Assert
    assert not scope.is_active()


async def test_deactivate_without_token_raises_when_inactive():
    """
    deactivate() with no token raises when the scope isn't active.

    Given: An AsyncScope that isn't active,
    When: deactivate() is awaited with no arguments,
    Then: A ScopeActivationError is raised.
    """
    # Arrange
    scope = AsyncScope("request")

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot deactivate 'request': not active."):
        await scope.deactivate()


async def test_deactivate_without_token_raises_in_copied_context():
    """
    deactivate() with no token raises in a task whose copied context only sees the activation.

    Given: A context AsyncScope activated in one task, and a child task copying its context,
    When: deactivate() is awaited with no arguments inside the child task,
    Then: A ScopeActivationError is raised, and the activation stays live where it began.
    """
    # Arrange
    scope = AsyncScope("request")
    await scope.__aenter__()

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="outside the context that activated it"):
        await asyncio.create_task(scope.deactivate(), context=copy_context())
    assert scope.is_active()
    await scope.deactivate()


async def test_deactivate_returns_whether_exception_was_suppressed():
    """
    deactivate() hands its exception to resource cleanup and reports whether it was suppressed.

    Given: An AsyncScope with an entered async resource whose cleanup suppresses any exception,
    When: The activation is ended with deactivate(activation, exc),
    Then: It returns True.
    """
    # Arrange
    scope = AsyncScope("request")

    @scope.cache()
    @async_resource
    async def get_value() -> AsyncGenerator[int, None]:
        try:
            yield 1
        except ValueError:
            pass

    activation = scope.activate()
    await get_value()

    # Act
    suppressed = await scope.deactivate(activation, ValueError("boom"))

    # Assert
    assert suppressed
