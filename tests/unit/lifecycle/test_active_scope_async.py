"""Tests for the AsyncActiveScope class in stratae.lifecycle._scope."""

from contextlib import asynccontextmanager
from unittest.mock import Mock

import pytest

from stratae.cache.memory import MemoryCache
from stratae.lifecycle._scope import AsyncActiveScope


async def test_context_behavior():
    """
    Test that the exit stack properly manages context managers.

    Given: an AsyncActiveScope with an exit stack containing a context manager
    When: the exit stack is closed
    Then: the context manager's cleanup function should be called
    """
    # Arrange
    scope = AsyncActiveScope(MemoryCache)

    spy_mock = Mock()
    spy_success = Mock()

    @asynccontextmanager
    async def generator():
        try:
            yield "resource"
            spy_success()
        finally:
            spy_mock()

    # Act
    await scope.exit_stack.enter_async_context(generator())
    spy_mock.assert_not_called()
    spy_success.assert_not_called()

    # Assert
    await scope.exit_stack.aclose()
    spy_mock.assert_called_once()
    spy_success.assert_called_once()


async def test_context_with_failure():
    """
    Test that the exit stack properly handles exceptions within a context.

    Given: an AsyncActiveScope containing a context manager that raises an exception
    When: the exit stack is closed
    Then: the exception should be propagated and the cleanup function should be called
    """
    # Arrange
    scope = AsyncActiveScope(MemoryCache)

    spy_mock = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    spy_except = Mock()

    @asynccontextmanager
    async def generator():
        try:
            yield "resource"
            mock_failure()
        except ValueError:
            spy_except()
            raise
        finally:
            spy_mock()

    # Act
    await scope.exit_stack.enter_async_context(generator())
    spy_mock.assert_not_called()
    mock_failure.assert_not_called()
    spy_except.assert_not_called()

    # Assert
    with pytest.raises(ValueError, match="Test Failure"):
        await scope.exit_stack.aclose()
    spy_except.assert_called_once()
    spy_mock.assert_called_once()


async def test_clear():
    """
    Test clearing the scope's cache and exit stack.

    Given: an AsyncActiveScope with mock cache and exit stack,
    When: clear is called,
    Then: both the cache and exit stack should be cleared.
    """
    # Arrange
    cleanup = Mock()

    scope = AsyncActiveScope(MemoryCache)

    @asynccontextmanager
    async def generator():
        try:
            yield "Something"
        finally:
            cleanup()

    scope.cache.set("key", "value")
    await scope.exit_stack.enter_async_context(generator())

    # Act
    await scope.clear()

    # Assert
    assert scope.cache.is_empty()
    cleanup.assert_called_once()
