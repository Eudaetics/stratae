"""Tests for AsyncExitStack and the exit-stack teardown behavior of AsyncLifecycle.pop."""

from contextlib import asynccontextmanager
from unittest.mock import Mock

import pytest

from stratae.lifecycle import AsyncLifecycle, Scope
from stratae.lifecycle._stack import AsyncExitStack


async def test_context_behavior():
    """
    Test that the exit stack properly manages context managers.

    Given: an AsyncExitStack containing an entered context manager
    When: the stack is closed
    Then: the context manager's cleanup function should be called
    """
    # Arrange
    stack = AsyncExitStack()

    spy_mock = Mock()
    spy_success = Mock()

    @asynccontextmanager
    async def generator():
        try:
            yield
            spy_success()
        finally:
            spy_mock()

    # Act
    await stack.enter_async_context(generator())
    spy_mock.assert_not_called()
    spy_success.assert_not_called()

    # Assert
    await stack.aclose()
    spy_mock.assert_called_once()
    spy_success.assert_called_once()


async def test_context_with_failure():
    """
    Test that the exit stack properly handles exceptions within a context.

    Given: an AsyncExitStack containing an entered context manager that raises an exception
    When: the stack is closed
    Then: the exception should be propagated and the cleanup function should be called
    """
    # Arrange
    stack = AsyncExitStack()

    spy_mock = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    spy_except = Mock()

    @asynccontextmanager
    async def generator():
        try:
            yield
            mock_failure()
        except ValueError:
            spy_except()
            raise
        finally:
            spy_mock()

    # Act
    await stack.enter_async_context(generator())
    spy_mock.assert_not_called()
    mock_failure.assert_not_called()
    spy_except.assert_not_called()

    # Assert
    with pytest.raises(ValueError, match="Test Failure"):
        await stack.aclose()
    spy_except.assert_called_once()
    spy_mock.assert_called_once()


async def test_pop_closes_stack():
    """
    Test that popping a scope closes the exit stack created during its activation.

    Given: an active AsyncLifecycle scope with an entered context manager,
    When: the scope is popped,
    Then: the context manager's cleanup function should be called.
    """
    # Arrange
    cleanup = Mock()
    lifecycle = AsyncLifecycle([Scope("application", "context")])
    token = lifecycle.push("application")

    @asynccontextmanager
    async def generator():
        try:
            yield
        finally:
            cleanup()

    await lifecycle.get_exit_stack("application").enter_async_context(generator())

    # Act
    await lifecycle.pop(token)

    # Assert
    cleanup.assert_called_once()
