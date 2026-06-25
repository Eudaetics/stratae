"""Tests for the ActiveScope class in stratae.lifecycle._scope."""

from contextlib import contextmanager
from typing import Generator
from unittest.mock import Mock

import pytest

from stratae.cache import MemoryCache
from stratae.lifecycle._scope import ActiveScope


def test_context_behavior():
    """
    Test that the exit stack properly manages context managers.

    Given: an ActiveScope with an exit stack containing a context manager
    When: the exit stack is closed
    Then: the context manager's cleanup function should be called
    """
    # Arrange
    scope = ActiveScope(MemoryCache)

    spy_mock = Mock()
    spy_success = Mock()

    @contextmanager
    def generator() -> Generator[str, None, None]:
        try:
            yield "resource"
            spy_success()
        finally:
            spy_mock()

    # Act
    scope.exit_stack.enter_context(generator())
    spy_mock.assert_not_called()
    spy_success.assert_not_called()

    # Assert
    scope.exit_stack.close()
    spy_mock.assert_called_once()
    spy_success.assert_called_once()


def test_context_with_failure():
    """
    Test that the exit stack properly handles exceptions within a context.

    Given: an ActiveScope with an exit stack containing a context manager that raises an exception
    When: the exit stack is closed
    Then: the exception should be propagated and the cleanup function should be called
    """
    # Arrange
    scope = ActiveScope(MemoryCache)

    spy_mock = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    spy_except = Mock()

    @contextmanager
    def generator() -> Generator[str, None, None]:
        try:
            yield "resource"
            mock_failure()
        except ValueError:
            spy_except()
            raise
        finally:
            spy_mock()

    # Act
    scope.exit_stack.enter_context(generator())
    spy_mock.assert_not_called()
    mock_failure.assert_not_called()
    spy_except.assert_not_called()

    # Assert
    with pytest.raises(ValueError, match="Test Failure"):
        scope.exit_stack.close()
    spy_except.assert_called_once()
    spy_mock.assert_called_once()


def test_clear():
    """
    Test clearing the scope's cache and exit stack.

    Given: an ActiveScope with mock cache and exit stack,
    When: clear is called,
    Then: both the cache and exit stack should be cleared.
    """
    # Arrange
    cleanup = Mock()

    scope = ActiveScope(MemoryCache)

    @contextmanager
    def generator():
        try:
            yield "Something"
        finally:
            cleanup()

    scope.cache.set("key", "value")
    scope.exit_stack.enter_context(generator())

    # Act
    scope.clear()

    # Assert
    assert scope.cache.is_empty()
    cleanup.assert_called_once()
