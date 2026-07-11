"""
Test suite for the synchronous generator registration and cleanup functionality in Lifecycle.

This module verifies:
- Registration of synchronous generators for cleanup in specific lifecycle scopes.
- Handling of invalid scope registration attempts.
- Decorator-based registration for factories and generators.
- Proper cleanup and resource management of generators.
- Collection of multiple exceptions during generator cleanup into an ExceptionGroup.
"""

from unittest.mock import Mock

import pytest

from stratae.lifecycle import Lifecycle, resource


def test_generator_sync(lifecycle: Lifecycle):
    """
    Test using a synchronous generator using the decorator syntax.

    Given: A Lifecycle instance and a generator function
    When: The generator function is decorated for a specific scope
    Then: The generator should be registered for cleanup in that scope
    """
    # Arrange
    mock_cleanup = Mock()
    mock_commit = Mock()

    class SimpleObject: ...

    @lifecycle.cache("application")
    @resource
    def sample_generator():
        try:
            yield SimpleObject()
            mock_commit()
        finally:
            mock_cleanup()

    # Act
    with lifecycle.start("application"):
        call_1 = sample_generator()
        call_2 = sample_generator()

        # Assert
        mock_commit.assert_not_called()
        mock_cleanup.assert_not_called()

    assert isinstance(call_1, SimpleObject)
    assert call_1 is call_2
    mock_commit.assert_called_once()
    mock_cleanup.assert_called_once()


def test_decorate_generator_inactive_scope(lifecycle: Lifecycle):
    """
    Test using a synchronous generator with an inactive scope.

    Given: A Lifecycle instance and a generator function
    When: An attempt is made to use the generator before its scope is active
    Then: A KeyError should be raised indicating the scope was not ready
    """

    # Arrange
    @lifecycle.cache("application")
    @resource
    def sample_generator():
        yield "test"

    # Act & Assert
    with pytest.raises(RuntimeError, match="Scope 'application' is not active."):
        sample_generator()


def test_decorate_generator_sync_exception_cleanup(lifecycle: Lifecycle):
    """
    Test using a synchronous generator that raises an exception during cleanup.

    Given: A Lifecycle instance and a decorated generator function
    When: The decorated function raises an exception in cleanup
    Then: The exception should be propagated correctly
    """
    # Arrange
    mock_cleanup = Mock()

    class SimpleObject: ...

    @lifecycle.cache("application")
    @resource
    def sample_generator():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup()
            raise ValueError("Test Failure")

    # Act & Assert
    with pytest.raises(ValueError, match="Test Failure"):
        with lifecycle.start("application"):
            sample_generator()
            mock_cleanup.assert_not_called()


def test_decorator_generator_sync_exception_handling(lifecycle: Lifecycle):
    """
    Test using a synchronous generator with an exception block.

    Given: A Lifecycle instance and a decorated generator function
    When: The decorated function raises an exception in the try block
    Then: the exception should be propagated and the cleanup function should be called
    """
    # Arrange
    mock_cleanup = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    mock_except = Mock()

    class SimpleObject: ...

    @lifecycle.cache("application")
    @resource
    def sample_generator():
        try:
            yield SimpleObject()
            mock_failure()
        except ValueError:
            mock_except()
            raise
        finally:
            mock_cleanup()

    # Act & Assert
    with pytest.raises(ValueError, match="Test Failure"):
        with lifecycle.start("application"):
            sample_generator()
            mock_cleanup.assert_not_called()
            mock_failure.assert_not_called()
            mock_except.assert_not_called()

    mock_except.assert_called_once()
    mock_cleanup.assert_called_once()


def test_multiple_generators_sync(lifecycle: Lifecycle):
    """
    Test registering multiple synchronous generators for cleanup.

    Given: A Lifecycle instance and multiple generator functions
    When: The generator functions are registered for a specific scope
    Then: All generators should be cleaned up in the correct order
    """
    # Arrange
    cleanup_order: list[str] = []
    mock_cleanup_1 = Mock(side_effect=lambda: cleanup_order.append("first"))
    mock_cleanup_2 = Mock(side_effect=lambda: cleanup_order.append("second"))
    mock_cleanup_3 = Mock(side_effect=lambda: cleanup_order.append("third"))

    class SimpleObject: ...

    @lifecycle.cache("application")
    @resource
    def generator_one():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @lifecycle.cache("application")
    @resource
    def generator_two():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @lifecycle.cache("application")
    @resource
    def generator_three():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    with lifecycle.start("application"):
        generator_one()
        generator_two()
        generator_three()

    # Assert
    assert cleanup_order == ["third", "second", "first"]
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()


def test_generator_sync_exception_group(lifecycle: Lifecycle):
    """
    Test that multiple exceptions during generator cleanup are collected into an ExceptionGroup.

    Given: A Lifecycle instance and multiple generator functions that raise exceptions
    When: The generators are cleaned up
    Then: An ExceptionGroup containing all exceptions should be raised
    """
    # Arrange
    mock_cleanup_1 = Mock(side_effect=ValueError("First Failure"))
    mock_cleanup_2 = Mock(side_effect=KeyError("Second Failure"))
    mock_cleanup_3 = Mock(side_effect=RuntimeError("Third Failure"))

    class SimpleObject: ...

    @lifecycle.cache("application")
    @resource
    def generator_one():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @lifecycle.cache("application")
    @resource
    def generator_two():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @lifecycle.cache("application")
    @resource
    def generator_three():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act & Assert
    with pytest.raises(ExceptionGroup) as exceptions:
        with lifecycle.start("application"):
            generator_one()
            generator_two()
            generator_three()

    assert len(exceptions.value.exceptions) == 3
    assert any(
        isinstance(exc, ValueError) and str(exc) == "First Failure"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, KeyError) and str(exc) == "'Second Failure'"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, RuntimeError) and str(exc) == "Third Failure"
        for exc in exceptions.value.exceptions
    )
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()
