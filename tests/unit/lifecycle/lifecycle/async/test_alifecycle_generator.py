"""
Tests for behavior of AsyncLifecycle with async generators.

This test module validates the behavior of the `AsyncLifecycle` class from the `stratae.lifecycle`
package, specifically focusing on the registration, cleanup, and error handling of async generators
within various lifecycle scopes. The tests cover:

- Registering async generators for cleanup in specific scopes.
- Handling invalid scope registration.
- Using decorator syntax for async factories and generators.
- Ensuring generators are cleaned up correctly.
- Collecting multiple errors during generator cleanup into an ExceptionGroup.
- Verifying error handling when cleaning up async generators in a synchronous context.

Each test is designed to ensure robust resource management and error reporting for async generator
lifecycles.
"""

import asyncio
from typing import Sequence
from unittest.mock import Mock

import pytest

from stratae.lifecycle import async_resource
from stratae.lifecycle.lifecycle import AsyncLifecycle


async def test_generator_async(async_lifecycle: AsyncLifecycle, scopes: Sequence[str]):
    """
    Test using an async generator in an async lifecycle with decorator syntax.

    Given: An AsyncLifecycle instance and an async generator
    When: The generator is decorated for a specific scope
    Then: The generator should yield the expected value in that scope and cleanup at end
    """
    # Arrange
    mock_commit = Mock()
    mock_cleanup = Mock()

    class SimpleObject: ...

    @async_lifecycle.cache("application")
    @async_resource
    async def sample_generator():
        try:
            yield SimpleObject()
            mock_commit()
        finally:
            mock_cleanup()

    # Act
    async with async_lifecycle.start("application"):
        result1 = await sample_generator()
        result2 = await sample_generator()

        mock_commit.assert_not_called()
        mock_cleanup.assert_not_called()

    assert isinstance(result1, SimpleObject)
    assert result1 is result2
    mock_commit.assert_called_once()
    mock_cleanup.assert_called_once()


async def test_register_generator_inactive_scope_async(async_lifecycle: AsyncLifecycle):
    """
    Test using an async generator with an inactive scope.

    Given: An AsyncLifecycle instance and a generator
    When: An attempt is made to use the generator before its scope is active
    Then: A KeyError should be raised indicating the scope was not ready
    """

    # Arrange
    @async_lifecycle.cache("application")
    @async_resource
    async def sample_generator():
        yield "test"

    # Act & Assert
    with pytest.raises(RuntimeError, match="Scope 'application' is not active."):
        await sample_generator()


async def test_decorate_generator_async_exception_cleanup(async_lifecycle: AsyncLifecycle):
    """
    Test using an asynchronous generator that raises an exception during cleanup.

    Given: An AsyncLifecycle instance and a decorated async generator function
    When: The decorated function raises an exception in cleanup
    Then: The exception should be propagated correctly
    """
    # Arrange
    mock_cleanup = Mock()

    class SimpleObject: ...

    @async_lifecycle.cache("application")
    @async_resource
    async def sample_generator():
        try:
            yield SimpleObject()
            await asyncio.sleep(0)
        finally:
            mock_cleanup()
            raise ValueError("Test Failure")

    # Act & Assert
    with pytest.raises(ValueError, match="Test Failure"):
        async with async_lifecycle.start("application"):
            await sample_generator()
            mock_cleanup.assert_not_called()


async def test_decorator_generator_async_exception_handling(async_lifecycle: AsyncLifecycle):
    """
    Test using an asynchronous generator with an exception block.

    Given: An AsyncLifecycle instance and a decorated async generator function
    When: The decorated function raises an exception in the try block
    Then: the exception should be propagated and the cleanup function should be called
    """
    # Arrange
    mock_cleanup = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    mock_except = Mock()

    class SimpleObject: ...

    @async_lifecycle.cache("application")
    @async_resource
    async def sample_generator():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
            mock_failure()
        except ValueError:
            mock_except()
            raise
        finally:
            mock_cleanup()

    # Act & Assert
    with pytest.raises(ValueError, match="Test Failure"):
        async with async_lifecycle.start("application"):
            await sample_generator()
            mock_cleanup.assert_not_called()
            mock_failure.assert_not_called()
            mock_except.assert_not_called()

    mock_except.assert_called_once()
    mock_cleanup.assert_called_once()


async def test_multiple_generators_async(async_lifecycle: AsyncLifecycle):
    """
    Test registering multiple synchronous generators for cleanup.

    Given: An AsyncLifecycle instance and multiple generator functions
    When: The generator functions are registered for a specific scope
    Then: All generators should be cleaned up in the correct order
    """
    # Arrange
    cleanup_order: list[str] = []
    mock_cleanup_1 = Mock(side_effect=lambda: cleanup_order.append("first"))
    mock_cleanup_2 = Mock(side_effect=lambda: cleanup_order.append("second"))
    mock_cleanup_3 = Mock(side_effect=lambda: cleanup_order.append("third"))

    class SimpleObject: ...

    @async_lifecycle.cache("application")
    @async_resource
    async def generator_one():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @async_lifecycle.cache("application")
    @async_resource
    async def generator_two():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @async_lifecycle.cache("application")
    @async_resource
    async def generator_three():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    async with async_lifecycle.start("application"):
        await generator_one()
        await generator_two()
        await generator_three()

    # Assert
    assert cleanup_order == ["third", "second", "first"]
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()


async def test_generator_async_exception_group(async_lifecycle: AsyncLifecycle):
    """
    Test that multiple exceptions during generator cleanup are collected into an ExceptionGroup.

    Given: An AsyncLifecycle instance and multiple generator functions that raise exceptions
    When: The generators are cleaned up
    Then: An ExceptionGroup containing all exceptions should be raised
    """
    # Arrange
    mock_cleanup_1 = Mock(side_effect=ValueError("First Failure"))
    mock_cleanup_2 = Mock(side_effect=KeyError("Second Failure"))
    mock_cleanup_3 = Mock(side_effect=RuntimeError("Third Failure"))

    class SimpleObject: ...

    @async_lifecycle.cache("application")
    @async_resource
    async def generator_one():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @async_lifecycle.cache("application")
    @async_resource
    async def generator_two():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @async_lifecycle.cache("application")
    @async_resource
    async def generator_three():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act & Assert
    with pytest.raises(ExceptionGroup) as exceptions:
        async with async_lifecycle.start("application"):
            await generator_one()
            await generator_two()
            await generator_three()

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
