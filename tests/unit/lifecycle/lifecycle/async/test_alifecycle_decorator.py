"""Tests for the AsyncLifecycle decorator syntax."""

from unittest.mock import Mock

from stratae.lifecycle import async_resource, resource
from stratae.lifecycle.lifecycle import AsyncLifecycle


async def test_decorator_on_sync(async_lifecycle: AsyncLifecycle):
    """
    Test registering a sync factory using the decorator syntax.

    Given: A AsyncLifecycle instance and a sync factory function
    When: The factory function is decorated for a specific scope
    Then: The factory should only get called once
    """
    # Arrange

    class Sample:
        def __init__(self, value: int = 42):
            self.value = value

    mock = Mock()

    @async_lifecycle.cache("application")
    def sample():
        mock()
        return Sample(2)

    async with async_lifecycle.start("application"):
        call_1 = sample()
        call_2 = sample()

    assert call_1 is call_2
    mock.assert_called_once()


async def test_decorator_on_sync_generator(async_lifecycle: AsyncLifecycle):
    """
    Test registering a sync generator using the decorator syntax.

    Given: A AsyncLifecycle instance and a generator function
    When: The generator function is decorated for a specific scope
    Then: The generator should be registered for cleanup in that scope
    """
    # Arrange
    mock_cleanup = Mock()

    @async_lifecycle.cache("application")
    @resource
    def sample_generator():
        try:
            yield "test"
        finally:
            mock_cleanup()

    async with async_lifecycle.start("application"):
        call_1 = sample_generator()
        call_2 = sample_generator()

    assert call_1 is call_2
    mock_cleanup.assert_called_once()


async def test_decorator_on_async(async_lifecycle: AsyncLifecycle):
    """
    Test registering an async factory using the decorator syntax.

    Given: A AsyncLifecycle instance and an async factory function
    When: The factory function is decorated for a specific scope
    Then: The factory should only get called once
    """
    # Arrange

    class Sample:
        def __init__(self, value: int = 42):
            self.value = value

    mock = Mock()

    @async_lifecycle.cache("application")
    async def sample():
        mock()
        return Sample(2)

    async with async_lifecycle.start("application"):
        call_1 = await sample()
        call_2 = await sample()

    assert call_1 is call_2
    mock.assert_called_once()


async def test_decorator_on_async_generator(async_lifecycle: AsyncLifecycle):
    """
    Test registering an async generator using the decorator syntax.

    Given: A AsyncLifecycle instance and an async generator function
    When: The generator function is decorated for a specific scope
    Then: The generator should be registered for cleanup in that scope
    """
    # Arrange
    mock_cleanup = Mock()

    @async_lifecycle.cache("application")
    @async_resource
    async def sample_generator():
        try:
            yield "test"
        finally:
            mock_cleanup()

    async with async_lifecycle.start("application"):
        call_1 = await sample_generator()
        call_2 = await sample_generator()

    assert call_1 is call_2
    mock_cleanup.assert_called_once()
