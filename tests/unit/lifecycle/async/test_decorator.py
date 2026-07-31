"""Tests for the AsyncScope.cache decorator syntax across sync/async factories and generators."""

from unittest.mock import Mock

from stratae.lifecycle.resource import async_resource, resource
from stratae.lifecycle.scope import AsyncScope


async def test_decorator_on_sync(async_scope: AsyncScope):
    """
    Test caching a sync factory using the decorator syntax.

    Given: An AsyncScope and a sync factory function,
    When: The factory function is cached in that scope,
    Then: The factory should only get called once.
    """
    # Arrange

    class Sample:
        def __init__(self, value: int = 42):
            self.value = value

    mock = Mock()

    @async_scope.cache()
    def sample():
        mock()
        return Sample(2)

    async with async_scope.activate():
        call_1 = sample()
        call_2 = sample()

    assert call_1 is call_2
    mock.assert_called_once()


async def test_decorator_on_sync_generator(async_scope: AsyncScope):
    """
    Test caching a sync generator (@resource) using the decorator syntax.

    Given: An AsyncScope and a sync generator function,
    When: The generator function is cached in that scope,
    Then: The generator should be registered for cleanup in that scope.
    """
    # Arrange
    mock_cleanup = Mock()

    @async_scope.cache()
    @resource
    def sample_generator():
        try:
            yield "test"
        finally:
            mock_cleanup()

    async with async_scope.activate():
        call_1 = sample_generator()
        call_2 = sample_generator()

    assert call_1 is call_2
    mock_cleanup.assert_called_once()


async def test_decorator_on_async(async_scope: AsyncScope):
    """
    Test caching an async factory using the decorator syntax.

    Given: An AsyncScope and an async factory function,
    When: The factory function is cached in that scope,
    Then: The factory should only get called once.
    """
    # Arrange

    class Sample:
        def __init__(self, value: int = 42):
            self.value = value

    mock = Mock()

    @async_scope.cache()
    async def sample():
        mock()
        return Sample(2)

    async with async_scope.activate():
        call_1 = await sample()
        call_2 = await sample()

    assert call_1 is call_2
    mock.assert_called_once()


async def test_decorator_on_async_generator(async_scope: AsyncScope):
    """
    Test caching an async generator (@async_resource) using the decorator syntax.

    Given: An AsyncScope and an async generator function,
    When: The generator function is cached in that scope,
    Then: The generator should be registered for cleanup in that scope.
    """
    # Arrange
    mock_cleanup = Mock()

    @async_scope.cache()
    @async_resource
    async def sample_generator():
        try:
            yield "test"
        finally:
            mock_cleanup()

    async with async_scope.activate():
        call_1 = await sample_generator()
        call_2 = await sample_generator()

    assert call_1 is call_2
    mock_cleanup.assert_called_once()
