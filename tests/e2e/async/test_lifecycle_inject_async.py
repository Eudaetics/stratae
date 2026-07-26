"""Tests for async lifecycle management with dependency injection."""

import asyncio
from typing import Annotated, AsyncGenerator
from unittest.mock import Mock

import pytest

from stratae.context import Context
from stratae.depends import Depends, inject
from stratae.lifecycle import AsyncLifecycle, async_resource


async def test_lifecycle_inject_async_gen(async_lifecycle: AsyncLifecycle):
    """
    Test lifecycle management with dependency injection using async generator dependencies.

    Given: An AsyncLifecycle with application scope.
    When: An async generator dependency is injected at application scope.
    Then: The async generator should yield the same instance within the same application scope.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_cleanup = Mock()
    mock_side_effect = Mock()

    @async_lifecycle.cache("application")
    @inject
    @async_resource
    async def get_resource(db: Annotated[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield db
            mock_side_effect()
        finally:
            mock_cleanup()

    # Act / Assert
    async with async_lifecycle.start("application"):
        resource_instance = await get_resource()
        assert isinstance(resource_instance, SimpleObject)
        assert resource_instance is await get_resource()
        mock_cleanup.assert_not_called()
    await asyncio.sleep(0.001)  # Allow cleanup to run
    mock_cleanup.assert_called_once()
    mock_side_effect.assert_called_once()


async def test_lifecycle_inject_nested_async_gen(async_lifecycle: AsyncLifecycle):
    """
    Test nested lifecycle management with dependency injection using async generator dependencies.

    Given: An AsyncLifecycle with application and request scopes.
    When: An async generator is injected at application scope and another at request scope.
    Then: The application scope async generator should yield the same instance across requests,
          while the request scope async generator should yield unique instances per request.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_app_cleanup = Mock()
    mock_request_cleanup = Mock()
    mock_side_effect = Mock()

    @async_lifecycle.cache("application")
    @inject
    @async_resource
    async def get_app_resource(resource: Annotated[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield resource
            mock_side_effect()
        finally:
            mock_app_cleanup()

    @async_lifecycle.cache("request")
    @inject
    @async_resource
    async def get_request_resource(
        resource: Annotated[SimpleObject, Depends(lambda: SimpleObject())],
    ):
        try:
            yield resource
        finally:
            mock_request_cleanup()

    # Act / Assert
    async with async_lifecycle.start("application"):
        app_resource_instance = await get_app_resource()
        assert isinstance(app_resource_instance, SimpleObject)

        async with async_lifecycle.start("request"):
            request_resource_instance_1 = await get_request_resource()
            assert isinstance(request_resource_instance_1, SimpleObject)
            assert await get_app_resource() is app_resource_instance
            assert await get_request_resource() is request_resource_instance_1
        async with async_lifecycle.start("request"):
            request_resource_instance_2 = await get_request_resource()
            assert isinstance(request_resource_instance_2, SimpleObject)
            assert await get_app_resource() is app_resource_instance
            assert await get_request_resource() is request_resource_instance_2
            assert request_resource_instance_1 is not request_resource_instance_2
    await asyncio.sleep(0.001)  # Allow cleanup to run
    assert mock_app_cleanup.call_count == 1
    assert mock_request_cleanup.call_count == 2
    assert mock_side_effect.call_count == 1


async def test_lifecycle_inject_async_with_exception(async_lifecycle: AsyncLifecycle):
    """
    Test using async dependencies that raise an exception.

    Given: An AsyncLifecycle with application scope.
    When: An async dependency is injected at application scope that raises an exception.
    Then: The exception should propagate correctly.
    """

    # Arrange
    class SimpleObject:
        pass

    @async_lifecycle.cache("application")
    @inject
    async def get_resource(_: Annotated[SimpleObject, Depends(lambda: SimpleObject())]):
        raise ValueError("Simulated exception")

    # Act / Assert
    async with async_lifecycle.start("application"):
        with pytest.raises(ValueError):
            await get_resource()


async def test_lifecycle_inject_async_gen_with_exception(async_lifecycle: AsyncLifecycle):
    """
    Test using async generator dependencies that raise an exception.

    Given: An AsyncLifecycle with application scope.
    When: An async generator dependency is injected at application scope that raises an exception.
    Then: The exception should propagate correctly.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_cleanup = Mock()

    @async_lifecycle.cache("application")
    @inject
    @async_resource
    async def get_resource(db: Annotated[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield db
        finally:
            mock_cleanup()
            raise ValueError("Simulated exception after yield")

    # Act / Assert
    with pytest.raises(ValueError, match="Simulated exception after yield"):
        async with async_lifecycle.start("application"):
            resource_instance = await get_resource()
            assert isinstance(resource_instance, SimpleObject)
    assert mock_cleanup.call_count == 1


async def test_lifecycle_inject_async_gen_with_multiple_exceptions(async_lifecycle: AsyncLifecycle):
    """
    Test using multiple sync generator dependencies that raise an exception.

    Given: An AsyncLifecycle with application scope.
    When: Generator dependencies are injected at application scope that raise an exception.
    Then: The exceptions should propagate correctly as an ExceptionGroup.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_cleanup = Mock()
    mock_side_effect = Mock()

    @async_lifecycle.cache("application")
    @inject
    @async_resource
    async def get_one(
        db: Annotated[SimpleObject, Depends(SimpleObject)],
    ) -> AsyncGenerator[SimpleObject, None]:
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
            mock_side_effect()
        finally:
            mock_cleanup()
            raise ValueError("Simulated exception after yield")

    @async_lifecycle.cache("application")
    @inject
    @async_resource
    async def get_two(
        db: Annotated[SimpleObject, Depends(SimpleObject)],
    ) -> AsyncGenerator[SimpleObject, None]:
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
            mock_side_effect()
        finally:
            mock_cleanup()
            raise AttributeError("Simulated exception after yield")

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exceptions:
        async with async_lifecycle.start("application"):
            resource_instance = await get_one()
            resource_instance_2 = await get_two()
            assert isinstance(resource_instance, SimpleObject)
            assert isinstance(resource_instance_2, SimpleObject)

    assert len(exceptions.value.exceptions) == 2
    assert any(isinstance(e, ValueError) for e in exceptions.value.exceptions)
    assert any(isinstance(e, AttributeError) for e in exceptions.value.exceptions)


async def test_async_lifecycle_outer_cache_and_context_change(async_lifecycle: AsyncLifecycle):
    """
    Test async lifecycle caching when cache is the outer decorator and injected values change.

    The order of the decorators for caching and dependency injection is important. When the cache
    decorator is the outermost decorator, the cache key generation happens before dependency
    injection. If a dependency returns a value that changes, the cache will not reflect that
    change unless the cache key generation accounts for it.

    Given: A function with a cache decorator outside of dependency injection.
    When: A dependency is injected with different argument values.
    Then: The first cached value should be returned regardless of value change.
    """
    # Arrange
    mock = Mock()

    @async_lifecycle.cache("application")
    @inject
    async def get_value(x: Annotated[int, Depends(lambda: mock.call_count)]) -> int:
        mock()
        return x + 1

    # Act / Assert
    async with async_lifecycle.start("application"):
        assert await get_value() == 1
        assert await get_value() == 1


async def test_async_lifecycle_outer_cache_inject_change_with_custom_key(
    async_lifecycle: AsyncLifecycle,
):
    """
    Test async lifecycle caching with custom cache key and injection changes.

    The order of the decorators for caching and dependency injection is important. When the cache
    decorator is the outermost decorator, the cache key generation happens before dependency
    injection. If a dependency is based on a value that changes, the cache key generation
    must account for it to reflect those changes.

    Given: A function with a cache decorator outside of injection using a custom cache key.
    When: A dependency is injected with different argument values.
    Then: The cached value should reflect the context changes based on the custom cache key.
    """
    # Arrange
    mock = Mock()

    @async_lifecycle.cache("application", cache_key=lambda: mock.call_count)
    @inject
    async def get_value(x: Annotated[int, Depends(lambda: mock.call_count)]) -> int:
        mock()
        return x + 1

    # Act / Assert
    async with async_lifecycle.start("application"):
        assert await get_value() == 1
        assert await get_value() == 2


async def test_async_lifecycle_inner_cache_inject(async_lifecycle: AsyncLifecycle):
    """
    Test async lifecycle caching when cache is the inner decorator and injected values change.

    The order of the decorators for caching and dependency injection is important. When the cache
    decorator is the innermost decorator, the cache key generation happens after dependency
    injection. If a dependency returns a value that changes, the cache will reflect that
    change.

    Given: A function with a cache decorator inside of dependency injection.
    When: A dependency is injected with different argument values.
    Then: The cached value should reflect the context changes.
    """
    # Arrange
    x = Context[int]("x")
    counter = Mock()

    @inject
    @async_lifecycle.cache("application")
    async def get_value(x: Annotated[int, Depends(x)]) -> int:
        counter()
        return x * 2

    # Act / Assert
    async with async_lifecycle.start("application"):
        with x.use(10):
            assert await get_value() == 20
            assert await get_value() == 20
            assert counter.call_count == 1
        with x.use(20):
            assert await get_value() == 40
            assert counter.call_count == 2
