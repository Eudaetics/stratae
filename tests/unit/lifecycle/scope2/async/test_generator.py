"""Test suite for async generator (@async_resource) registration and cleanup."""

import asyncio
from unittest.mock import Mock

import pytest

from stratae.lifecycle._scope2 import AsyncScope
from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.resource import async_resource


async def test_generator_async(async_scope: AsyncScope):
    """
    Test using an async generator via the decorator syntax.

    Given: An AsyncScope and an async generator,
    When: The generator is cached in that scope,
    Then: The generator should yield the expected value in that scope and clean up at end.
    """
    # Arrange
    mock_commit = Mock()
    mock_cleanup = Mock()

    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def sample_generator():
        try:
            yield SimpleObject()
            mock_commit()
        finally:
            mock_cleanup()

    # Act
    async with async_scope.activate():
        result1 = await sample_generator()
        result2 = await sample_generator()

        mock_commit.assert_not_called()
        mock_cleanup.assert_not_called()

    assert isinstance(result1, SimpleObject)
    assert result1 is result2
    mock_commit.assert_called_once()
    mock_cleanup.assert_called_once()


async def test_register_generator_inactive_scope_async(async_scope: AsyncScope):
    """
    Test using an async generator with an inactive scope.

    Given: An AsyncScope and a cached generator,
    When: An attempt is made to use the generator before its scope is active,
    Then: A ScopeInactiveError should be raised.
    """

    # Arrange
    @async_scope.cache()
    @async_resource
    async def sample_generator():
        yield "test"

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="Scope 'application' is not active."):
        await sample_generator()


async def test_decorate_generator_async_exception_cleanup(async_scope: AsyncScope):
    """
    Test using an asynchronous generator that raises an exception during cleanup.

    Given: An AsyncScope and a cached async generator function,
    When: The decorated function raises an exception in cleanup,
    Then: The exception should be propagated correctly.
    """
    # Arrange
    mock_cleanup = Mock()

    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def sample_generator():
        try:
            yield SimpleObject()
            await asyncio.sleep(0)
        finally:
            mock_cleanup()
            raise ValueError("Test Failure")

    # Act & Assert
    with pytest.raises(ValueError, match="Test Failure"):  # noqa: S5778
        async with async_scope.activate():
            await sample_generator()
            mock_cleanup.assert_not_called()


async def test_decorator_generator_async_exception_handling(async_scope: AsyncScope):
    """
    Test using an asynchronous generator with an exception block.

    Given: An AsyncScope and a cached async generator function,
    When: The decorated function raises an exception in the try block,
    Then: the exception should be propagated and the cleanup function should be called.
    """
    # Arrange
    mock_cleanup = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    mock_except = Mock()

    class SimpleObject: ...

    @async_scope.cache()
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
    with pytest.raises(ValueError, match="Test Failure"):  # noqa: S5778
        async with async_scope.activate():
            await sample_generator()
            mock_cleanup.assert_not_called()
            mock_failure.assert_not_called()
            mock_except.assert_not_called()

    mock_except.assert_called_once()
    mock_cleanup.assert_called_once()


async def test_multiple_generators_async(async_scope: AsyncScope):
    """
    Test registering multiple async generators for cleanup.

    Given: An AsyncScope and multiple cached async generator functions,
    When: The generator functions are cached in that scope,
    Then: All generators should be cleaned up in the correct (LIFO) order.
    """
    # Arrange
    cleanup_order: list[str] = []
    mock_cleanup_1 = Mock(side_effect=lambda: cleanup_order.append("first"))
    mock_cleanup_2 = Mock(side_effect=lambda: cleanup_order.append("second"))
    mock_cleanup_3 = Mock(side_effect=lambda: cleanup_order.append("third"))

    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def generator_one():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @async_scope.cache()
    @async_resource
    async def generator_two():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @async_scope.cache()
    @async_resource
    async def generator_three():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    async with async_scope.activate():
        await generator_one()
        await generator_two()
        await generator_three()

    # Assert
    assert cleanup_order == ["third", "second", "first"]
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()


async def test_generator_async_exception_group(async_scope: AsyncScope):
    """
    Test that multiple exceptions during generator cleanup are collected into an ExceptionGroup.

    Given: An AsyncScope and multiple cached generator functions that raise exceptions,
    When: The generators are cleaned up,
    Then: An ExceptionGroup containing all exceptions should be raised.
    """
    # Arrange
    mock_cleanup_1 = Mock(side_effect=ValueError("First Failure"))
    mock_cleanup_2 = Mock(side_effect=KeyError("Second Failure"))
    mock_cleanup_3 = Mock(side_effect=RuntimeError("Third Failure"))

    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def generator_one():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @async_scope.cache()
    @async_resource
    async def generator_two():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @async_scope.cache()
    @async_resource
    async def generator_three():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act & Assert
    with pytest.raises(ExceptionGroup) as exceptions:  # noqa: S5778
        async with async_scope.activate():
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


async def test_generator_sees_exception_raised_in_caller_block_async(async_scope: AsyncScope):
    """
    Test that an exception raised by the caller, not the generator, still reaches it.

    Given: An AsyncScope and a cached async generator using try/except around its yield
        to distinguish success from failure,
    When: Code inside the active scope's async with block raises after the resource has
        already been entered, rather than the generator's own body raising,
    Then: The exception should be delivered to the generator at its yield point, so the
        except branch runs, and the same exception should still propagate.
    """
    # Arrange
    mock_commit = Mock()
    mock_rollback = Mock()

    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def sample_generator():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
            mock_commit()
        except Exception:
            mock_rollback()
            raise

    # Act & Assert
    with pytest.raises(ValueError, match="Caller Failure"):  # noqa: S5778
        async with async_scope.activate():
            await sample_generator()
            raise ValueError("Caller Failure")

    mock_rollback.assert_called_once()
    mock_commit.assert_not_called()


async def test_generator_can_suppress_exception_raised_in_caller_block_async(
    async_scope: AsyncScope,
):
    """
    Test that an async generator can suppress an exception raised in the caller's block.

    Given: An AsyncScope and a cached async generator that catches and swallows, rather
        than re-raising, an exception raised after its yield,
    When: Code inside the active scope's async with block raises after the resource has
        already been entered,
    Then: The exception should be suppressed - the async with block should exit normally.
    """
    # Arrange
    mock_suppressed = Mock()

    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def sample_generator():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except ValueError:
            mock_suppressed()

    # Act
    async with async_scope.activate():
        await sample_generator()
        raise ValueError("Caller Failure")

    # Assert
    mock_suppressed.assert_called_once()


async def test_multiple_generators_each_raise_from_callers_exception_async(
    async_scope: AsyncScope,
):
    """
    Test that resources each raising their own exception from the caller's are grouped.

    Given: An AsyncScope and two cached async generators, each of which raises its own
        new exception while handling whatever it's thrown, rather than re-raising it
        unchanged,
    When: Code inside the active scope's async with block raises after both resources
        have been entered,
    Then: An ExceptionGroup containing the caller's original exception plus each
        resource's own exception should be raised, chained in close (LIFO) order.
    """

    # Arrange
    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def resource_a():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except Exception:
            raise RuntimeError("A Failure") from None

    @async_scope.cache()
    @async_resource
    async def resource_b():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except Exception:
            raise KeyError("B Failure") from None

    # Act & Assert
    with pytest.raises(ExceptionGroup) as exceptions:  # noqa: S5778
        async with async_scope.activate():
            await resource_a()
            await resource_b()
            raise ValueError("Caller Failure")

    assert len(exceptions.value.exceptions) == 3
    assert any(
        isinstance(exc, ValueError) and str(exc) == "Caller Failure"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, RuntimeError) and str(exc) == "A Failure"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, KeyError) and str(exc) == "'B Failure'"
        for exc in exceptions.value.exceptions
    )


async def test_resource_swallowing_exception_hides_it_from_earlier_resources_async(
    async_scope: AsyncScope,
):
    """
    Test that a resource swallowing the caller's exception hides it from earlier resources.

    Given: An AsyncScope and two cached async generators - resource_a entered first,
        resource_b entered second, where resource_b catches and swallows the caller's
        exception instead of re-raising it,
    When: Code inside the active scope's async with block raises after both resources
        have been entered,
    Then: resource_b's swallow should run, resource_a should see a clean close (its
        success path, not its except branch) since it closes after resource_b in LIFO
        order, and the exception should not propagate out of the async with block at all.
    """
    # Arrange
    mock_commit_a = Mock()
    mock_rollback_a = Mock()
    mock_swallowed_b = Mock()

    class SimpleObject: ...

    @async_scope.cache()
    @async_resource
    async def resource_a():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
            mock_commit_a()
        except Exception:
            mock_rollback_a()
            raise

    @async_scope.cache()
    @async_resource
    async def resource_b():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except ValueError:
            mock_swallowed_b()

    # Act
    async with async_scope.activate():
        await resource_a()
        await resource_b()
        raise ValueError("Caller Failure")

    # Assert
    mock_swallowed_b.assert_called_once()
    mock_commit_a.assert_called_once()
    mock_rollback_a.assert_not_called()
