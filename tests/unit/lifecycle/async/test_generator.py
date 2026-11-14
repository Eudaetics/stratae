"""Test suite for caching async generator functions: registration and cleanup."""

import asyncio
import re
from contextlib import suppress
from unittest.mock import Mock

import pytest

from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.scope import AsyncScope


class _AFailure(RuntimeError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("A Failure")


class _BFailure(KeyError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("B Failure")


class _CallerFailure(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("Caller Failure")


class _FirstFailure(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("first failure")


class _TestFailure(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("Test Failure")


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
    async def sample_generator():
        await asyncio.sleep(0)
        try:
            yield SimpleObject()
            mock_commit()
        finally:
            mock_cleanup()

    # Act
    async with async_scope:
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
    async def sample_generator():
        await asyncio.sleep(0)
        yield "test"

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match=re.escape("Scope 'application' is not active.")):
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
    async def sample_generator():
        try:
            yield SimpleObject()
            await asyncio.sleep(0)
        finally:
            mock_cleanup()
            raise _TestFailure()

    # Act & Assert
    async def act() -> None:
        async with async_scope:
            await sample_generator()
            mock_cleanup.assert_not_called()

    with pytest.raises(ValueError, match="Test Failure"):
        await act()


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
    async def act() -> None:
        async with async_scope:
            await sample_generator()
            mock_cleanup.assert_not_called()
            mock_failure.assert_not_called()
            mock_except.assert_not_called()

    with pytest.raises(ValueError, match="Test Failure"):
        await act()

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
    async def generator_one():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @async_scope.cache()
    async def generator_two():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @async_scope.cache()
    async def generator_three():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    async with async_scope:
        await generator_one()
        await generator_two()
        await generator_three()

    # Assert
    assert cleanup_order == ["third", "second", "first"]
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()


async def test_last_cleanup_failure_propagates_with_earlier_ones_chained_async(
    async_scope: AsyncScope,
):
    """
    Each cleanup failure should replace the one before it, which stays in its context chain.

    Given: A AsyncScope and three cached generator functions whose cleanups each raise
    When: The generators are cleaned up, last entered first
    Then: The first-entered resource's error is raised, with the second's as its context and
        the third's as the second's, and every cleanup runs
    """
    # Arrange
    mock_cleanup_1 = Mock(side_effect=ValueError("First Failure"))
    mock_cleanup_2 = Mock(side_effect=KeyError("Second Failure"))
    mock_cleanup_3 = Mock(side_effect=RuntimeError("Third Failure"))

    class SimpleObject: ...

    @async_scope.cache()
    async def generator_one():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @async_scope.cache()
    async def generator_two():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @async_scope.cache()
    async def generator_three():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    async def act() -> None:
        async with async_scope:
            await generator_one()
            await generator_two()
            await generator_three()

    with pytest.raises(ValueError, match="First Failure") as raised:
        await act()

    # Assert
    second = raised.value.__context__
    assert isinstance(second, KeyError)
    assert isinstance(second.__context__, RuntimeError)
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
    async def sample_generator():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
            mock_commit()
        except Exception:
            mock_rollback()
            raise

    # Act & Assert
    async def act() -> None:
        async with async_scope:
            await sample_generator()
            raise _CallerFailure()

    with pytest.raises(ValueError, match="Caller Failure"):
        await act()

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
    async def sample_generator():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except ValueError:
            mock_suppressed()

    # Act
    async with async_scope:
        await sample_generator()
        raise _CallerFailure()

    # Assert
    mock_suppressed.assert_called_once()


async def test_each_resource_error_replaces_the_one_before_async(async_scope: AsyncScope):
    """
    A resource raising its own error should replace the one it was given, keeping it chained.

    Given: A AsyncScope and two cached generators, each raising its own new error while
        handling whatever it's thrown
    When: Code inside the active scope's async with block raises after both are entered
    Then: The first-entered resource's error is raised, with the other's as its context and
        the caller's as that one's
    """

    # Arrange
    class SimpleObject: ...

    @async_scope.cache()
    async def resource_a():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except Exception:
            raise _AFailure() from None

    @async_scope.cache()
    async def resource_b():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except Exception:
            raise _BFailure() from None

    # Act
    async def act() -> None:
        async with async_scope:
            await resource_a()
            await resource_b()
            raise _CallerFailure()

    with pytest.raises(_AFailure) as raised:
        await act()

    # Assert
    b_failure = raised.value.__context__
    assert isinstance(b_failure, _BFailure)
    assert isinstance(b_failure.__context__, _CallerFailure)


async def test_resource_suppressing_a_later_cleanup_error_suppresses_the_activations_async(
    async_scope: AsyncScope,
):
    """
    A resource suppressing the error a later-entered resource raised should end the activation.

    Given: Two cached generators: the later raising its own error from the caller's, and the
        earlier suppressing whatever it's thrown
    When: Code inside the active scope's async with block raises after both are entered
    Then: Nothing propagates, and both resources closed
    """
    # Arrange
    closed: list[str] = []

    @async_scope.cache()
    async def resource_a():
        await asyncio.sleep(0)
        with suppress(Exception):
            yield "a"
        closed.append("a")

    @async_scope.cache()
    async def resource_b():
        await asyncio.sleep(0)
        try:
            yield "b"
        except Exception:
            closed.append("b")
            raise _BFailure() from None

    # Act
    async with async_scope:
        await resource_a()
        await resource_b()
        raise _CallerFailure()

    # Assert
    assert closed == ["b", "a"]


async def test_resource_reraising_callers_exception_propagates_it_async(async_scope: AsyncScope):
    """
    A resource re-raising the caller's exception should make it propagate again.

    Given: A AsyncScope and two cached generators, the later one raising its own new error and
        the earlier one re-raising the caller's exception in place of it
    When: Code inside the active scope's async with block raises after both are entered
    Then: The caller's exception is raised, with the later resource's error as its context
    """
    # Arrange
    caller_failure = _CallerFailure()

    class SimpleObject: ...

    @async_scope.cache()
    async def resource_a():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except Exception:
            raise caller_failure from None

    @async_scope.cache()
    async def resource_b():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except Exception:
            raise _BFailure() from None

    # Act
    async def act() -> None:
        async with async_scope:
            await resource_a()
            await resource_b()
            raise caller_failure

    with pytest.raises(_CallerFailure) as raised:
        await act()

    # Assert
    assert raised.value is caller_failure
    assert isinstance(caller_failure.__context__, _BFailure)


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
    async def resource_a():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
            mock_commit_a()
        except Exception:
            mock_rollback_a()
            raise

    @async_scope.cache()
    async def resource_b():
        try:
            await asyncio.sleep(0)
            yield SimpleObject()
        except ValueError:
            mock_swallowed_b()

    # Act
    async with async_scope:
        await resource_a()
        await resource_b()
        raise _CallerFailure()

    # Assert
    mock_swallowed_b.assert_called_once()
    mock_commit_a.assert_called_once()
    mock_rollback_a.assert_not_called()


class _Interrupt(BaseException):
    """A non-Exception error, standing in for cancellation or KeyboardInterrupt."""

    def __init__(self) -> None:
        super().__init__("cancelled during cleanup")


async def test_cleanup_raising_non_exception_still_closes_earlier_resources(
    async_scope: AsyncScope,
):
    """
    A cleanup raising a BaseException that isn't an Exception doesn't skip later cleanups.

    Given: Two cached resources, the second-entered of which raises a non-Exception
        BaseException while closing,
    When: The scope deactivates,
    Then: The first resource should still be closed, and the BaseException should propagate.
    """
    # Arrange
    closed: list[str] = []

    @async_scope.cache()
    async def first():
        await asyncio.sleep(0)
        try:
            yield "first"
        finally:
            closed.append("first")

    @async_scope.cache()
    async def second():
        await asyncio.sleep(0)
        yield "second"
        raise _Interrupt()

    # Act
    async def act() -> None:
        async with async_scope:
            await first()
            await second()

    with pytest.raises(_Interrupt):
        await act()

    # Assert
    assert closed == ["first"]


async def test_cleanup_error_replaces_a_base_exception_and_chains_it_async(async_scope: AsyncScope):
    """
    An error a cleanup raises while a BaseException propagates should replace it, as `with` does.

    Given: Two cached resources, the later raising a non-Exception BaseException while
        closing, and the earlier raising a ValueError from its cleanup
    When: The scope deactivates
    Then: The ValueError is raised, with the BaseException as its context
    """

    # Arrange
    @async_scope.cache()
    async def first():
        await asyncio.sleep(0)
        try:
            yield "first"
        finally:
            raise _FirstFailure()

    @async_scope.cache()
    async def second():
        await asyncio.sleep(0)
        yield "second"
        raise _Interrupt()

    # Act
    async def act() -> None:
        async with async_scope:
            await first()
            await second()

    with pytest.raises(_FirstFailure) as raised:
        await act()

    # Assert
    assert isinstance(raised.value.__context__, _Interrupt)
