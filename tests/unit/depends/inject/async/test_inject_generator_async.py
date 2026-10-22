"""Test inject with generators."""

import inspect
from contextlib import asynccontextmanager
from typing import Annotated, Any, AsyncGenerator, Callable
from unittest.mock import Mock

import pytest

from stratae.depends import Depends, inject


def _sync_number() -> int:
    return 1


async def _async_number() -> int:
    return 1


with_each_dep_kind = pytest.mark.parametrize(
    "provider", [_sync_number, _async_number], ids=["sync_dep", "async_dep"]
)


async def test_inject_on_async_generator():
    """
    Test injection on an async generator function.

    Given: an async generator function that is decorated with inject
    When: the function is called
    Then: the injection should work correctly within the generator.
    """
    # Arrange
    mock_cleanup = Mock()

    # Act (the decorator)
    @inject
    async def gen_func(dep: Annotated[int, Depends(lambda: 5)]):
        """Inject into a function that returns a generator."""
        for i in range(dep):
            yield i
        mock_cleanup()

    generator = gen_func()

    # Assert
    mock_cleanup.assert_not_called()
    assert [x async for x in generator] == [0, 1, 2, 3, 4]

    try:
        await anext(generator)
    except StopAsyncIteration:
        pass
    mock_cleanup.assert_called_once()


async def test_inject_generator_dep():
    """
    Test injection of an async generator function as a dependency.

    Given: an async generator function used as a dependency
    When: another function is injected with this generator dependency
    Then: the injection should work correctly and the generator should be used.
    """
    # Arrange
    mock_cleanup = Mock()

    async def gen_dep():
        """Create an async generator dependency."""
        for i in range(3):
            yield i
        mock_cleanup()

    @inject
    async def func_with_gen(gen: Annotated[AsyncGenerator[int, Any], Depends(gen_dep)]):
        """Test function that uses the generator dependency."""
        return [x async for x in gen]

    # Act
    result = await func_with_gen()

    # Assert
    assert result == [0, 1, 2]
    mock_cleanup.assert_called_once()


async def test_inject_nested_async_generators():
    """
    Test nested async generator dependencies with injection.

    Given: an async generator dependency that itself depends on another async generator
    When: a function is injected with the nested generator dependency
    Then: the injection should work correctly through the nested generators.
    """

    # Arrange
    async def inner_gen():
        """Inner async generator dependency."""
        for i in range(2):
            yield f"inner-{i}"

    @inject
    async def outer_gen(inner: Annotated[AsyncGenerator[str, Any], Depends(inner_gen)]):
        """Outer async generator that depends on inner generator."""
        async for item in inner:
            yield f"outer-{item}"

    # Act
    result: list[str] = []
    async for x in outer_gen():
        result.append(x)

    # Assert
    assert result == ["outer-inner-0", "outer-inner-1"]


async def test_inject_on_async_generator_with_args():
    """
    Test injection on an async generator function that takes arguments.

    Given: an async generator function with parameters, decorated with inject
    When: the function is called with arguments
    Then: the injection should work correctly alongside the provided arguments.
    """

    # Act (the decorator)
    @inject
    async def gen_func_with_args(count: int, dep: Annotated[int, Depends(lambda: 3)]):
        """Inject into a function that returns a generator and takes args."""
        for i in range(min(count, dep)):
            yield i

    generator = gen_func_with_args(5)

    # Assert
    assert [x async for x in generator] == [0, 1, 2]


async def test_inject_async_generator_with_kwargs():
    """
    Test injection on an async generator function that takes keyword arguments.

    Given: an async generator function with keyword parameters, decorated with inject
    When: the function is called with keyword arguments
    Then: the injection should work correctly alongside the provided keyword arguments.
    """

    # Act (the decorator)
    @inject
    async def gen_func_with_kwargs(*, count: int = 4, dep: Annotated[int, Depends(lambda: 2)]):
        """Inject into a function that returns a generator and takes kwargs."""
        for i in range(count):
            yield i + dep

    generator = gen_func_with_kwargs(count=5)

    # Assert
    assert [x async for x in generator] == [2, 3, 4, 5, 6]


async def test_inject_async_generator_with_mixed_args():
    """
    Test injection on an async generator function that takes mixed arguments.

    Given: an async generator function with both positional and keyword parameters,
           decorated with inject
    When: the function is called with a mix of arguments
    Then: the injection should work correctly alongside the provided arguments.
    """

    # Act (the decorator)
    @inject
    async def gen_func_with_mixed_args(
        count: int, *, start: int = 0, dep: Annotated[int, Depends(lambda: 3)]
    ):
        for i in range(start, count + start):
            yield i * dep

    generator = gen_func_with_mixed_args(3, start=1)

    # Assert
    assert [x async for x in generator] == [3, 6, 9]


@with_each_dep_kind
def test_injected_async_generator_is_an_async_generator_function(provider: Callable[[], Any]):
    """
    An injected async generator function is still an async generator function.

    Given: An async generator function with an injected parameter
    When: inject is applied
    Then: inspect.isasyncgenfunction reports the wrapper as one, whatever the dependency
    """

    # Act
    @inject
    async def gen(n: Annotated[int, Depends(provider)]):
        yield n

    # Assert
    assert inspect.isasyncgenfunction(gen)


@with_each_dep_kind
async def test_exception_in_asynccontextmanager_body_reaches_generator(
    provider: Callable[[], Any],
):
    """
    An exception raised in an asynccontextmanager's body is thrown into the generator.

    Given: An asynccontextmanager over an injected async generator that rolls back on error
    When: The body of its async with block raises
    Then: The generator's except block sees the exception before its finally runs
    """
    # Arrange
    events: list[str] = []

    @asynccontextmanager
    @inject
    async def session(n: Annotated[int, Depends(provider)]):
        try:
            yield n
            events.append("commit")
        except ValueError:
            events.append("rollback")
            raise
        finally:
            events.append("close")

    # Act
    with pytest.raises(ValueError, match="boom"):
        async with session():
            raise ValueError("boom")

    # Assert
    assert events == ["rollback", "close"]


@with_each_dep_kind
async def test_sent_values_reach_generator(provider: Callable[[], Any]):
    """
    Values sent into the wrapper are sent into the injected generator.

    Given: An injected async generator that yields back what it's sent
    When: Values are sent with asend
    Then: Each is received by the generator
    """

    # Arrange
    @inject
    async def echo(n: Annotated[int, Depends(provider)]) -> AsyncGenerator[object, object]:
        received: object = yield n
        while True:
            received = yield received

    gen = echo()

    # Act
    first = await anext(gen)
    echoed = [await gen.asend("a"), await gen.asend("b")]
    await gen.aclose()

    # Assert
    assert first == 1
    assert echoed == ["a", "b"]


@with_each_dep_kind
async def test_aclose_closes_generator(provider: Callable[[], Any]):
    """
    Closing the wrapper closes the injected generator at once.

    Given: An injected async generator suspended at a yield, with a finally block
    When: The wrapper is closed with aclose
    Then: The generator's finally block has run by the time aclose returns
    """
    # Arrange
    closed = Mock()

    @inject
    async def gen(n: Annotated[int, Depends(provider)]):
        try:
            yield n
        finally:
            closed()

    wrapper = gen()
    await anext(wrapper)

    # Act
    await wrapper.aclose()

    # Assert
    closed.assert_called_once()


@with_each_dep_kind
async def test_generator_handling_a_thrown_exception_keeps_yielding(
    provider: Callable[[], Any],
):
    """
    A generator that handles an exception thrown into it can go on yielding, then finish.

    Given: An injected async generator that yields a fallback when a ValueError is thrown in
    When: A ValueError is thrown in with athrow, then the generator is advanced to its end
    Then: athrow returns the fallback, and iteration then stops normally
    """

    # Arrange
    @inject
    async def gen(n: Annotated[int, Depends(provider)]):
        try:
            yield n
        except ValueError:
            yield "fallback"

    wrapper = gen()
    await anext(wrapper)

    # Act
    recovered = await wrapper.athrow(ValueError("boom"))

    # Assert
    assert recovered == "fallback"
    with pytest.raises(StopAsyncIteration):
        await anext(wrapper)
