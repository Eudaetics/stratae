"""Test inject with generators."""

import asyncio
from typing import Any, AsyncGenerator
from unittest.mock import Mock

import pytest

from stratae.depends import Depends, inject
from stratae.depends.exceptions import OverrideNotAllowedError


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
    async def gen_func(dep: int = Depends(lambda: 5)):
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
    async def func_with_gen(gen: AsyncGenerator[int, Any] = Depends(gen_dep)):
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
    async def outer_gen(inner: AsyncGenerator[str, Any] = Depends(inner_gen)):
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
    async def gen_func_with_args(count: int, dep: int = Depends(lambda: 3)):
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
    async def gen_func_with_kwargs(*, count: int = 4, dep: int = Depends(lambda: 2)):
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
        count: int, *, start: int = 0, dep: int = Depends(lambda: 3)
    ):
        for i in range(start, count + start):
            yield i * dep

    generator = gen_func_with_mixed_args(3, start=1)

    # Assert
    assert [x async for x in generator] == [3, 6, 9]


async def test_inject_async_generator_dependency_override():
    """
    Test overriding dependency on an async generator during injection.

    Given: an async generator function injected with a dependency
    When: the function is called with an overridden dependency
    Then: the injection should use the overridden dependency correctly.
    """

    # Arrange
    def original():
        return "original"

    def override():
        return "override"

    @inject
    async def gen_func(val: str = Depends(original)):
        await asyncio.sleep(0)
        for i in range(2):
            yield f"{val}-{i}"

    # Act
    result: list[str] = []
    async for item in gen_func(val=override()):
        result.append(item)

    # Assert
    assert result == ["override-0", "override-1"]


async def test_inject_async_generator_dependency_override_false():
    """
    Test disabling overriding dependencies on an async generator during injection.

    Given: an async generator function injected with a dependency
    When: the dependency is set to not allow override
    Then: attempting to override the dependency should raise a RegistrationError.
    """

    # Arrange
    def original():
        return "original"

    def override():
        return "override"

    @inject
    async def gen_func(val: str = Depends(original, allow_override=False)):
        await asyncio.sleep(0)
        for item in range(2):
            yield f"{val}-{item}"

    # Act & Assert
    with pytest.raises(
        OverrideNotAllowedError, match="Overriding these dependencies is not allowed: val"
    ):
        await anext(gen_func(val=override()))
