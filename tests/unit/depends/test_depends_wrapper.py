"""Test suite for the DependsWrapper class in the dependency injection module."""

import asyncio
from functools import wraps
from inspect import unwrap
from typing import Any, Callable

from stratae.depends import DependsWrapper


def test_depends_wrapper_initialization():
    """
    Verify that DependsWrapper can be initialized with a dependency.

    Given: a dependency,
    When: DependsWrapper is initialized with that dependency,
    Then: it should store the dependency correctly.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    # Act
    depends = DependsWrapper(sample_dependency)

    # Assert
    assert depends.dependency == sample_dependency


def test_depends_wrapper_call():
    """
    Verify that DependsWrapper can be called as a function.

    Given: a DependsWrapper instance,
    When: it is called as a function,
    Then: it should return the result of the dependency.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    depends = DependsWrapper(sample_dependency)

    # Act
    result = depends.provide()

    # Assert
    assert result == "sample"


def test_depends_wrapper_with_lambda():
    """
    Verify that DependsWrapper can be initialized with a lambda function.

    Given: a lambda function as a dependency,
    When: DependsWrapper is initialized with it,
    Then: it should store the lambda function correctly.
    """
    # Arrange
    sample_lambda = lambda: "sample"  # noqa: E731

    # Act
    depends = DependsWrapper(sample_lambda)

    # Assert
    assert depends.dependency == sample_lambda


def test_sync_depends_is_async():
    """
    Verify the is_async property on a DependsWrapper is False.

    Given: a DependsWrapper instance,
    When: its is_async property is accessed,
    Then: it should return False.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    depends = DependsWrapper(sample_dependency)

    # Act
    result = depends.is_async

    # Assert
    assert result is False


def test_async_depends_is_async():
    """
    Verify the is_async property on an ADependsWrapper is True.

    Given: an ADependsWrapper instance,
    When: its is_async property is accessed,
    Then: it should return True.
    """

    # Arrange
    async def sample_dependency():
        await asyncio.sleep(0)
        return "sample"

    depends = DependsWrapper(sample_dependency)

    # Act
    result = depends.is_async

    # Assert
    assert result is True


async def test_depends_wrapper_call_with_coroutine():
    """
    Verify that DependsWrapper can handle coroutine dependencies.

    Given: a coroutine dependency,
    When: DependsWrapper is called,
    Then: it should return the result of the coroutine.
    """

    # Arrange
    async def sample_dependency():
        await asyncio.sleep(0)
        return "sample"

    depends = DependsWrapper(sample_dependency)

    # Act
    result = await depends.aprovide()

    # Assert
    assert result == "sample"


def test_depends_wrapper_call_multiple_times():
    """
    Verify that DependsWrapper can be called multiple times.

    Given: a DependsWrapper instance,
    When: it is called multiple times,
    Then: it should return the correct result each time.
    """
    # Arrange
    call_count = 0

    def sample_dependency():
        nonlocal call_count
        call_count += 1
        return f"sample {call_count}"

    depends = DependsWrapper(sample_dependency)

    # Act & Assert
    result1 = depends.provide()
    assert result1 == "sample 1"

    result2 = depends.provide()
    assert result2 == "sample 2"

    result3 = depends.provide()
    assert result3 == "sample 3"


def test_depends_wrapper_outermost_fixing():
    """
    Verify that DependsWrapper fixes the outermost dependency.

    Given: a DependsWrapper instance with a wrapped dependency,
    When: it is called,
    Then: it should use the outermost version of the dependency.
    """

    # Arrange
    def wrapper(func: Callable[[], str]) -> Callable[[], str]:
        @wraps(func)
        def gen_wrapper() -> str:
            return f"{func()} wrapped"

        original = unwrap(func)
        original.__outermost__ = gen_wrapper
        return gen_wrapper

    def dependency() -> str:
        return "inner"

    depends = DependsWrapper(dependency)
    dependency = wrapper(dependency)

    # Act
    result = depends.provide()

    # Assert
    assert result == "inner wrapped"


def test_depends_wrapper_without_outermost():
    """
    Verify that DependsWrapper works correctly when there is no outermost dependency.

    Given: a DependsWrapper instance with a normal dependency,
    When: it is called,
    Then: it should return the result of the original dependency.
    """

    # Arrange
    def wrapper(func: Callable[[], str]) -> Callable[[], str]:
        @wraps(func)
        def gen_wrapper() -> str:
            return f"{func()} wrapped"

        return gen_wrapper

    def dependency() -> str:
        return "normal"

    depends = DependsWrapper(dependency)
    dependency = wrapper(dependency)

    # Act
    result = depends.provide()
    wrapped_result = dependency()

    # Assert
    assert result == "normal"
    assert wrapped_result == "normal wrapped"


async def test_depends_wrapper_async_outermost_fixing():
    """
    Verify that DependsWrapper fixes the outermost async dependency.

    Given: a DependsWrapper instance with a wrapped async dependency,
    When: it is called,
    Then: it should use the outermost version of the async dependency.
    """

    # Arrange
    def wrapper(func: Callable[[], Any]) -> Callable[[], Any]:
        @wraps(func)
        async def gen_wrapper() -> str:
            inner = await func()
            return f"{inner} wrapped"

        original = unwrap(func)
        original.__outermost__ = gen_wrapper
        return gen_wrapper

    async def dependency() -> str:
        await asyncio.sleep(0)
        return "inner"

    depends = DependsWrapper(dependency)
    dependency = wrapper(dependency)

    # Act
    result = await depends.aprovide()

    # Assert
    assert result == "inner wrapped"


async def test_depends_wrapper_async_without_outermost():
    """
    Verify that DependsWrapper works correctly when there is no outermost async dependency.

    Given: a DependsWrapper instance with a normal async dependency,
    When: it is called,
    Then: it should return the result of the original async dependency.
    """

    # Arrange
    def wrapper(func: Callable[[], Any]) -> Callable[[], Any]:
        @wraps(func)
        async def gen_wrapper() -> str:
            inner = await func()
            return f"{inner} wrapped"

        return gen_wrapper

    async def dependency() -> str:
        await asyncio.sleep(0)
        return "normal"

    depends = DependsWrapper(dependency)
    dependency = wrapper(dependency)

    # Act
    result = await depends.aprovide()
    wrapped_result = await dependency()

    # Assert
    assert result == "normal"
    assert wrapped_result == "normal wrapped"
