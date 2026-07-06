"""Test suite for the DependsWrapper class in the dependency injection module."""

import asyncio

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
