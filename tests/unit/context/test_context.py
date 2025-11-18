"""Test suite for context variable wrapper."""

import asyncio
from contextvars import Token

import pytest

from stratae.context import Context


def test_context_set():
    """
    Verify the assignment of a value to the context variable.

    Given: A context variable instance.
    When: A value is set to the context variable.
    Then: The context variable should return the assigned value, and a valid token returned.
    """
    # Arrange
    test = Context[int]("test")

    # Act
    token = test.set(5)

    # Assert
    assert test() == 5
    assert isinstance(token, Token)


def test_context_get():
    """
    Verify retrieval of the context variable's value.

    Given: A context variable instance with a set value.
    When: The value is retrieved.
    Then: The retrieved value should match the set value.
    """
    # Arrange
    test = Context[int]("test")
    test.set(2)

    # Act
    value = test.get()

    # Assert
    assert value == 2


def test_context_get_default():
    """
    Verify retrieval of the context variable's value with a default.

    Given: A context variable instance without a set value.
    When: The value is retrieved with a default.
    Then: The retrieved value should match the default value.
    """
    # Arrange
    test = Context[int]("test")

    # Act
    value = test.get(default=1)

    # Assert
    assert value == 1


def test_context_get_unset_raises():
    """
    Verify that accessing an unset context variable raises an error.

    Given: A context variable instance without a set value.
    When: The value is accessed directly.
    Then: A RuntimeError should be raised indicating the context is not set.
    """
    # Arrange
    test = Context[float]("test")

    # Act / Assert
    with pytest.raises(RuntimeError, match="Context 'test' is not set."):
        test()


def test_context_reset():
    """
    Verify resetting the context variable to a previous state.

    Given: A context variable instance with a set value.
    When: The context variable is reset using a token.
    Then: The context variable should revert to its previous state.
    """
    # Arrange
    test = Context[int]("test")
    test.set(1)
    token = test.set(0)

    # Act
    test.reset(token)

    # Assert
    assert test.get() == 1


def test_context_reset_to_unset():
    """
    Verify resetting the context variable to an unset state.

    Given: A context variable instance with a set value.
    When: The context variable is reset to an unset state using a token.
    Then: Accessing the context variable should raise a RuntimeError.
    """
    # Arrange
    test = Context[str]("test")
    token = test.set("value")

    # Act
    test.reset(token)

    # Assert
    with pytest.raises(RuntimeError, match="Context 'test' is not set."):
        test()


def test_context_manager():
    """
    Verify the context manager functionality for setting and resetting context values.

    Given: A context variable instance.
    When: A value is set using the context manager.
    Then: The context variable should return the set value within the context,
            and revert to the previous state after exiting the context.
    """
    # Arrange
    test = Context[int]("test")
    test.set(1)

    # Act / Assert
    with test.use(5):
        assert test() == 5

    assert test() == 1


def test_context_manager_exception():
    """
    Verify that the context manager resets the context value even when an exception occurs.

    Given: A context variable instance.
    When: An exception is raised within the context manager.
    Then: The context variable should revert to the previous state after exiting the context.
    """
    # Arrange
    test = Context[int]("test")
    test.set(1)

    # Act / Assert
    with pytest.raises(ValueError):
        with test.use(0):
            assert test() == 0
            raise ValueError("test")

    assert test() == 1


def test_nested_contexts():
    """
    Verify nested context managers work correctly.

    Given: Nested context variable instances.
    When: Different contexts are used to set different values.
    Then: The context variable should return the correct value within each context,
            and revert to the previous state after exiting each context.
    """
    # Arrange
    test = Context[int]("test")

    # Act / Assert
    with test.use(1):
        assert test() == 1
        with test.use(2):
            assert test() == 2
        assert test() == 1
    assert test.get(default=-1) == -1


def test_context_isolation():
    """
    Verify contexts are isolated across different instances.

    Given: Two separate context variable instances.
    When: Different values are set in each context.
    Then: Each context variable should return its own value without interference.
    """
    # Arrange
    test_a = Context[int]("test")
    test_b = Context[int]("test")

    # Act
    test_a.set(1)
    test_b.set(2)

    # Assert
    assert test_a() == 1
    assert test_b() == 2


async def test_context_isolation_async():
    """
    Verify contexts are isolated across async tasks.

    Given: A context variable instance.
    When: Different values are set in separate async tasks.
    Then: Each task should see its own context value without interference.
    """
    # Arrange
    test = Context[int]("test")

    async def task_a():
        test.set(1)
        await asyncio.sleep(0.001)
        assert test() == 1

    async def task_b():
        test.set(2)
        await asyncio.sleep(0.001)
        assert test() == 2

    # Act / Assert (in functions)
    await asyncio.gather(task_a(), task_b())
