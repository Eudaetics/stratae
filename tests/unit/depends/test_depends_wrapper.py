"""Test suite for the DependsWrapper class in the dependency injection module."""

import asyncio

import pytest

from stratae.depends import DependsWrapper
from stratae.depends.exceptions import DependencyNotFoundError


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
    result = await depends.provide()

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


def test_create_returns_same_instance_for_same_dependency():
    """
    Verify that DependsWrapper.create returns the same instance for the same dependency.

    Given: a dependency already wrapped via DependsWrapper.create,
    When: DependsWrapper.create is called again with the same dependency,
    Then: it should return the exact same DependsWrapper instance.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    # Act
    first = DependsWrapper(sample_dependency)
    second = DependsWrapper(sample_dependency)

    # Assert
    assert first is second


def test_create_returns_different_instance_for_different_dependency():
    """
    Verify that DependsWrapper.create returns distinct instances for distinct dependencies.

    Given: two different dependencies,
    When: DependsWrapper.create is called with each,
    Then: it should return two distinct DependsWrapper instances.
    """

    # Arrange
    def first_dependency():
        return "first"

    def second_dependency():
        return "second"

    # Act
    first = DependsWrapper(first_dependency)
    second = DependsWrapper(second_dependency)

    # Assert
    assert first is not second


def test_create_wraps_the_given_dependency():
    """
    Verify that DependsWrapper.create wraps the given dependency correctly.

    Given: a dependency,
    When: DependsWrapper.create is called with it,
    Then: the resulting instance's dependency should be the one given.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    # Act
    depends = DependsWrapper(sample_dependency)

    # Assert
    assert depends.dependency == sample_dependency


def test_update_dependency():
    """
    Updating a dependnecy replaces both dependency and provide.

    Given: a DependsWrapper instance with no active override,
    When: update is called with a new dependency,
    Then: both dependency and provide should reflect the new dependency.
    """

    # Arrange
    def original_dependency():
        return "original"

    def updated_dependency():
        return "updated"

    depends = DependsWrapper(original_dependency)

    # Act
    depends.update(updated_dependency)

    # Assert
    assert depends.dependency == updated_dependency
    assert depends.provide == updated_dependency


def test_update_dependency_during_override():
    """
    Verify that update does not resync provide while an override is active.

    Given: a DependsWrapper instance with an active override,
    When: update is called with a new dependency,
    Then: dependency should update but provide should be left untouched.
    """

    # Arrange
    def original_dependency():
        return "original"

    def updated_dependency():
        return "updated"

    depends = DependsWrapper(original_dependency)
    depends.override_count = 1

    # Act
    depends.update(updated_dependency)

    # Assert
    assert depends.dependency == updated_dependency
    assert depends.provide == original_dependency


def test_returns_same_instance_after_update():
    """
    Verify that DependsWrapper singleton identity survives update.

    Given: a DependsWrapper instance that has been updated to a new dependency,
    When: DependsWrapper is constructed again with the original dependency,
    Then: it should return the same instance as before the update.
    """

    # Arrange
    def original_dependency():
        return "original"

    def updated_dependency():
        return "updated"

    depends = DependsWrapper(original_dependency)

    # Act
    depends.update(updated_dependency)
    same = DependsWrapper(original_dependency)

    # Assert
    assert same is depends


def test_find_with_dependency():
    """
    Verify that find returns the wrapper for a registered dependency.

    Given: a dependency wrapped via DependsWrapper,
    When: find is called with that dependency,
    Then: it should return the associated DependsWrapper instance.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    depends = DependsWrapper(sample_dependency)

    # Act
    found = DependsWrapper.find(sample_dependency)

    # Assert
    assert found is depends


def test_find_invalid_dependency():
    """
    Verify that find raises for an unregistered dependency.

    Given: a function that has never been wrapped via DependsWrapper,
    When: find is called with that function,
    Then: it should raise DependencyNotFoundError.
    """

    # Arrange
    def unregistered_dependency():
        return "unregistered"

    # Act & Assert
    with pytest.raises(DependencyNotFoundError):
        DependsWrapper.find(unregistered_dependency)


def test_find_after_update():
    """
    Verify that find still resolves the original dependency after an update.

    Given: a DependsWrapper that has been updated to a new dependency,
    When: find is called with the original dependency,
    Then: it should return the same DependsWrapper instance.
    """

    # Arrange
    def original_dependency():
        return "original"

    def updated_dependency():
        return "updated"

    depends = DependsWrapper(original_dependency)

    # Act
    depends.update(updated_dependency)
    found = DependsWrapper.find(original_dependency)

    # Assert
    assert found is depends


def test_provide_override_unset():
    """
    Verify that provide_override falls back to the dependency when unset.

    Given: a DependsWrapper instance with no override set,
    When: provide_override is called,
    Then: it should return the result of calling the dependency.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    depends = DependsWrapper(sample_dependency)

    # Act
    result = depends.provide_override()

    # Assert
    assert result == "sample"


def test_provide_override_set():
    """
    Verify that provide_override returns the override value when set.

    Given: a DependsWrapper instance with an override value set,
    When: provide_override is called,
    Then: it should return the override value instead of calling the dependency.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    depends = DependsWrapper(sample_dependency)
    token = depends.override.set("overridden")

    try:
        # Act
        result = depends.provide_override()

        # Assert
        assert result == "overridden"
    finally:
        depends.override.reset(token)


def test_provide_override_nested():
    """
    Verify that nested overrides resolve and restore correctly.

    Given: a DependsWrapper instance with an override set inside another override,
    When: provide_override is called at each nesting level,
    Then: it should return the innermost value while nested, and fall back to
    the outer value and then the dependency as each override is reset.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    depends = DependsWrapper(sample_dependency)

    # Act & Assert
    outer_token = depends.override.set("outer")
    assert depends.provide_override() == "outer"

    inner_token = depends.override.set("inner")
    assert depends.provide_override() == "inner"

    depends.override.reset(inner_token)
    assert depends.provide_override() == "outer"

    depends.override.reset(outer_token)
    assert depends.provide_override() == "sample"
