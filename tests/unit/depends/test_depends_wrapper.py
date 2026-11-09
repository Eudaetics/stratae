"""Test suite for the Provider class in the dependency injection module."""

from unittest.mock import AsyncMock, Mock

from stratae.depends import override
from stratae.depends._provide import Provider


def test_depends_wrapper_initialization():
    """
    Provider is initialized with a dependency.

    Given: a dependency,
    When: Provider is initialized with that dependency,
    Then: it should store the dependency correctly.
    """
    # Arrange
    sample_dependency = Mock()

    # Act
    depends = Provider(sample_dependency)

    # Assert
    assert depends.dependency == sample_dependency


def test_depends_wrapper_call():
    """
    Provider provides the result of the wrapped dependency.

    Given: a Provider instance,
    When: it is called as a function,
    Then: it should return the result of the dependency.
    """
    # Arrange
    sample_dependency = Mock(return_value="sample")
    depends = Provider(sample_dependency)

    # Act
    result = depends.provide()

    # Assert
    assert result == "sample"


def test_depends_wrapper_with_lambda():
    """
    Provider is initialized with a lambda function.

    Given: a lambda function as a dependency,
    When: Provider is initialized with it,
    Then: it should store the lambda function correctly.
    """
    # Arrange
    sample_lambda = lambda: "sample"  # noqa: E731

    # Act
    depends = Provider(sample_lambda)

    # Assert
    assert depends.dependency == sample_lambda


def test_sync_depends_is_async():
    """
    is_async is False for a synchronous dependency.

    Given: a Provider instance,
    When: its is_async property is accessed,
    Then: it should return False.
    """
    # Arrange
    sample_dependency = Mock()
    depends = Provider(sample_dependency)

    # Act
    result = depends.is_async

    # Assert
    assert result is False


def test_async_depends_is_async():
    """
    is_async is True for an asynchronous dependency.

    Given: a Provider instance wrapping an async dependency,
    When: its is_async property is accessed,
    Then: it should return True.
    """
    # Arrange
    sample_dependency = AsyncMock()
    depends = Provider(sample_dependency)

    # Act
    result = depends.is_async

    # Assert
    assert result is True


async def test_depends_wrapper_call_with_coroutine():
    """
    Awaiting provide returns the coroutine dependency's result.

    Given: a coroutine dependency,
    When: Provider is called,
    Then: it should return the result of the coroutine.
    """
    # Arrange
    sample_dependency = AsyncMock(return_value="sample")
    depends = Provider(sample_dependency)

    # Act
    result = await depends.provide()

    # Assert
    assert result == "sample"


def test_depends_wrapper_call_multiple_times():
    """
    Provider can be called multiple times, returning each result in turn.

    Given: a Provider instance,
    When: it is called multiple times,
    Then: it should return the correct result each time.
    """
    # Arrange
    sample_dependency = Mock(side_effect=["sample 1", "sample 2", "sample 3"])
    depends = Provider(sample_dependency)

    # Act & Assert
    result1 = depends.provide()
    assert result1 == "sample 1"

    result2 = depends.provide()
    assert result2 == "sample 2"

    result3 = depends.provide()
    assert result3 == "sample 3"


def test_create_returns_same_instance_for_same_dependency():
    """
    Provider returns the same instance for the same dependency.

    Given: a dependency already wrapped via Provider.create,
    When: Provider.create is called again with the same dependency,
    Then: it should return the exact same Provider instance.
    """
    # Arrange
    sample_dependency = Mock()

    # Act
    first = Provider(sample_dependency)
    second = Provider(sample_dependency)

    # Assert
    assert first is second


def test_create_returns_different_instance_for_different_dependency():
    """
    Provider returns distinct instances for distinct dependencies.

    Given: two different dependencies,
    When: Provider.create is called with each,
    Then: it should return two distinct Provider instances.
    """
    # Arrange
    first_dependency = Mock()
    second_dependency = Mock()

    # Act
    first = Provider(first_dependency)
    second = Provider(second_dependency)

    # Assert
    assert first is not second


def test_create_wraps_the_given_dependency():
    """
    Provider wraps the given dependency.

    Given: a dependency,
    When: Provider.create is called with it,
    Then: the resulting instance's dependency should be the one given.
    """
    # Arrange
    sample_dependency = Mock()

    # Act
    depends = Provider(sample_dependency)

    # Assert
    assert depends.dependency == sample_dependency


def test_update_dependency():
    """
    Updating a dependency replaces both dependency and provide.

    Given: a Provider instance with no active override,
    When: update is called with a new dependency,
    Then: both dependency and provide should reflect the new dependency.
    """
    # Arrange
    original_dependency = Mock()
    updated_dependency = Mock()

    depends = Provider(original_dependency)

    # Act
    depends.update(updated_dependency)

    # Assert
    assert depends.dependency == updated_dependency
    assert depends.provide == updated_dependency


def test_update_dependency_during_override():
    """
    Updating during an active override leaves provide untouched.

    Given: a Provider instance with an active override,
    When: update is called with a new dependency,
    Then: dependency should update but provide should be left untouched.
    """
    # Arrange
    original_dependency = Mock()
    updated_dependency = Mock()

    depends = Provider(original_dependency)
    depends.live = 1

    # Act
    depends.update(updated_dependency)

    # Assert
    assert depends.dependency == updated_dependency
    assert depends.provide == original_dependency


def test_returns_same_instance_after_update():
    """
    Provider singleton identity survives an update.

    Given: a Provider instance that has been updated to a new dependency,
    When: Provider is constructed again with the original dependency,
    Then: it should return the same instance as before the update.
    """
    # Arrange
    original_dependency = Mock()
    updated_dependency = Mock()

    depends = Provider(original_dependency)

    # Act
    depends.update(updated_dependency)
    same = Provider(original_dependency)

    # Assert
    assert same is depends


def test_provide_held_unset():
    """
    provide_held falls back to the dependency when unset.

    Given: a Provider instance with no override set,
    When: provide_held is called,
    Then: it should return the result of calling the dependency.
    """
    # Arrange
    sample_dependency = Mock(return_value="sample")
    depends = Provider(sample_dependency)

    # Act
    result = depends.provide_held()

    # Assert
    assert result == "sample"


def test_provide_held_set():
    """
    provide_held returns the override value when set.

    Given: a Provider instance with an override value set,
    When: provide_held is called,
    Then: it should return the override value instead of calling the dependency.
    """
    # Arrange
    sample_dependency = Mock()
    depends = Provider(sample_dependency)
    with override(sample_dependency).to("overridden"):
        # Act
        result = depends.provide_held()

    # Assert
    assert result == "overridden"
    sample_dependency.assert_not_called()


def test_provide_held_nested():
    """
    provide_held resolves and restores correctly across nested overrides.

    Given: a Provider instance with an override set inside another override,
    When: provide_held is called at each nesting level,
    Then: it should return the innermost value while nested, and fall back to
    the outer value and then the dependency as each override is reset.
    """
    # Arrange
    sample_dependency = Mock(return_value="sample")
    depends = Provider(sample_dependency)

    # Act & Assert
    with override(sample_dependency).to("outer"):
        assert depends.provide_held() == "outer"

        with override(sample_dependency).to("inner"):
            assert depends.provide_held() == "inner"

        assert depends.provide_held() == "outer"

    assert depends.provide_held() == "sample"
