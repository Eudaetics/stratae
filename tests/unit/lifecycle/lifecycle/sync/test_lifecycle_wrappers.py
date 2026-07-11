"""Test suite for cache key functionality in lifecycle scopes."""

from typing import Any, Callable, Literal
from unittest.mock import Mock

import pytest

from stratae.lifecycle import Lifecycle, resource


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


type TestType = Literal["sync", "cm_sync"]


def callable_factory(
    type: TestType,
    lifecycle: Lifecycle,
    call_counter: Mock,
    cleanup_counter: Mock | None = None,
    config: dict[str, Any] | None = None,
) -> Callable[..., SimpleObject]:
    """Create functions for lifecycle cache wrapper testing."""

    def calc(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0) -> int:
        value = x if isinstance(x, int) else x.value
        value += y
        value += z if isinstance(z, int) else z.value
        return value

    @lifecycle.cache("application", **(config or {}))
    def get_object_sync(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0):
        call_counter()
        return SimpleObject(calc(x, y, z))

    @lifecycle.cache("application", **(config or {}))
    @resource
    def get_object_cm_sync(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0):
        call_counter()
        yield SimpleObject(calc(x, y, z))
        if cleanup_counter:
            cleanup_counter()

    if type == "sync":
        return get_object_sync
    return get_object_cm_sync


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with arguments.

    Given: A function that takes arguments and uses lifecycle caching.
    When: The function is called multiple times with the same and different arguments within the
          same lifecycle scope.
    Then: The cached result is returned for the same arguments, and new results are created for
          different arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    with lifecycle.start("application"):
        # Act
        obj1 = get_object(1)
        obj2 = get_object(1)
        obj3 = get_object(2)

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert obj3.value == 2
        assert call_counter.call_count == 2
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_kwargs(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with keyword arguments.

    Given: A function that takes keyword arguments and uses lifecycle caching.
    When: The function is called multiple times with the same and different keyword arguments
            within the same lifecycle scope.
    Then: The cached result is returned for the same keyword arguments, and new results are
            created for different keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    with lifecycle.start("application"):
        # Act
        obj1 = get_object(0, y=1)
        obj2 = get_object(0, y=1)
        obj3 = get_object(0, y=2)

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert obj3.value == 2
        assert call_counter.call_count == 2
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_object(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with object arguments.

    Given: A function that takes an object as an argument and uses lifecycle caching.
    When: The function is called multiple times with the same and different object instances within
          the same lifecycle scope.
    Then: The cached result is returned for the same object instance, and new results are created
          for different object instances.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    with lifecycle.start("application"):
        # Act
        input_obj1 = SimpleObject(1)
        input_obj2 = SimpleObject(1)

        obj1 = get_object(input_obj1)
        obj2 = get_object(input_obj1)
        obj3 = get_object(input_obj2)

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert obj3 is not obj1
        assert obj3.value == 1
        assert call_counter.call_count == 2
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_object_kwargs(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with object keyword arguments.

    Given: A function that takes an object as a keyword argument and uses lifecycle caching.
    When: The function is called multiple times with the same and different object instances within
          the same lifecycle scope.
    Then: The cached result is returned for the same object instance, and new results are created
          for different object instances.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    with lifecycle.start("application"):
        # Act
        input_obj1 = SimpleObject(1)
        input_obj2 = SimpleObject(1)

        obj1 = get_object(1, z=input_obj1)
        obj2 = get_object(1, z=input_obj1)
        obj3 = get_object(1, z=input_obj2)

        # Assert
        assert obj1 is obj2
        assert obj1.value == 2
        assert obj3 is not obj1
        assert obj3.value == 2
        assert call_counter.call_count == 2
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_args_mixed(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with args used as kwargs.

    Note: Args are not parsed and updated from kwargs to try to keep performance high.

    To ensure cache hits, use consistent calling conventions within lifecycle-scoped functions:
    - Use all positional: get_simple_object(1, 2, z=3)
    - Use all kwargs: get_simple_object(x=1, y=2, z=3)

    Given: A function that takes both positional and keyword arguments and uses
           lifecycle caching.
    When: The function is called multiple times with the same and different combinations of
          arguments as kwargs within the same lifecycle scope.
    Then: The cached result is different for different combinations of arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    with lifecycle.start("application"):
        # Act
        obj1 = get_object(1, y=2, z=3)
        obj2 = get_object(1, 2, z=3)
        obj3 = get_object(y=2, x=1, z=3)

        # Assert
        assert obj1 is not obj2
        assert obj2 is not obj3
        assert obj1.value == 6
        assert obj2.value == 6
        assert obj3.value == 6
        assert call_counter.call_count == 3
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 3


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_no_args(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with no arguments.

    Given: A function that takes no arguments and uses lifecycle caching.
    When: The function is called multiple times within the same lifecycle scope.
    Then: The cached result is returned for all calls.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    with lifecycle.start("application"):
        # Act
        obj1 = get_object()
        obj2 = get_object()

        # Assert
        assert obj1 is obj2
        assert obj1.value == 0
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_different_scopes(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality across different scopes.

    Given: A function that uses lifecycle caching.
    When: The function is called within different lifecycle scopes.
    Then: The cached result is returned within the same scope, but new results are created
          for different scopes.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    # Act & Assert
    with lifecycle.start("application"):
        obj1 = get_object(1, y=2)
        obj2 = get_object(1, y=2)
        assert obj1 is obj2
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0

    with lifecycle.start("application"):
        obj3 = get_object(1, y=2)
        assert obj3 is not obj1
        assert obj3.value == 3
    assert call_counter.call_count == 2
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_unhashable_arg(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with unhashable arguments.

    Given: A function that takes an unhashable argument (like a list) and uses
           lifecycle caching.
    When: The function is called multiple times with the same and different unhashable arguments
          within the same lifecycle scope.
    Then: A TypeError is raised when trying to cache with unhashable arguments.
    """
    # Arrange
    get_object = callable_factory(func_type, lifecycle, Mock())

    with lifecycle.start("application"):
        # Act
        list1 = [1, 2, 3]

        with pytest.raises(TypeError, match="unhashable type: 'list'"):
            get_object(list1)


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_unhashable_kwarg(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with unhashable keyword arguments.

    Given: A function that takes an unhashable keyword argument (like a dict) and
           uses lifecycle caching.
    When: The function is called multiple times with the same and different unhashable keyword
          arguments within the same lifecycle scope.
    Then: A TypeError is raised when trying to cache with unhashable keyword arguments.
    """
    # Arrange
    get_object = callable_factory(func_type, lifecycle, Mock())

    with lifecycle.start("application"):
        # Act
        dict1 = {"a": 1, "b": 2}

        with pytest.raises(TypeError, match="unhashable type: 'dict'"):
            get_object(z=dict1)


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_with_different_kwarg_order(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with keyword arguments in different orders.

    Given: A function that takes keyword arguments and uses lifecycle caching.
    When: The function is called multiple times with the same keyword arguments in different orders
          within the same lifecycle scope.
    Then: The cached result is returned regardless of the order of keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, lifecycle, call_counter, cleanup_counter)

    with lifecycle.start("application"):
        # Act
        obj1 = get_object(x=1, y=2)
        obj2 = get_object(y=2, x=1)

        # Assert
        assert obj1 is obj2
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_ignore_params(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with ignore_params set to True.

    Given: A function that takes arguments and uses lifecycle caching with ignore_params=True.
    When: The function is called multiple times with different arguments within the same lifecycle
          scope.
    Then: The cached result is returned for all calls, ignoring the arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    config = {"ignore_params": True}
    get_object = callable_factory(
        func_type, lifecycle, call_counter, cleanup_counter, config=config
    )

    with lifecycle.start("application"):
        # Act
        obj1 = get_object(1)
        obj2 = get_object(2)
        obj3 = get_object(3)

        # Assert
        assert obj1 is obj2 is obj3
        assert obj1.value == 1
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_ignore_params_with_kwargs(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with ignore_params set to True and keyword arguments.

    Given: A function that takes kwargs and uses lifecycle caching with ignore_params=True.
    When: The function is called multiple times with different keyword arguments within the same
          lifecycle scope.
    Then: The cached result is returned for all calls, ignoring the keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    config = {"ignore_params": True}
    get_object = callable_factory(
        func_type, lifecycle, call_counter, cleanup_counter, config=config
    )

    with lifecycle.start("application"):
        # Act
        obj1 = get_object(x=1, y=2)
        obj2 = get_object(x=3, y=4)
        obj3 = get_object(x=5, y=6)

        # Assert
        assert obj1 is obj2 is obj3
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_lifecycle_cache_with_custom_cache_key(lifecycle: Lifecycle, func_type: TestType):
    """
    Test the lifecycle cache functionality with a custom cache key function.

    Given: A function that takes arguments and uses lifecycle caching with a custom cache key.
    When: The function is called multiple times with different arguments that map to the same
          cache key within the same lifecycle scope.
    Then: The cached result is returned for calls that produce the same cache key.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()

    def custom_cache_key(*args: tuple[str, ...], **kwargs: dict[str, Any]) -> str:
        return f"{args[0]}-{kwargs.get('y', 'default_key')}"

    config = {"cache_key": custom_cache_key}
    get_object = callable_factory(
        func_type, lifecycle, call_counter, cleanup_counter, config=config
    )

    with lifecycle.start("application"):
        # Act
        obj1 = get_object(1, y=0)
        obj2 = get_object(1, y=0, z=1)
        obj3 = get_object(4, y=1)
        obj4 = get_object(1, y=1)

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert obj3.value == 5
        assert obj4.value == 2
        assert call_counter.call_count == 3
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 3


def test_lifecycle_cache_invalid_params(lifecycle: Lifecycle):
    """
    Test that providing both ignore_params and cache_key raises a ValueError.

    Given: A lifecycle instance.
    When: Attempting to create a cache decorator with both ignore_params=True and a cache_key.
    Then: A ValueError is raised.
    """
    with pytest.raises(ValueError, match="Cannot use both ignore_params and cache_key together."):
        lifecycle.cache(
            "application",
            cache_key=lambda *args, **kwargs: (args[0], kwargs.get("x", None)),
            ignore_params=True,
        )


def test_lifecycle_cache_key_no_params(lifecycle: Lifecycle):
    """
    Test lifecycle caching with cache_key when there are no parameters.

    Given: A Lifecycle with application scope.
    When: The function takes no parameters but has a custom cache key.
    Then: The function should use the custom cache key.
    """
    # Arrange
    counter = Mock()
    x = 0

    @lifecycle.cache("application", cache_key=lambda: x)
    def get_value() -> int:
        counter()
        return counter.call_count

    # Act / Assert
    with lifecycle.start("application"):
        x = 1
        assert get_value() == 1
        assert get_value() == 1
        x = 0
        assert get_value() == 2
        x = 1
        assert get_value() == 1
