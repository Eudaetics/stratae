"""Test suite for Scope.cache's wrapper/cache-key behavior, exercised via the sync Scope."""

from typing import Any, Callable, Literal
from unittest.mock import Mock

import pytest

from stratae.lifecycle.exceptions import LifecycleConfigurationError
from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


type TestType = Literal["sync", "cm_sync"]


def callable_factory(
    type: TestType,
    scope: Scope,
    call_counter: Mock,
    cleanup_counter: Mock | None = None,
    config: dict[str, Any] | None = None,
) -> Callable[..., SimpleObject]:
    """Create functions for Scope.cache wrapper testing."""

    def calc(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0) -> int:
        value = x if isinstance(x, int) else x.value
        value += y
        value += z if isinstance(z, int) else z.value
        return value

    @scope.cache(**(config or {}))
    def get_object_sync(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0):
        call_counter()
        return SimpleObject(calc(x, y, z))

    @scope.cache(**(config or {}))
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
def test_cache(scope: Scope, func_type: TestType):
    """
    Test cache functionality with arguments.

    Given: A function that takes arguments and is cached in a scope.
    When: The function is called multiple times with the same and different arguments within
          the same scope activation.
    Then: The cached result is returned for the same arguments, and new results are created for
          different arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter)

    with scope.activate():
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
def test_cache_kwargs(scope: Scope, func_type: TestType):
    """
    Test cache functionality with keyword arguments.

    Given: A function that takes keyword arguments and is cached in a scope.
    When: The function is called multiple times with the same and different keyword arguments
            within the same scope activation.
    Then: The cached result is returned for the same keyword arguments, and new results are
            created for different keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter)

    with scope.activate():
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
def test_cache_object(scope: Scope, func_type: TestType):
    """
    Test cache functionality with object arguments.

    Given: A function that takes an object as an argument and is cached in a scope.
    When: The function is called multiple times with the same and different object instances
          within the same scope activation.
    Then: The cached result is returned for the same object instance, and new results are
          created for different object instances.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter)

    with scope.activate():
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
def test_cache_args_mixed(scope: Scope, func_type: TestType):
    """
    Test cache functionality with equivalent positional/keyword calling styles.

    Given: A function that takes both positional and keyword arguments and is cached.
    When: The function is called multiple times with the same argument values but different
          combinations of positional and keyword calling style within the same activation.
    Then: The cached result is shared for plain wrappers and distinct for context-manager
          wrappers.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter)

    with scope.activate():
        # Act
        obj1 = get_object(1, y=2, z=3)
        obj2 = get_object(1, 2, z=3)
        obj3 = get_object(y=2, x=1, z=3)

        # Assert
        assert obj1.value == 6
        assert obj1 is obj2 is obj3
        assert call_counter.call_count == 1
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_cache_no_args(scope: Scope, func_type: TestType):
    """
    Test cache functionality with no arguments.

    Given: A function that takes no arguments and is cached.
    When: The function is called multiple times within the same scope activation.
    Then: The cached result is returned for all calls.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter)

    with scope.activate():
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
def test_cache_different_activations(scope: Scope, func_type: TestType):
    """
    Test cache functionality across different activations of the same scope.

    Given: A function that is cached in a scope.
    When: The function is called within two different activations of that scope.
    Then: The cached result is returned within one activation, but new results are created
          in the next activation.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter)

    # Act & Assert
    with scope.activate():
        obj1 = get_object(1, y=2)
        obj2 = get_object(1, y=2)
        assert obj1 is obj2
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0

    with scope.activate():
        obj3 = get_object(1, y=2)
        assert obj3 is not obj1
        assert obj3.value == 3
    assert call_counter.call_count == 2
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_unhashable_arg(scope: Scope, func_type: TestType):
    """
    Test cache functionality with unhashable arguments.

    Given: A function that takes an unhashable argument (like a list) and is cached.
    When: The function is called with an unhashable argument within an active scope.
    Then: A TypeError is raised when trying to cache with unhashable arguments.
    """
    # Arrange
    get_object = callable_factory(func_type, scope, Mock())

    with scope.activate():
        # Act
        list1 = [1, 2, 3]

        with pytest.raises(TypeError, match="unhashable type: 'list'"):
            get_object(list1)


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_unhashable_kwarg(scope: Scope, func_type: TestType):
    """
    Test cache functionality with unhashable keyword arguments.

    Given: A function that takes an unhashable keyword argument (like a dict) and is cached.
    When: The function is called with an unhashable keyword argument within an active scope.
    Then: A TypeError is raised when trying to cache with unhashable keyword arguments.
    """
    # Arrange
    get_object = callable_factory(func_type, scope, Mock())

    with scope.activate():
        # Act
        dict1 = {"a": 1, "b": 2}

        with pytest.raises(TypeError, match="unhashable type: 'dict'"):
            get_object(z=dict1)


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
def test_cache_with_different_kwarg_order(scope: Scope, func_type: TestType):
    """
    Test cache functionality with keyword arguments in different orders.

    Given: A function that takes keyword arguments and is cached.
    When: The function is called multiple times with the same keyword arguments in different
          orders within the same scope activation.
    Then: The cached result is returned regardless of the order of keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter)

    with scope.activate():
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
def test_cache_ignore_params(scope: Scope, func_type: TestType):
    """
    Test cache functionality with ignore_params set to True.

    Given: A function that takes arguments and is cached with ignore_params=True.
    When: The function is called multiple times with different arguments within the same
          scope activation.
    Then: The cached result is returned for all calls, ignoring the arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    config = {"ignore_params": True}
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter, config=config)

    with scope.activate():
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
def test_cache_ignore_params_with_kwargs(scope: Scope, func_type: TestType):
    """
    Test cache functionality with ignore_params set to True and keyword arguments.

    Given: A function that takes kwargs and is cached with ignore_params=True.
    When: The function is called multiple times with different keyword arguments within the
          same scope activation.
    Then: The cached result is returned for all calls, ignoring the keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    config = {"ignore_params": True}
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter, config=config)

    with scope.activate():
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
def test_cache_with_custom_cache_key(scope: Scope, func_type: TestType):
    """
    Test cache functionality with a custom cache key function.

    Given: A function that takes arguments and is cached with a custom cache key.
    When: The function is called multiple times with different arguments that map to the
          same cache key within the same scope activation.
    Then: The cached result is returned for calls that produce the same cache key.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()

    def custom_cache_key(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0) -> str:
        return f"{x}-{y}"

    config = {"cache_key": custom_cache_key}
    get_object = callable_factory(func_type, scope, call_counter, cleanup_counter, config=config)

    with scope.activate():
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


def test_cache_invalid_params(scope: Scope):
    """
    Test that providing both ignore_params and cache_key raises a ValueError.

    Given: A Scope instance.
    When: Attempting to create a cache decorator with both ignore_params=True and a cache_key.
    Then: A ValueError is raised.
    """
    with pytest.raises(ValueError, match="Cannot use both ignore_params and cache_key together."):
        scope.cache(
            cache_key=lambda *args, **kwargs: (args[0], kwargs.get("x", None)),
            ignore_params=True,
        )


def test_cache_key_no_params(scope: Scope):
    """
    Test caching with cache_key when the function has no parameters.

    Given: A Scope.
    When: A cached function takes no parameters but has a custom cache key.
    Then: The function should use the custom cache key.
    """
    # Arrange
    counter = Mock()
    x = 0

    @scope.cache(cache_key=lambda: x)
    def get_value() -> int:
        counter()
        return counter.call_count

    # Act / Assert
    with scope.activate():
        x = 1
        assert get_value() == 1
        assert get_value() == 1
        x = 0
        assert get_value() == 2
        x = 1
        assert get_value() == 1


def test_wrapper_parameter_types(scope: Scope):
    """
    Caching must work for every parameter kind.

    Given: A cached function with positional-only, positional-or-keyword, *args,
           keyword-only, and **kwargs parameters.
    When: The function is called with matching arguments.
    Then: The result should be cached and returned for identical calls.
    """

    # Arrange
    class Container:
        def __init__(self, value: int):
            self.value = value

    @scope.cache()
    def foo(x: int, /, y: int, z: int = 0, *args: int, c: int = 1, **kwargs: int) -> Container:
        return Container(x + y + z + c + sum(args) + sum(kwargs.values()))

    with scope.activate():
        # Act
        simple = foo(1, 2)
        complex = foo(1, 1, 1, 1, 1, 1, c=1, f=1)

        # Assert
        assert simple.value == 4
        assert simple is foo(1, 2)
        assert simple is not foo(1, 1)

        assert complex.value == 8
        assert complex is foo(1, 1, 1, 1, 1, 1, c=1, f=1)
        assert complex is not foo(1, 1, 1, 1, 1, c=1, f=1)


def test_wrapper_kwargs_only(scope: Scope):
    """
    Test caching with a function that only takes **kwargs.

    Given: A cached function whose only parameter is **kwargs.
    When: The function is called with the same and different keyword arguments.
    Then: The cached result is returned for the same keyword arguments, and a new
          result is created for different keyword arguments.
    """

    # Arrange
    class Container:
        def __init__(self, value: int):
            self.value = value

    @scope.cache()
    def foo(**kwargs: int):
        return Container(sum(kwargs.values()))

    with scope.activate():
        # Act
        result = foo(a=1, b=2, c=3)

        # Assert
        assert result.value == 6
        assert result is foo(a=1, b=2, c=3)
        assert result is not foo(a=0, b=2, c=3)


def test_cache_same_function_in_two_scopes_raises():
    """
    A function can only ever be cached in one scope.

    Given: A function already cached in one scope,
    When: An attempt is made to cache it again in a different scope,
    Then: A LifecycleConfigurationError is raised, and the function stays claimed by the
          first scope.
    """
    # Arrange
    scope_a = Scope("a", "shared")
    scope_b = Scope("b", "shared")

    def get_value():
        return object()

    scope_a.cache()(get_value)

    # Act & Assert
    with pytest.raises(
        LifecycleConfigurationError,
        match="already cached in scope 'a'; cannot also cache it in 'b'",
    ):
        scope_b.cache()(get_value)
