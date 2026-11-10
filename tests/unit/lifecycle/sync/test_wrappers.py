"""Test suite for Scope.cache's wrapper/cache-key behavior, exercised via the sync Scope."""

import functools
import re
from collections.abc import Callable
from functools import wraps
from typing import Any, Literal
from unittest.mock import Mock

import pytest

from stratae.lifecycle.exceptions import UnreadableSignatureError
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

    with scope:
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

    with scope:
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

    with scope:
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

    with scope:
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

    with scope:
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
    with scope:
        obj1 = get_object(1, y=2)
        obj2 = get_object(1, y=2)
        assert obj1 is obj2
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0

    with scope:
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

    with scope:
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

    with scope:
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

    with scope:
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

    with scope:
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

    with scope:
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

    with scope:
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

    The `# pyright: ignore[...]` comments mark the expected type error: the project enables
    `reportUnnecessaryTypeIgnoreComment`, so if it stops being one, the type check fails.

    Given: A Scope instance.
    When: Attempting to create a cache decorator with both ignore_params=True and a cache_key.
    Then: The call is a type error, and a ValueError is raised.
    """
    with pytest.raises(
        ValueError, match=re.escape("Cannot use both ignore_params and cache_key together.")
    ):
        scope.cache(  # pyright: ignore[reportCallIssue]
            cache_key=lambda *args, **kwargs: (args[0], kwargs.get("x", None)),
            ignore_params=True,  # pyright: ignore[reportArgumentType]
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
    with scope:
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

    with scope:
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

    with scope:
        # Act
        result = foo(a=1, b=2, c=3)

        # Assert
        assert result.value == 6
        assert result is foo(a=1, b=2, c=3)
        assert result is not foo(a=0, b=2, c=3)


def test_same_function_in_two_scopes_has_independent_caches():
    """
    A function cached in two scopes should get a separate cache in each.

    Given: One function cached in two scopes,
    When: Both are called while both scopes are active, and one scope is activated again,
    Then: Each scope caches its own value, and refills only its own on a new activation.
    """
    # Arrange
    scope_a, scope_b = Scope("a"), Scope("b")

    def get_value() -> object:
        return object()

    in_a, in_b = scope_a.cache()(get_value), scope_b.cache()(get_value)

    # Act
    with scope_a:
        with scope_b:
            first_a, first_b = in_a(), in_b()
            assert in_a() is first_a
            assert in_b() is first_b
        with scope_b:
            second_b = in_b()
        second_a = in_a()

    # Assert
    assert first_a is not first_b
    assert second_a is first_a
    assert second_b is not first_b


def test_stacked_caches_cache_the_inner_value():
    """
    A cache over another scope's cache should cache what the inner one returns.

    Given: A function cached in an inner scope, and that cached function cached again in an
        outer scope,
    When: The inner scope is activated twice within one outer activation,
    Then: The outer cache keeps the first value, while the inner one refills.
    """
    # Arrange
    outer, inner = Scope("outer"), Scope("inner")

    def get_value() -> object:
        return object()

    in_inner = inner.cache()(get_value)
    in_outer = outer.cache()(in_inner)

    # Act
    with outer:
        with inner:
            first = in_outer()
            assert in_inner() is first
        with inner:
            refilled = in_inner()
            kept = in_outer()

    # Assert
    assert kept is first
    assert refilled is not first


class _Repo:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, key: int) -> list[int]:
        self.calls += 1
        return [key]

    @classmethod
    def make(cls) -> "_Repo":
        return cls()

    @staticmethod
    def helper() -> list[int]:
        return [1]


class _Callable:
    def __call__(self, value: int) -> list[int]:
        return [value]


@pytest.mark.parametrize(
    ("target", "args"),
    [
        (_Repo().get, (1,)),
        (_Repo.make, ()),
        (_Repo.helper, ()),
        (sorted, ((3, 1),)),
        (object, ()),
        ([1].copy, ()),
        (_Callable(), (5,)),
    ],
    ids=[
        "bound_method",
        "classmethod",
        "staticmethod",
        "builtin_function",
        "builtin_type",
        "builtin_bound_method",
        "callable_object",
    ],
)
def test_any_callable_with_a_signature_can_be_cached(
    target: Callable[..., Any], args: tuple[Any, ...]
):
    """
    Any callable whose signature can be read should be cacheable.

    Given: A bound, class or static method, a builtin function, type or bound method, or a
        callable object,
    When: It's cached and called twice with the same arguments in an active scope,
    Then: Both calls return the same object.
    """
    # Arrange
    scope = Scope("request")
    cached = scope.cache()(target)

    # Act
    with scope:
        first, second = cached(*args), cached(*args)

    # Assert
    assert first is second


def test_cached_bound_method_runs_once_per_activation():
    """
    A cached bound method should run once per activation for the same arguments.

    Given: A bound method cached in a scope,
    When: It's called twice in one activation, then twice in a second activation,
    Then: The method runs once in the first activation, and once more in the second.
    """
    # Arrange
    repo = _Repo()
    scope = Scope("request")
    cached = scope.cache()(repo.get)

    # Act
    with scope:
        cached(1)  # runs repo.get
        cached(1)  # cache hit
    calls_after_first = repo.calls
    with scope:
        cached(1)  # new activation, empty cache: runs repo.get again
        cached(1)  # cache hit

    # Assert
    assert calls_after_first == 1
    assert repo.calls == 2


def test_caching_leaves_the_callable_unchanged():
    """
    Caching should write nothing onto the callable, or onto what a decorator wraps.

    Given: A function, and another behind a `functools.wraps` decorator,
    When: Each is cached, and the first is cached in a second scope too,
    Then: Neither function nor the wrapped one gains attributes.
    """

    # Arrange
    def plain() -> int:
        return 1

    def inner() -> int:
        return 2

    @wraps(inner)
    def decorated() -> int:
        return inner()

    before = (dict(vars(plain)), dict(vars(inner)))

    # Act
    Scope("a").cache()(plain)
    Scope("b").cache()(plain)
    Scope("c").cache()(decorated)

    # Assert
    assert (dict(vars(plain)), dict(vars(inner))) == before


@pytest.mark.parametrize(
    "target", [dict, functools.partial(max, 1)], ids=["builtin_type", "builtin_partial"]
)
def test_callable_without_a_readable_signature_is_refused(target: Callable[..., Any]):
    """
    A callable whose signature can't be read should be refused when it's decorated.

    Given: The dict type, or a partial over a builtin, neither with a readable signature,
    When: It's cached in a scope,
    Then: UnreadableSignatureError should be raised, suggesting wrapping it in a function.
    """
    # Act & Assert
    with pytest.raises(UnreadableSignatureError, match="Wrap it in a function"):
        Scope("request").cache()(target)


def test_cached_wrapper_keeps_defaults_of_every_kind(scope: Scope):
    """
    A cached wrapper should keep the defaults of every parameter kind.

    Given: A cached function with positional-only, positional-or-keyword and keyword-only
        defaults, *args and **kwargs
    When: Its wrapper is read, and called without the defaulted arguments
    Then: The wrapper's defaults are the function's, and the call uses them
    """

    # Arrange
    @scope.cache()
    def compute(a: int = 1, /, b: int = 2, *args: int, c: int = 3, **kwargs: int) -> int:
        return a + b + sum(args) + c + sum(kwargs.values())

    # Act
    with scope:
        result = compute()

    # Assert
    wrapper: Any = compute  # Cached doesn't declare a function's attributes.
    assert wrapper.__defaults__ == (1, 2)
    assert wrapper.__kwdefaults__ == {"c": 3}
    assert result == 6
