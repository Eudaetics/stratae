"""Test suite for AsyncScope.cache's wrapper/cache-key behavior, exercised via AsyncScope."""

import asyncio
import re
from collections.abc import Awaitable, Callable
from typing import Any, Literal
from unittest.mock import Mock

import pytest

from stratae.lifecycle.scope import AsyncScope


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


type TestType = Literal["sync", "cm_sync", "async", "cm_async"]


def callable_factory(
    type: TestType,
    scope: AsyncScope,
    call_counter: Mock,
    cleanup_counter: Mock | None = None,
    config: dict[str, Any] | None = None,
) -> Callable[..., Awaitable[SimpleObject] | SimpleObject]:
    """Create functions for AsyncScope.cache wrapper testing."""

    def calc(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0) -> int:
        value = x if isinstance(x, int) else x.value
        value += y
        value += z if isinstance(z, int) else z.value
        return value

    @scope.cache(**(config or {}))
    async def get_object_async(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0):
        call_counter()
        await asyncio.sleep(0)
        return SimpleObject(calc(x, y, z))

    @scope.cache(**(config or {}))
    async def get_object_cm_async(x: int | SimpleObject = 0, y: int = 0, z: int | SimpleObject = 0):
        call_counter()
        await asyncio.sleep(0)
        yield SimpleObject(calc(x, y, z))
        if cleanup_counter:
            cleanup_counter()

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

    return {
        "sync": get_object_sync,
        "cm_sync": get_object_cm_sync,
        "async": get_object_async,
    }.get(type, get_object_cm_async)


async def maybe_await(result: Awaitable[SimpleObject] | SimpleObject) -> SimpleObject:
    """Await the result if it's awaitable, else return directly."""
    if isinstance(result, Awaitable):
        return await result
    return result


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with arguments.

    Given: An async-scoped function that takes arguments and is cached.
    When: The function is called multiple times with the same and different arguments within
          the same scope activation.
    Then: The cached result is returned for the same arguments, and new results are created for
          different arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter)

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object(1))
        obj2 = await maybe_await(get_object(1))
        obj3 = await maybe_await(get_object(2))

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert obj3.value == 2
        assert call_counter.call_count == 2
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_kwargs(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with keyword arguments.

    Given: An async-scoped function that takes keyword arguments and is cached.
    When: The function is called multiple times with the same and different keyword
          arguments within the same scope activation.
    Then: The cached result is returned for the same keyword arguments, and new results are
          created for different keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter)

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object(0, y=1))
        obj2 = await maybe_await(get_object(0, y=1))
        obj3 = await maybe_await(get_object(0, y=2))

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert obj3.value == 2
        assert call_counter.call_count == 2
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_object(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with object arguments.

    Given: An async-scoped function that takes an object as an argument and is cached.
    When: The function is called multiple times with the same and different object
          instances within the same scope activation.
    Then: The cached result is returned for the same object instance, and new results are
          created for different object instances.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter)

    async with async_scope:
        # Act
        input_obj1 = SimpleObject(1)
        input_obj2 = SimpleObject(1)

        obj1 = await maybe_await(get_object(input_obj1))
        obj2 = await maybe_await(get_object(input_obj1))
        obj3 = await maybe_await(get_object(input_obj2))

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert obj3 is not obj1
        assert obj3.value == 1
        assert call_counter.call_count == 2
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_args_mixed(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with equivalent positional/keyword calling styles.

    Given: An async-scoped function that takes both positional and keyword arguments and
           is cached.
    When: The function is called multiple times with the same argument values but
          different combinations of positional and keyword calling style within the same
          scope activation.
    Then: The cached result is shared for plain wrappers and distinct for context-manager
          wrappers.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter)

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object(1, y=2, z=3))
        obj2 = await maybe_await(get_object(1, 2, z=3))
        obj3 = await maybe_await(get_object(y=2, x=1, z=3))

        # Assert
        assert obj1.value == 6
        assert obj1 is obj2 is obj3
        assert call_counter.call_count == 1
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_no_args(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with no arguments.

    Given: An async-scoped function that takes no arguments and is cached.
    When: The function is called multiple times within the same scope activation.
    Then: The cached result is returned for all calls.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter)

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object())
        obj2 = await maybe_await(get_object())

        # Assert
        assert obj1 is obj2
        assert obj1.value == 0
        assert call_counter.call_count == 1
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_different_activations(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality across different activations of the same scope.

    Given: An async-scoped function that is cached.
    When: The function is called within two different activations of that scope.
    Then: The cached result is returned within one activation, but new results are created
          in the next activation.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter)

    # Act & Assert
    async with async_scope:
        obj1 = await maybe_await(get_object(1, y=2))
        obj2 = await maybe_await(get_object(1, y=2))
        assert obj1 is obj2
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0

    async with async_scope:
        obj3 = await maybe_await(get_object(1, y=2))
        assert obj3 is not obj1
        assert obj3.value == 3
    assert call_counter.call_count == 2
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 2


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_unhashable_arg(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with unhashable arguments.

    Given: An async-scoped function that takes an unhashable argument (like a list) and is
           cached.
    When: The function is called with an unhashable argument within an active scope.
    Then: A TypeError is raised when trying to cache with unhashable arguments.
    """
    # Arrange
    get_object = callable_factory(func_type, async_scope, Mock())

    async with async_scope:
        # Act
        list1 = [1, 2, 3]

        with pytest.raises(TypeError, match="unhashable type: 'list'"):
            await maybe_await(get_object(list1))


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_unhashable_kwarg(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with unhashable keyword arguments.

    Given: An async-scoped function that takes an unhashable keyword argument (like a
           dict) and is cached.
    When: The function is called with an unhashable keyword argument within an active
          scope.
    Then: A TypeError is raised when trying to cache with unhashable keyword arguments.
    """
    # Arrange
    get_object = callable_factory(func_type, async_scope, Mock())

    async with async_scope:
        # Act
        dict1 = {"a": 1, "b": 2}

        with pytest.raises(TypeError, match="unhashable type: 'dict'"):
            await maybe_await(get_object(z=dict1))


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_with_different_kwarg_order(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with keyword arguments in different orders.

    Given: An async-scoped function that takes keyword arguments and is cached.
    When: The function is called multiple times with the same keyword arguments in
          different orders within the same scope activation.
    Then: The cached result is returned regardless of the order of keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter)

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object(x=1, y=2))
        obj2 = await maybe_await(get_object(y=2, x=1))

        # Assert
        assert obj1 is obj2
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_ignore_params(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with ignore_params set to True.

    Given: An async-scoped function that takes arguments and is cached with
           ignore_params=True.
    When: The function is called multiple times with different arguments within the same
          scope activation.
    Then: The cached result is returned for all calls, ignoring the arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    config = {"ignore_params": True}
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter, config)

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object(1))
        obj2 = await maybe_await(get_object(2))
        obj3 = await maybe_await(get_object(3))

        # Assert
        assert obj1 is obj2 is obj3
        assert obj1.value == 1
        assert call_counter.call_count == 1
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_ignore_params_with_kwargs(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with ignore_params set to True and keyword arguments.

    Given: An async-scoped function that takes kwargs and is cached with
           ignore_params=True.
    When: The function is called multiple times with different keyword arguments within
          the same scope activation.
    Then: The cached result is returned for all calls, ignoring the keyword arguments.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    config = {"ignore_params": True}
    get_object = callable_factory(
        func_type, async_scope, call_counter, cleanup_counter, config=config
    )

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object(x=1, y=2))
        obj2 = await maybe_await(get_object(x=3, y=4))
        obj3 = await maybe_await(get_object(x=5, y=6))

        # Assert
        assert obj1 is obj2 is obj3
        assert obj1.value == 3
        assert call_counter.call_count == 1
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 1


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
async def test_cache_with_custom_cache_key(async_scope: AsyncScope, func_type: TestType):
    """
    Test cache functionality with a custom cache key function.

    Given: An async-scoped function that is cached with a custom cache key.
    When: The function is called multiple times with different arguments that map to the
          same custom cache key within the same scope activation.
    Then: The cached result is returned for calls with the same custom cache key.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()

    def custom_cache_key(*args: Any, **kwargs: Any) -> str:
        return f"{args[0]}-{args[1]}"

    config = {"cache_key": custom_cache_key}
    get_object = callable_factory(func_type, async_scope, call_counter, cleanup_counter, config)

    async with async_scope:
        # Act
        obj1 = await maybe_await(get_object(1, y=1))
        obj2 = await maybe_await(get_object(1, y=1, z=1))
        obj3 = await maybe_await(get_object(4, y=1))
        obj4 = await maybe_await(get_object(1, y=2))

        # Assert
        assert obj1 is obj2
        assert obj1.value == 2
        assert obj3.value == 5
        assert obj4.value == 3
        assert call_counter.call_count == 3
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 3


def test_cache_invalid_params(async_scope: AsyncScope):
    """
    Test that providing both ignore_params and cache_key raises a ValueError.

    Given: An AsyncScope.
    When: Attempting to create a cache decorator with both ignore_params=True and a
          cache_key.
    Then: A ValueError is raised.
    """
    with pytest.raises(
        ValueError, match=re.escape("Cannot use both ignore_params and cache_key together.")
    ):
        async_scope.cache(
            cache_key=lambda *args, **kwargs: (args[0], kwargs.get("x", None)),
            ignore_params=True,
        )


async def test_cache_key_no_params(async_scope: AsyncScope):
    """
    Test caching with cache_key when the function has no parameters.

    Given: An AsyncScope.
    When: A cached async function takes no parameters but has a custom cache key.
    Then: The function should use the custom cache key.
    """
    # Arrange
    counter = Mock()
    x = 0

    @async_scope.cache(cache_key=lambda: x)
    async def get_value() -> int:
        await asyncio.sleep(0)
        counter()
        return counter.call_count

    # Act / Assert
    async with async_scope:
        x = 1
        assert await get_value() == 1
        assert await get_value() == 1
        x = 0
        assert await get_value() == 2
        x = 1
        assert await get_value() == 1


async def test_cache_sync_in_async_scope(async_scope: AsyncScope):
    """
    A plain sync function cached in an AsyncScope stays sync (not wrapped as awaitable).

    Given: An AsyncScope,
    When: A plain sync function is cached in it and called,
    Then: The result is returned directly, without needing to be awaited.
    """
    # Arrange
    calls = Mock()

    @async_scope.cache()
    def get_value() -> int:
        calls()
        return 42

    # Act
    async with async_scope:
        result = get_value()

    # Assert
    assert result == 42
    assert calls.call_count == 1


async def test_same_function_in_two_scopes_has_independent_caches():
    """
    An async function cached in two scopes should get a separate cache in each.

    Given: One async function cached in two AsyncScopes,
    When: Both are awaited while both scopes are active,
    Then: Each scope caches its own value.
    """
    # Arrange
    scope_a, scope_b = AsyncScope("a"), AsyncScope("b")

    async def get_value() -> object:
        await asyncio.sleep(0)
        return object()

    in_a, in_b = scope_a.cache()(get_value), scope_b.cache()(get_value)

    # Act
    async with scope_a, scope_b:
        first_a, first_b = await in_a(), await in_b()
        again_a, again_b = await in_a(), await in_b()

    # Assert
    assert (again_a, again_b) == (first_a, first_b)
    assert first_a is not first_b


class _AsyncRepo:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, key: int) -> list[int]:
        await asyncio.sleep(0)
        self.calls += 1
        return [key]


async def test_cached_async_bound_method_runs_once_per_activation():
    """
    A cached async bound method should run once per activation for the same arguments.

    Given: An async bound method cached in an AsyncScope,
    When: It's awaited twice in one activation, then twice in a second activation,
    Then: The method runs once in the first activation, and once more in the second,
        returning the same object within each.
    """
    # Arrange
    repo = _AsyncRepo()
    scope = AsyncScope("request")
    cached = scope.cache()(repo.get)

    # Act
    async with scope:
        first = await cached(1)  # runs repo.get
        assert await cached(1) is first  # cache hit
    calls_after_first = repo.calls
    async with scope:
        second = await cached(1)  # new activation, empty cache: runs repo.get again
        assert await cached(1) is second  # cache hit

    # Assert
    assert calls_after_first == 1
    assert repo.calls == 2
