"""Test suite for sparse-backed slot storage (Scope(storage="sparse")), via AsyncLifecycle."""

import asyncio
from typing import Awaitable, Callable, Literal
from unittest.mock import Mock

import pytest

from stratae.lifecycle import Scope, async_resource, resource
from stratae.lifecycle.lifecycle import AsyncLifecycle


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


@pytest.fixture
async def sparse_async_lifecycle():
    """Provide an AsyncLifecycle with a sparse-backed shared scope and context scope."""
    yield AsyncLifecycle(
        [Scope("app_sparse", "shared", "sparse"), Scope("req_sparse", "context", "sparse")]
    )


type TestType = Literal["sync", "cm_sync", "async", "cm_async"]


def callable_factory(
    type: TestType,
    lifecycle: AsyncLifecycle,
    scope: str,
    call_counter: Mock,
    cleanup_counter: Mock | None = None,
    ignore_params: bool = False,
) -> Callable[..., Awaitable[SimpleObject] | SimpleObject]:
    """Create cache-wrapped functions of every func_type for sparse-scope testing."""

    @lifecycle.cache(scope, ignore_params=ignore_params)
    async def get_object_async(x: int = 0):
        call_counter()
        await asyncio.sleep(0)
        return SimpleObject(x)

    @lifecycle.cache(scope, ignore_params=ignore_params)
    @async_resource
    async def get_object_cm_async(x: int = 0):
        call_counter()
        await asyncio.sleep(0)
        yield SimpleObject(x)
        if cleanup_counter:
            cleanup_counter()

    @lifecycle.cache(scope, ignore_params=ignore_params)
    def get_object_sync(x: int = 0):
        call_counter()
        return SimpleObject(x)

    @lifecycle.cache(scope, ignore_params=ignore_params)
    @resource
    def get_object_cm_sync(x: int = 0):
        call_counter()
        yield SimpleObject(x)
        if cleanup_counter:
            cleanup_counter()

    if type == "sync":
        return get_object_sync
    elif type == "cm_sync":
        return get_object_cm_sync
    elif type == "async":
        return get_object_async
    return get_object_cm_async


async def maybe_await(result: Awaitable[SimpleObject] | SimpleObject) -> SimpleObject:
    """Await the result if it's awaitable, else return directly."""
    if isinstance(result, Awaitable):
        return await result
    return result


@pytest.mark.parametrize("func_type", ["sync", "cm_sync", "async", "cm_async"])
@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
async def test_async_sparse_storage_cache_keyed(
    sparse_async_lifecycle: AsyncLifecycle, scope: str, func_type: TestType
):
    """
    Sparse-backed scopes cache keyed (argument-bearing) calls the same as dense-backed ones.

    Given: A sparse-backed shared or context scope with a cached function that takes args,
    When: The function is called multiple times with the same and different arguments,
    Then: Identical arguments hit the cache and distinct arguments produce new values.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(
        func_type, sparse_async_lifecycle, scope, call_counter, cleanup_counter
    )

    async with sparse_async_lifecycle.start(scope):
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
@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
async def test_async_sparse_storage_cache_slot_eligible(
    sparse_async_lifecycle: AsyncLifecycle, scope: str, func_type: TestType
):
    """
    Sparse-backed scopes cache slot-eligible (ignore_params) calls by writing the slot directly.

    Given: A sparse-backed shared or context scope with an ignore_params cached function,
    When: The function is called multiple times,
    Then: The same cached value is returned for every call without re-invoking the function.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(
        func_type, sparse_async_lifecycle, scope, call_counter, cleanup_counter, ignore_params=True
    )

    async with sparse_async_lifecycle.start(scope):
        # Act
        obj1 = await maybe_await(get_object(1))
        obj2 = await maybe_await(get_object(2))

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert call_counter.call_count == 1
        if func_type in ["cm_sync", "cm_async"]:
            assert cleanup_counter.call_count == 0
    if func_type in ["cm_sync", "cm_async"]:
        assert cleanup_counter.call_count == 1


async def test_async_sparse_storage_shared_reset_between_activations(
    sparse_async_lifecycle: AsyncLifecycle,
):
    """
    A sparse-backed shared scope's slots are cleared, not merely stale, between activations.

    Given: A sparse-backed shared scope entered via async_lifecycle.start(),
    When: A cached value is populated, the scope exits, and is then re-entered,
    Then: The second activation misses the cache and recomputes the value.
    """
    # Arrange
    call_counter = Mock()

    @sparse_async_lifecycle.cache("app_sparse", ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    async with sparse_async_lifecycle.start("app_sparse"):
        obj1 = get_object(1)
    async with sparse_async_lifecycle.start("app_sparse"):
        obj2 = get_object(2)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2


async def test_async_sparse_storage_context_fresh_per_activation(
    sparse_async_lifecycle: AsyncLifecycle,
):
    """
    A sparse-backed context scope gets an independent slot dict for every activation.

    Given: A sparse-backed context scope entered via async_lifecycle.start(),
    When: A cached value is populated, the scope exits, and is then re-entered,
    Then: The second activation misses the cache and recomputes the value.
    """
    # Arrange
    call_counter = Mock()

    @sparse_async_lifecycle.cache("req_sparse", ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    async with sparse_async_lifecycle.start("req_sparse"):
        obj1 = get_object(1)
    async with sparse_async_lifecycle.start("req_sparse"):
        obj2 = get_object(2)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2


@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
def test_async_sparse_storage_allocate_slot_sequential(
    sparse_async_lifecycle: AsyncLifecycle, scope: str
):
    """
    allocate_slot hands out sequential int keys for a sparse-backed scope, skipping slot 0.

    Given: A sparse-backed scope with no functions registered yet,
    When: allocate_slot is called repeatedly,
    Then: It returns 1, 2, 3, ... - slot 0 stays reserved for the lazily-created exit stack.
    """
    # Act & Assert
    assert sparse_async_lifecycle.allocate_slot(scope) == 1
    assert sparse_async_lifecycle.allocate_slot(scope) == 2
    assert sparse_async_lifecycle.allocate_slot(scope) == 3


@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
async def test_async_sparse_storage_exit_stack_lazy_and_cleaned_up(
    sparse_async_lifecycle: AsyncLifecycle, scope: str
):
    """
    A sparse-backed scope's exit stack is created on first use and closed on pop.

    Given: A sparse-backed scope with a registered async resource,
    When: The resource is entered and the scope is popped,
    Then: The exit stack cleans up the resource, same as a dense-backed scope would.
    """
    # Arrange
    mock = Mock()

    @sparse_async_lifecycle.cache(scope)
    @async_resource
    async def test_resource():
        try:
            yield
        finally:
            mock()

    with pytest.raises(RuntimeError, match=f"Scope '{scope}' is not active."):
        sparse_async_lifecycle.get_exit_stack(scope)

    # Act
    async with sparse_async_lifecycle.start(scope):
        stack = sparse_async_lifecycle.get_exit_stack(scope)
        assert stack is sparse_async_lifecycle.get_exit_stack(scope)
        await test_resource()

    # Assert
    mock.assert_called_once()


@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
async def test_async_sparse_storage_push_pop_api(
    sparse_async_lifecycle: AsyncLifecycle, scope: str
):
    """
    The manual push()/pop() API resets sparse-backed scopes the same way start() does.

    Given: A sparse-backed scope activated and deactivated via push()/pop() rather than
           the async_lifecycle.start() context manager,
    When: A cached value is populated, the scope is popped, and then pushed again,
    Then: The second activation misses the cache, same as with the context manager API.
    """
    # Arrange
    call_counter = Mock()

    @sparse_async_lifecycle.cache(scope, ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    handle = sparse_async_lifecycle.push(scope)
    obj1 = get_object(1)
    await sparse_async_lifecycle.pop(handle)

    handle = sparse_async_lifecycle.push(scope)
    obj2 = get_object(2)
    await sparse_async_lifecycle.pop(handle)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2
