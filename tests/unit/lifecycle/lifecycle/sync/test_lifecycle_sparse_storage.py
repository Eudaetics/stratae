"""
Test suite for sparse-backed slot storage (Scope(storage="sparse")), exercised via Lifecycle.

allocate_slot's sequential-key behavior itself is BaseLifecycle behavior and is only
covered once, via Lifecycle, in
tests/unit/lifecycle/lifecycle/base/test_slot_allocation.py.
"""

from typing import Callable, Literal
from unittest.mock import Mock

import pytest

from stratae.lifecycle import Lifecycle, Scope, resource


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


@pytest.fixture
def sparse_lifecycle():
    """Provide a Lifecycle with a sparse-backed shared scope and a sparse-backed context scope."""
    yield Lifecycle(
        [Scope("app_sparse", "shared", "sparse"), Scope("req_sparse", "context", "sparse")]
    )


type TestType = Literal["sync", "cm_sync"]


def callable_factory(
    type: TestType,
    lifecycle: Lifecycle,
    scope: str,
    call_counter: Mock,
    cleanup_counter: Mock | None = None,
    ignore_params: bool = False,
) -> Callable[..., SimpleObject]:
    """Create keyed (takes args) cache-wrapped functions for sparse-scope testing."""

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
    return get_object_cm_sync


@pytest.mark.parametrize("func_type", ["sync", "cm_sync"])
@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
def test_sparse_storage_cache_keyed(sparse_lifecycle: Lifecycle, scope: str, func_type: TestType):
    """
    Sparse-backed scopes cache keyed (argument-bearing) calls the same as dense-backed ones.

    Given: A sparse-backed shared or context scope with a cached function that takes args,
    When: The function is called multiple times with the same and different arguments,
    Then: Identical arguments hit the cache and distinct arguments produce new values.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(func_type, sparse_lifecycle, scope, call_counter, cleanup_counter)

    with sparse_lifecycle.start(scope):
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
@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
def test_sparse_storage_cache_slot_eligible(
    sparse_lifecycle: Lifecycle, scope: str, func_type: TestType
):
    """
    Sparse-backed scopes cache slot-eligible (no-arg) calls by writing the slot directly.

    Given: A sparse-backed shared or context scope with an ignore_params cached function,
    When: The function is called multiple times,
    Then: The same cached value is returned for every call without re-invoking the function.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(
        func_type, sparse_lifecycle, scope, call_counter, cleanup_counter, ignore_params=True
    )

    with sparse_lifecycle.start(scope):
        # Act
        obj1 = get_object(1)
        obj2 = get_object(2)

        # Assert
        assert obj1 is obj2
        assert obj1.value == 1
        assert call_counter.call_count == 1
        if func_type == "cm_sync":
            assert cleanup_counter.call_count == 0
    if func_type == "cm_sync":
        assert cleanup_counter.call_count == 1


def test_sparse_storage_shared_reset_between_activations(sparse_lifecycle: Lifecycle):
    """
    A sparse-backed shared scope's slots are cleared, not merely stale, between activations.

    Given: A sparse-backed shared scope entered via lifecycle.start(),
    When: A cached value is populated, the scope exits, and is then re-entered,
    Then: The second activation misses the cache and recomputes the value.
    """
    # Arrange
    call_counter = Mock()

    @sparse_lifecycle.cache("app_sparse", ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    with sparse_lifecycle.start("app_sparse"):
        obj1 = get_object(1)
    with sparse_lifecycle.start("app_sparse"):
        obj2 = get_object(2)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2


def test_sparse_storage_context_fresh_per_activation(sparse_lifecycle: Lifecycle):
    """
    A sparse-backed context scope gets an independent slot dict for every activation.

    Given: A sparse-backed context scope entered via lifecycle.start(),
    When: A cached value is populated, the scope exits, and is then re-entered,
    Then: The second activation misses the cache and recomputes the value.
    """
    # Arrange
    call_counter = Mock()

    @sparse_lifecycle.cache("req_sparse", ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    with sparse_lifecycle.start("req_sparse"):
        obj1 = get_object(1)
    with sparse_lifecycle.start("req_sparse"):
        obj2 = get_object(2)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2


@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
def test_sparse_storage_exit_stack_lazy_and_cleaned_up(sparse_lifecycle: Lifecycle, scope: str):
    """
    A sparse-backed scope's exit stack is created on first use and closed on pop.

    Given: A sparse-backed scope with a registered resource,
    When: The resource is entered and the scope is popped,
    Then: The exit stack cleans up the resource, same as a dense-backed scope would.
    """
    # Arrange
    mock = Mock()

    @sparse_lifecycle.cache(scope)
    @resource
    def test_resource():
        try:
            yield
        finally:
            mock()

    with pytest.raises(RuntimeError, match=f"Scope '{scope}' is not active."):
        sparse_lifecycle.get_exit_stack(scope)

    # Act
    with sparse_lifecycle.start(scope):
        assert sparse_lifecycle.get_exit_stack(scope) is sparse_lifecycle.get_exit_stack(scope)
        test_resource()

    # Assert
    mock.assert_called_once()


@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
def test_sparse_storage_push_pop_api(sparse_lifecycle: Lifecycle, scope: str):
    """
    The manual push()/pop() API resets sparse-backed scopes the same way start() does.

    Given: A sparse-backed scope activated and deactivated via push()/pop() rather than
           the lifecycle.start() context manager,
    When: A cached value is populated, the scope is popped, and then pushed again,
    Then: The second activation misses the cache, same as with the context manager API.
    """
    # Arrange
    call_counter = Mock()

    @sparse_lifecycle.cache(scope, ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    handle = sparse_lifecycle.push(scope)
    obj1 = get_object(1)
    sparse_lifecycle.pop(handle)

    handle = sparse_lifecycle.push(scope)
    obj2 = get_object(2)
    sparse_lifecycle.pop(handle)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2
