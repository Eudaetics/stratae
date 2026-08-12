"""Test suite for sparse-backed slot storage (Scope(storage="sparse")), sync flavor."""

from typing import Callable, Literal
from unittest.mock import Mock

import pytest

from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


@pytest.fixture
def app_sparse():
    """Provide a sparse-backed shared scope."""
    yield Scope("app_sparse", "shared", "sparse")


@pytest.fixture
def req_sparse():
    """Provide a sparse-backed context scope."""
    yield Scope("req_sparse", "context", "sparse")


type TestType = Literal["sync", "cm_sync"]


def callable_factory(
    type: TestType,
    scope: Scope,
    call_counter: Mock,
    cleanup_counter: Mock | None = None,
    ignore_params: bool = False,
) -> Callable[..., SimpleObject]:
    """Create keyed (takes args) cache-wrapped functions for sparse-scope testing."""

    @scope.cache(ignore_params=ignore_params)
    def get_object_sync(x: int = 0):
        call_counter()
        return SimpleObject(x)

    @scope.cache(ignore_params=ignore_params)
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
@pytest.mark.parametrize("scope_fixture", ["app_sparse", "req_sparse"])
def test_sparse_storage_cache_keyed(
    scope_fixture: str, func_type: TestType, request: pytest.FixtureRequest
):
    """
    Sparse-backed scopes cache keyed (argument-bearing) calls the same as dense-backed ones.

    Given: A sparse-backed shared or context scope with a cached function that takes args,
    When: The function is called multiple times with the same and different arguments,
    Then: Identical arguments hit the cache and distinct arguments produce new values.
    """
    # Arrange
    scope: Scope = request.getfixturevalue(scope_fixture)
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
@pytest.mark.parametrize("scope_fixture", ["app_sparse", "req_sparse"])
def test_sparse_storage_cache_slot_eligible(
    scope_fixture: str, func_type: TestType, request: pytest.FixtureRequest
):
    """
    Sparse-backed scopes cache slot-eligible (no-arg) calls by writing the slot directly.

    Given: A sparse-backed shared or context scope with an ignore_params cached function,
    When: The function is called multiple times,
    Then: The same cached value is returned for every call without re-invoking the function.
    """
    # Arrange
    scope: Scope = request.getfixturevalue(scope_fixture)
    call_counter = Mock()
    cleanup_counter = Mock()
    get_object = callable_factory(
        func_type, scope, call_counter, cleanup_counter, ignore_params=True
    )

    with scope.activate():
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


def test_sparse_storage_shared_reset_between_activations(app_sparse: Scope):
    """
    A sparse-backed shared scope's slots are cleared, not merely stale, between activations.

    Given: A sparse-backed shared scope,
    When: A cached value is populated, the scope is deactivated, and reactivated,
    Then: The second activation misses the cache and recomputes the value.
    """
    # Arrange
    call_counter = Mock()

    @app_sparse.cache(ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    with app_sparse.activate():
        obj1 = get_object(1)
    with app_sparse.activate():
        obj2 = get_object(2)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2


def test_sparse_storage_context_fresh_per_activation(req_sparse: Scope):
    """
    A sparse-backed context scope gets an independent slot dict for every activation.

    Given: A sparse-backed context scope,
    When: A cached value is populated, the scope is deactivated, and reactivated,
    Then: The second activation misses the cache and recomputes the value.
    """
    # Arrange
    call_counter = Mock()

    @req_sparse.cache(ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    with req_sparse.activate():
        obj1 = get_object(1)
    with req_sparse.activate():
        obj2 = get_object(2)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2


@pytest.mark.parametrize("scope_fixture", ["app_sparse", "req_sparse"])
def test_sparse_storage_exit_stack_cleaned_up(scope_fixture: str, request: pytest.FixtureRequest):
    """
    A sparse-backed scope's exit stack is closes on deactivation.

    Given: A sparse-backed scope with a registered resource,
    When: The resource is entered and the scope is deactivated,
    Then: The exit stack cleans up the resource, same as a dense-backed scope would.
    """
    # Arrange
    scope: Scope = request.getfixturevalue(scope_fixture)
    mock = Mock()

    @scope.cache()
    @resource
    def test_resource():
        try:
            yield
        finally:
            mock()

    # Act
    with scope.activate():
        test_resource()
        test_resource()

    # Assert
    mock.assert_called_once()


@pytest.mark.parametrize("scope_fixture", ["app_sparse", "req_sparse"])
def test_sparse_storage_manual_activate_deactivate(
    scope_fixture: str, request: pytest.FixtureRequest
):
    """
    The manual activate()/deactivate() API resets sparse-backed scopes the same way `with` does.

    Given: A sparse-backed scope activated and deactivated manually rather than via `with`,
    When: A cached value is populated, the scope is deactivated, and then reactivated,
    Then: The second activation misses the cache, same as with the context manager API.
    """
    # Arrange
    scope: Scope = request.getfixturevalue(scope_fixture)
    call_counter = Mock()

    @scope.cache(ignore_params=True)
    def get_object(x: int = 0):
        call_counter()
        return SimpleObject(x)

    # Act
    activation = scope.activate()
    obj1 = get_object(1)
    scope.deactivate(activation)

    activation = scope.activate()
    obj2 = get_object(2)
    scope.deactivate(activation)

    # Assert
    assert obj1 is not obj2
    assert obj2.value == 2
    assert call_counter.call_count == 2
