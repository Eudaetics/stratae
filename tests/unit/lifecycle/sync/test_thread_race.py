"""Tests for threads=True context scopes - threads sharing one activation via copied contexts."""

import threading
import time
from contextvars import copy_context
from typing import Any, Callable

import pytest

from stratae.lifecycle.exceptions import CacheCycleError, ScopeInactiveError
from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope, StorageType


def _run_in_threads(target: Callable[[], Any], count: int) -> list[Any]:
    """
    Call target on `count` threads, each in a copy of the calling context.

    The copies all hold the caller's activation, so the threads share it.
    """
    start = threading.Barrier(count)
    results: list[Any] = []
    results_lock = threading.Lock()

    def worker() -> None:
        start.wait()
        value = target()
        with results_lock:
            results.append(value)

    threads = [threading.Thread(target=copy_context().run, args=(worker,)) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


@pytest.mark.parametrize("storage", ["dense", "sparse"])
def test_threaded_slot_eligible_computed_once_under_thread_contention(storage: StorageType):
    """
    Verify a no-arg cached function is computed once across threads sharing an activation.

    Given: a threads=True context scope, and several threads sharing one activation,
    When: they all call a cached function for the first time,
    Then: the underlying function should run exactly once, and every thread should see the
          same value.
    """
    # Arrange
    scope = Scope("request", storage=storage, threads=True)
    call_count = 0

    @scope.cache()
    def get_thing() -> object:
        nonlocal call_count
        call_count += 1
        time.sleep(0.01)
        return object()

    # Act
    with scope:
        results = _run_in_threads(get_thing, 20)
        cached = get_thing()

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1
    assert cached is results[0]


def test_threaded_keyed_computed_once_per_key_under_thread_contention(threaded_scope: Scope):
    """
    Verify a keyed cached function computes each key once across threads sharing an activation.

    Given: a threads=True context scope, and several threads sharing one activation,
    When: they all call a keyed cached function with the same argument,
    Then: the underlying function should run exactly once for that key.
    """
    # Arrange
    call_count = 0

    @threaded_scope.cache()
    def get_thing(x: int) -> object:
        nonlocal call_count
        call_count += 1
        time.sleep(0.01)
        return object()

    # Act
    with threaded_scope:
        results = _run_in_threads(lambda: get_thing(1), 20)

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


def test_threaded_resource_entered_once_under_thread_contention(threaded_scope: Scope):
    """
    Verify a resource is entered once across threads sharing an activation.

    Given: a threads=True context scope, and several threads sharing one activation,
    When: they all call a cached resource for the first time,
    Then: it should be entered once, and cleaned up once when the scope deactivates.
    """
    # Arrange
    enter_count = 0
    cleanup_count = 0

    @threaded_scope.cache()
    @resource
    def get_resource():
        nonlocal enter_count, cleanup_count
        enter_count += 1
        time.sleep(0.01)
        yield object()
        cleanup_count += 1

    # Act
    with threaded_scope:
        results = _run_in_threads(get_resource, 20)
        assert cleanup_count == 0

    # Assert
    assert enter_count == 1
    assert cleanup_count == 1
    assert len({id(value) for value in results}) == 1


def test_threaded_separate_activations_compute_independently(threaded_scope: Scope):
    """
    Threads with their own activations don't wait on each other's computations.

    Given: a threads=True context scope, and two threads each with its own activation,
    When: both compute the same cached function at the same time,
    Then: each should compute and cache its own value, without one blocking the other.
    """
    # Arrange
    both_inside = threading.Barrier(2, timeout=2)

    @threaded_scope.cache()
    def get_thing() -> object:
        both_inside.wait()
        return object()

    results: list[object] = []

    def worker() -> None:
        with threaded_scope:
            results.append(get_thing())

    # Act
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    # Assert
    assert not any(thread.is_alive() for thread in threads), "activations blocked each other"
    assert len(results) == 2
    assert results[0] is not results[1]


def test_threaded_self_dependent_slot_raises_cycle_error(threaded_scope: Scope):
    """
    A slot-eligible cached function calling itself raises instead of deadlocking.

    Given: a threads=True ignore_params function whose computation calls itself,
    When: it's called for the first time,
    Then: the inner call should raise CacheCycleError, and the slot should stay unclaimed.
    """
    # Arrange
    recurse = True

    @threaded_scope.cache(ignore_params=True)
    def countdown(n: int) -> int:
        return countdown(n - 1) if recurse else n

    # Act
    with threaded_scope:
        with pytest.raises(CacheCycleError):
            countdown(3)
        recurse = False
        result = countdown(5)

    # Assert
    assert result == 5


def _start_owner_and_waiter(
    get_thing: Callable[[], Any], owner_started: threading.Event
) -> tuple[threading.Thread, threading.Thread, dict[str, Any]]:
    """
    Start an owner thread computing get_thing, then a waiter thread on the same entry.

    Both run in copies of the calling context, so they share its activation.
    """
    outcomes: dict[str, Any] = {}

    def call(name: str) -> None:
        try:
            outcomes[name] = get_thing()
        except BaseException as exc:
            outcomes[name] = exc

    owner = threading.Thread(target=copy_context().run, args=(call, "owner"), daemon=True)
    owner.start()
    owner_started.wait(timeout=2)
    waiter = threading.Thread(target=copy_context().run, args=(call, "waiter"), daemon=True)
    waiter.start()
    time.sleep(0.05)  # let the waiter reach the claimed entry
    return owner, waiter, outcomes


def test_threaded_waiter_recomputes_after_owner_fails(threaded_scope: Scope):
    """
    A thread waiting on another thread's failed computation computes the value itself.

    Given: one thread computing a cached function, and a second thread waiting on that entry,
    When: the first thread's computation raises,
    Then: the first should see its exception, and the waiter should compute and cache the
          value itself.
    """
    # Arrange
    owner_started = threading.Event()
    release_owner = threading.Event()
    calls = 0

    @threaded_scope.cache()
    def get_thing() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            owner_started.set()
            release_owner.wait(timeout=2)
            raise RuntimeError("first computation fails")
        return 42

    # Act
    with threaded_scope:
        owner, waiter, outcomes = _start_owner_and_waiter(get_thing, owner_started)
        release_owner.set()
        owner.join(timeout=2)
        waiter.join(timeout=2)
        cached = get_thing()

    # Assert
    assert isinstance(outcomes["owner"], RuntimeError)
    assert outcomes["waiter"] == 42
    assert cached == 42
    assert calls == 2


def test_threaded_waiter_raises_when_activation_ends(threaded_scope: Scope):
    """
    A thread waiting on a computation raises once the activation has ended.

    Given: one thread computing a cached function, and a second thread waiting on that entry,
    When: the activation ends before the computation finishes,
    Then: the owner should still get its value, and the waiter should raise
          ScopeInactiveError.
    """
    # Arrange
    owner_started = threading.Event()
    release_owner = threading.Event()

    @threaded_scope.cache()
    def get_thing() -> int:
        owner_started.set()
        release_owner.wait(timeout=2)
        return 42

    # Act
    with threaded_scope:
        owner, waiter, outcomes = _start_owner_and_waiter(get_thing, owner_started)
    release_owner.set()
    owner.join(timeout=2)
    waiter.join(timeout=2)

    # Assert
    assert outcomes["owner"] == 42
    assert isinstance(outcomes["waiter"], ScopeInactiveError)
