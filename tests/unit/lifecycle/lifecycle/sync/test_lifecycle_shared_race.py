"""Tests for shared-scope cache races - concurrent threads computing the same cached slot."""

import threading
import time

from stratae.lifecycle import Lifecycle, resource


def test_shared_scope_slot_eligible_computed_once_under_thread_contention(lifecycle: Lifecycle):
    """
    Verify a no-arg cached function in a shared scope is computed exactly once.

    Given: several threads all calling a shared-scope cached function for the first time,
    When: they race to fill the same slot,
    Then: the underlying function should run exactly once, and every thread should see the
          same cached value.
    """
    # Arrange
    call_count = 0
    thread_count = 20
    start = threading.Barrier(thread_count)

    @lifecycle.cache("application")
    def get_thing() -> object:
        nonlocal call_count
        call_count += 1
        time.sleep(0.01)
        return object()

    results: list[object] = []
    results_lock = threading.Lock()

    def worker():
        start.wait()
        value = get_thing()
        with results_lock:
            results.append(value)

    # Act
    with lifecycle.start("application"):
        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    # Assert
    assert call_count == 1
    assert len(results) == thread_count
    assert len({id(value) for value in results}) == 1


def test_shared_scope_keyed_computed_once_per_key_under_thread_contention(lifecycle: Lifecycle):
    """
    Verify a keyed cached function in a shared scope computes each key exactly once.

    Given: several threads all calling a shared-scope cached function with the same argument,
    When: they race to fill the same cache entry,
    Then: the underlying function should run exactly once for that key.
    """
    # Arrange
    call_count = 0
    thread_count = 20
    start = threading.Barrier(thread_count)

    @lifecycle.cache("application")
    def get_thing(x: int) -> object:
        nonlocal call_count
        call_count += 1
        time.sleep(0.01)
        return object()

    results: list[object] = []
    results_lock = threading.Lock()

    def worker():
        start.wait()
        value = get_thing(1)
        with results_lock:
            results.append(value)

    # Act
    with lifecycle.start("application"):
        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


def test_shared_scope_resource_entered_once_under_thread_contention(lifecycle: Lifecycle):
    """
    Verify a resource-tagged cached function in a shared scope is entered exactly once.

    Given: several threads all calling a shared-scope cached resource for the first time,
    When: they race to enter and cache it,
    Then: the resource should be entered exactly once, and cleaned up exactly once when the
          scope exits.
    """
    # Arrange
    enter_count = 0
    cleanup_count = 0
    thread_count = 20
    start = threading.Barrier(thread_count)

    @lifecycle.cache("application")
    @resource
    def get_resource():
        nonlocal enter_count, cleanup_count
        enter_count += 1
        time.sleep(0.01)
        yield object()
        cleanup_count += 1

    results: list[object] = []
    results_lock = threading.Lock()

    def worker():
        start.wait()
        value = get_resource()
        with results_lock:
            results.append(value)

    # Act
    with lifecycle.start("application"):
        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert cleanup_count == 0

    # Assert
    assert enter_count == 1
    assert cleanup_count == 1
    assert len({id(value) for value in results}) == 1


def test_shared_scope_reentrant_dependency_does_not_deadlock(lifecycle: Lifecycle):
    """
    Cached function calling another cached function in the same shared scope doesn't deadlock.

    Given: a cached function whose first computation calls a second cached function in the
           same shared scope,
    When: the outer function is called for the first time,
    Then: both should compute without deadlocking.
    """

    # Arrange
    @lifecycle.cache("application")
    def inner() -> int:
        return 1

    @lifecycle.cache("application")
    def outer() -> int:
        return inner() + 1

    result: list[int] = []

    def call_outer():
        with lifecycle.start("application"):
            result.append(outer())

    # Act
    thread = threading.Thread(target=call_outer, daemon=True)
    thread.start()
    thread.join(timeout=2)

    # Assert
    assert not thread.is_alive(), "outer() deadlocked calling inner() in the same shared scope"
    assert result == [2]
