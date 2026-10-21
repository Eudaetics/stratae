"""Tests for shared-scope cache races - concurrent threads computing the same cached slot."""

import threading
import time
from typing import Callable

from stratae.lifecycle.exceptions import CacheCycleError
from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope


def test_shared_scope_slot_eligible_computed_once_under_thread_contention(scope: Scope):
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

    @scope.cache()
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
    with scope.activate():
        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    # Assert
    assert call_count == 1
    assert len(results) == thread_count
    assert len({id(value) for value in results}) == 1


def test_shared_scope_keyed_computed_once_per_key_under_thread_contention(scope: Scope):
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

    @scope.cache()
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
    with scope.activate():
        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


def test_shared_scope_resource_entered_once_under_thread_contention(scope: Scope):
    """
    Verify a resource-tagged cached function in a shared scope is entered exactly once.

    Given: several threads all calling a shared-scope cached resource for the first time,
    When: they race to enter and cache it,
    Then: the resource should be entered exactly once, and cleaned up exactly once when the
          scope deactivates.
    """
    # Arrange
    enter_count = 0
    cleanup_count = 0
    thread_count = 20
    start = threading.Barrier(thread_count)

    @scope.cache()
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
    with scope.activate():
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


def test_shared_scope_reentrant_dependency_does_not_deadlock(scope: Scope):
    """
    Cached function calling another cached function in the same shared scope doesn't deadlock.

    Given: a cached function whose first computation calls a second cached function in the
           same shared scope,
    When: the outer function is called for the first time,
    Then: both should compute without deadlocking.
    """

    # Arrange
    @scope.cache()
    def inner() -> int:
        return 1

    @scope.cache()
    def outer() -> int:
        return inner() + 1

    result: list[int] = []

    def call_outer():
        with scope.activate():
            result.append(outer())

    # Act
    thread = threading.Thread(target=call_outer, daemon=True)
    thread.start()
    thread.join(timeout=2)

    # Assert
    assert not thread.is_alive(), "outer() deadlocked calling inner() in the same shared scope"
    assert result == [2]


def _run_with_timeout(target: Callable[[], object], timeout: float = 2) -> threading.Thread:
    """Run target on a daemon thread, so a deadlock fails the test instead of hanging it."""
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    return thread


def test_shared_scope_keyed_recursion_does_not_deadlock(scope: Scope):
    """
    A keyed cached function recursing into itself with other keys doesn't deadlock.

    Given: a shared-scope cached fib(n) whose first computation calls fib(n - 1) and fib(n - 2),
    When: it's called for the first time,
    Then: every key should compute without deadlocking, each exactly once.
    """
    # Arrange
    computed: list[int] = []

    @scope.cache()
    def fib(n: int) -> int:
        computed.append(n)
        return n if n < 2 else fib(n - 1) + fib(n - 2)

    result: list[int] = []

    def call_fib():
        with scope.activate():
            result.append(fib(10))

    # Act
    thread = _run_with_timeout(call_fib)

    # Assert
    assert not thread.is_alive(), "fib() deadlocked recursing into itself in a shared scope"
    assert result == [55]
    assert sorted(computed) == list(range(11))


def test_shared_scope_mutual_recursion_does_not_deadlock(scope: Scope):
    """
    Two keyed cached functions calling each other with different keys don't deadlock.

    Given: shared-scope cached functions a(n) -> b(n - 1) -> a(n - 1),
    When: a is called for the first time,
    Then: the chain should resolve without deadlocking.
    """

    # Arrange
    @scope.cache()
    def a(n: int) -> int:
        return 0 if n == 0 else b(n - 1) + 1

    @scope.cache()
    def b(n: int) -> int:
        return a(n)

    result: list[int] = []

    def call_a():
        with scope.activate():
            result.append(a(3))

    # Act
    thread = _run_with_timeout(call_a)

    # Assert
    assert not thread.is_alive(), "a() -> b() -> a() deadlocked in a shared scope"
    assert result == [3]


def test_shared_scope_cross_thread_dependencies_do_not_deadlock(scope: Scope):
    """
    Two threads each computing one function that calls the other don't deadlock.

    Given: shared-scope cached functions a and b that each call the other with a new key,
    When: one thread is inside a(1) and another inside b(1) at the same time,
    Then: both calls should finish - neither function's computation blocks the other.
    """
    # Arrange
    both_inside = threading.Barrier(2, timeout=2)

    @scope.cache()
    def a(n: int) -> int:
        if n == 1:
            both_inside.wait()
            return b(2)
        return n

    @scope.cache()
    def b(n: int) -> int:
        if n == 1:
            both_inside.wait()
            return a(2)
        return n

    results: list[int] = []

    # Act
    with scope.activate():
        thread_a = threading.Thread(target=lambda: results.append(a(1)), daemon=True)
        thread_b = threading.Thread(target=lambda: results.append(b(1)), daemon=True)
        thread_a.start()
        thread_b.start()
        thread_a.join(timeout=2)
        thread_b.join(timeout=2)

    # Assert
    assert not thread_a.is_alive() and not thread_b.is_alive(), "a/b deadlocked across threads"
    assert results == [2, 2]


def test_shared_scope_self_dependent_slot_raises_cycle_error(scope: Scope):
    """
    A slot-eligible cached function calling itself raises instead of deadlocking.

    Given: a shared-scope ignore_params function whose computation calls itself,
    When: it's called for the first time,
    Then: the inner call should raise CacheCycleError, and the slot should stay unclaimed.
    """
    # Arrange
    recurse = True

    @scope.cache(ignore_params=True)
    def countdown(n: int) -> int:
        return countdown(n - 1) if recurse else n

    outcomes: list[object] = []

    def call_countdown():
        nonlocal recurse
        with scope.activate():
            try:
                countdown(3)
            except CacheCycleError as exc:
                outcomes.append(exc)
            recurse = False
            outcomes.append(countdown(5))

    # Act
    thread = _run_with_timeout(call_countdown)

    # Assert
    assert not thread.is_alive(), "countdown() deadlocked waiting on itself"
    assert isinstance(outcomes[0], CacheCycleError)
    assert outcomes[1] == 5


def test_shared_scope_waiter_recomputes_after_owner_fails(scope: Scope):
    """
    A caller waiting on another thread's failed computation computes the value itself.

    Given: one thread computing a shared-scope cached function, and a second thread
           waiting on that same entry,
    When: the first thread's computation raises,
    Then: the first should see its exception, and the waiter should compute and cache the
          value itself.
    """
    # Arrange
    owner_started = threading.Event()
    release_owner = threading.Event()
    calls = 0

    @scope.cache()
    def get_thing() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            owner_started.set()
            release_owner.wait(timeout=2)
            raise RuntimeError("first computation fails")
        return 42

    outcomes: dict[str, object] = {}

    def owner():
        try:
            get_thing()
        except RuntimeError as exc:
            outcomes["owner"] = exc

    def waiter():
        outcomes["waiter"] = get_thing()

    # Act
    with scope.activate():
        owner_thread = threading.Thread(target=owner, daemon=True)
        owner_thread.start()
        owner_started.wait(timeout=2)
        waiter_thread = threading.Thread(target=waiter, daemon=True)
        waiter_thread.start()
        time.sleep(0.05)  # let the waiter reach the claimed entry before the owner fails
        release_owner.set()
        owner_thread.join(timeout=2)
        waiter_thread.join(timeout=2)
        cached = get_thing()

    # Assert
    assert isinstance(outcomes["owner"], RuntimeError)
    assert outcomes["waiter"] == 42
    assert cached == 42
    assert calls == 2
