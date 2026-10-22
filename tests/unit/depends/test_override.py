"""Test suite for the override tool in the dependency injection module."""

import contextvars
import threading
from unittest.mock import AsyncMock, Mock

import pytest

from stratae.depends import override, overrides
from stratae.depends._provide import Provider
from stratae.depends.exceptions import DependencyNotFoundError


def test_override_returns_value_while_active():
    """
    Provide should return the override value while the override is active.

    Given: a Provider for a dependency,
    When: it is overridden inside a with block,
    Then: provide should return the override value instead of the dependency's result.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep, "overridden"):
        assert depends.provide() == "overridden"


async def test_override_returns_value_while_active_async():
    """
    Provide should return the override value while the override is active, for an async dependency.

    Given: a Provider for an async dependency,
    When: it is overridden inside a with block,
    Then: awaiting provide should return the override value instead of the dependency's result.
    """
    # Arrange
    dep = AsyncMock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep, "overridden"):
        assert await depends.provide() == "overridden"


async def test_override_supports_repeated_provide_calls_async():
    """
    Provide should return the override value on every call, not just the first, for an async dep.

    Given: a Provider for an async dependency,
    When: it is overridden inside a with block and provide is awaited more than once,
    Then: every call should return the override value.
    """
    # Arrange
    dep = AsyncMock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep, "overridden"):
        assert await depends.provide() == "overridden"
        assert await depends.provide() == "overridden"


def test_override_restores_dependency_after_exit():
    """
    Provide should fall back to the dependency after the override exits.

    Given: a Provider for a dependency,
    When: an override is entered and then exited,
    Then: provide should call the real dependency again.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act
    with override(dep, "overridden"):
        ...

    # Assert
    assert depends.provide() == "real"


def test_override_does_not_call_dependency_while_active():
    """
    The real dependency should not be invoked while an override is active.

    Given: a Provider for a dependency,
    When: it is overridden inside a with block and provide is called,
    Then: the real dependency should not be invoked.
    """
    # Arrange
    dep = Mock()
    depends = Provider(dep)

    # Act
    with override(dep, "overridden"):
        depends.provide()

    # Assert
    dep.assert_not_called()


def test_override_tracks_active_count():
    """
    override_count should reflect the number of active overrides.

    Given: a Provider for a dependency,
    When: an override is entered and then exited,
    Then: override_count should increment on entry and decrement back on exit.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    assert depends.override_count == 0

    with override(dep, "overridden"):
        assert depends.override_count == 1

    assert depends.override_count == 0


def test_nested_override_same_dependency():
    """
    Nested overrides on the same dependency should resolve and restore correctly.

    Given: a Provider for a dependency,
    When: it is overridden inside another override with a different value,
    Then: provide should reflect the innermost value while nested, and fall back
    to the outer value and then the real dependency as each override exits.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep, "outer"):
        assert depends.provide() == "outer"

        with override(dep, "inner"):
            assert depends.provide() == "inner"

        assert depends.provide() == "outer"

    assert depends.provide() == "real"


def test_override_dict_overrides_all_while_active():
    """
    All deps in the mapping should return their override values while active.

    Given: two Providers for separate dependencies,
    When: both are overridden together via a dict,
    Then: both should return their override values inside the with block.
    """
    # Arrange
    dep_a = Mock(return_value="real-a")
    dep_b = Mock(return_value="real-b")
    wrapper_a = Provider(dep_a)
    wrapper_b = Provider(dep_b)

    # Act & Assert
    with overrides({dep_a: "override-a", dep_b: "override-b"}):
        assert wrapper_a.provide() == "override-a"
        assert wrapper_b.provide() == "override-b"


def test_override_dict_restores_all_after_exit():
    """
    All deps in the mapping should be restored to their real values after the block exits.

    Given: two Providers for separate dependencies,
    When: both are overridden together and the block exits,
    Then: both should call their real dependencies again.
    """
    # Arrange
    dep_a = Mock(return_value="real-a")
    dep_b = Mock(return_value="real-b")
    wrapper_a = Provider(dep_a)
    wrapper_b = Provider(dep_b)

    # Act
    with overrides({dep_a: "override-a", dep_b: "override-b"}):
        ...

    # Assert
    assert wrapper_a.provide() == "real-a"
    assert wrapper_b.provide() == "real-b"


def test_override_dict_does_not_call_dependencies_while_active():
    """
    The real dependencies should not be invoked while the dict override is active.

    Given: two Providers for separate dependencies,
    When: both are overridden together and provide is called on each,
    Then: neither real dependency should be invoked.
    """
    # Arrange
    dep_a = Mock()
    dep_b = Mock()
    wrapper_a = Provider(dep_a)
    wrapper_b = Provider(dep_b)

    # Act
    with overrides({dep_a: "override-a", dep_b: "override-b"}):
        wrapper_a.provide()
        wrapper_b.provide()

    # Assert
    dep_a.assert_not_called()
    dep_b.assert_not_called()


async def test_override_dict_with_async_dependency():
    """
    A dict override containing an async dependency should wrap the value as awaitable.

    Given: a Provider for an async dependency,
    When: it is overridden via a dict,
    Then: awaiting provide should return the override value.
    """
    # Arrange
    dep = AsyncMock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with overrides({dep: "overridden"}):
        assert await depends.provide() == "overridden"


def test_overrides_unwinds_entered_on_partial_failure():
    """
    If __enter__ fails partway through, already-entered overrides should be unwound.

    Given: two Providers where the second dep's lock raises on entry,
    When: overrides.__enter__ fails on the second dep,
    Then: the first dep's override should be cleaned up and provide restored.
    """
    # Arrange
    dep_a = Mock(return_value="real-a")
    dep_b = Mock(return_value="real-b")
    wrapper_a = Provider(dep_a)
    wrapper_b = Provider(dep_b)

    failing_lock = Mock()
    failing_lock.__enter__ = Mock(side_effect=RuntimeError("forced failure"))
    failing_lock.__exit__ = Mock(return_value=False)
    wrapper_b.lock = failing_lock

    # Act
    with pytest.raises(RuntimeError):
        with overrides({dep_a: "override-a", dep_b: "override-b"}):
            ...

    # Assert
    assert wrapper_a.provide() == "real-a"
    assert wrapper_a.override_count == 0


def test_override_raises_for_unregistered_dependency():
    """
    Override should raise for a dependency that was never wrapped.

    Given: a dependency that has never been wrapped via Provider,
    When: override is called with that dependency,
    Then: it should raise DependencyNotFoundError.
    """
    # Arrange
    dep = Mock()

    # Act & Assert
    with pytest.raises(DependencyNotFoundError):
        override(dep, "value")


def test_overrides_raises_for_empty_mapping():
    """
    overrides() should raise if given an empty mapping.

    Given: an empty dict,
    When: overrides is called with it,
    Then: it should raise ValueError.
    """
    with pytest.raises(ValueError):
        overrides({})


def test_override_thread_isolation():
    """
    Concurrent overrides on the same dependency should be isolated per thread.

    Given: a Provider for a dependency shared across threads,
    When: two threads each override it with a different value at the same time,
    Then: each thread should only observe its own override value, and the
    dependency should be restored once both overrides have exited.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    results: dict[str, str] = {}
    barrier = threading.Barrier(2)

    def run(name: str, value: str):
        with override(dep, value):
            barrier.wait()
            results[name] = depends.provide()

    thread_a = threading.Thread(target=run, args=("a", "value-a"))
    thread_b = threading.Thread(target=run, args=("b", "value-b"))

    # Act
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    # Assert
    assert results["a"] == "value-a"
    assert results["b"] == "value-b"
    assert depends.provide() == "real"


def test_override_can_be_reentered_while_held():
    """
    The same override object can be entered again while it's already held.

    Given: an override context manager for a dependency,
    When: it is entered, entered again inside itself, and both are exited,
    Then: the override applies throughout, and the real dependency is restored at the end.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    ctx = override(dep, "fake")

    # Act & Assert
    with ctx:
        with ctx:
            assert depends.provide() == "fake"
        assert depends.provide() == "fake"
    assert depends.provide() == "real"


def test_overrides_can_be_reentered_while_held():
    """
    The same overrides object can be entered again while it's already held.

    Given: an overrides context manager for two dependencies,
    When: it is entered, entered again inside itself, and both are exited,
    Then: both overrides apply throughout, and the real dependencies are restored at the end.
    """
    # Arrange
    dep_a, dep_b = Mock(return_value="real_a"), Mock(return_value="real_b")
    depends_a, depends_b = Provider(dep_a), Provider(dep_b)
    ctx = overrides({dep_a: "fake_a", dep_b: "fake_b"})

    # Act & Assert
    with ctx:
        with ctx:
            assert (depends_a.provide(), depends_b.provide()) == ("fake_a", "fake_b")
        assert (depends_a.provide(), depends_b.provide()) == ("fake_a", "fake_b")
    assert (depends_a.provide(), depends_b.provide()) == ("real_a", "real_b")


def test_override_exit_in_other_context_still_restores_provide():
    """
    An override exited outside the context that entered it should still stop counting itself.

    Given: an override entered in one context,
    When: it is exited from a copied context, where resetting its token fails,
    Then: the reset error propagates, and provide is restored to the real dependency.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    ctx = override(dep, "fake")
    ctx.__enter__()

    # Act
    with pytest.raises(ValueError):
        contextvars.copy_context().run(ctx.__exit__)

    # Assert
    assert depends.override_count == 0
    assert depends.provide() == "real"
