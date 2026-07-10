"""Test suite for the override tool in the dependency injection module."""

import threading
from unittest.mock import Mock

import pytest

from stratae.depends import DependsWrapper, override
from stratae.depends.exceptions import DependencyNotFoundError


def test_override_returns_value_while_active():
    """
    Provide should return the override value while the override is active.

    Given: a DependsWrapper for a dependency,
    When: it is overridden inside a with block,
    Then: provide should return the override value instead of the dependency's result.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = DependsWrapper(dep)

    # Act & Assert
    with override(dep, "overridden"):
        assert depends.provide() == "overridden"


def test_override_restores_dependency_after_exit():
    """
    Provide should fall back to the dependency after the override exits.

    Given: a DependsWrapper for a dependency,
    When: an override is entered and then exited,
    Then: provide should call the real dependency again.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = DependsWrapper(dep)

    # Act
    with override(dep, "overridden"):
        ...

    # Assert
    assert depends.provide() == "real"


def test_override_does_not_call_dependency_while_active():
    """
    The real dependency should not be invoked while an override is active.

    Given: a DependsWrapper for a dependency,
    When: it is overridden inside a with block and provide is called,
    Then: the real dependency should not be invoked.
    """
    # Arrange
    dep = Mock()
    depends = DependsWrapper(dep)

    # Act
    with override(dep, "overridden"):
        depends.provide()

    # Assert
    dep.assert_not_called()


def test_override_tracks_active_count():
    """
    override_count should reflect the number of active overrides.

    Given: a DependsWrapper for a dependency,
    When: an override is entered and then exited,
    Then: override_count should increment on entry and decrement back on exit.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = DependsWrapper(dep)

    # Act & Assert
    assert depends.override_count == 0

    with override(dep, "overridden"):
        assert depends.override_count == 1

    assert depends.override_count == 0


def test_nested_override_same_dependency():
    """
    Nested overrides on the same dependency should resolve and restore correctly.

    Given: a DependsWrapper for a dependency,
    When: it is overridden inside another override with a different value,
    Then: provide should reflect the innermost value while nested, and fall back
    to the outer value and then the real dependency as each override exits.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = DependsWrapper(dep)

    # Act & Assert
    with override(dep, "outer"):
        assert depends.provide() == "outer"

        with override(dep, "inner"):
            assert depends.provide() == "inner"

        assert depends.provide() == "outer"

    assert depends.provide() == "real"


def test_override_raises_for_unregistered_dependency():
    """
    Override should raise for a dependency that was never wrapped.

    Given: a dependency that has never been wrapped via DependsWrapper,
    When: override is called with that dependency,
    Then: it should raise DependencyNotFoundError.
    """
    # Arrange
    dep = Mock()

    # Act & Assert
    with pytest.raises(DependencyNotFoundError):
        override(dep, "value")


def test_override_thread_isolation():
    """
    Concurrent overrides on the same dependency should be isolated per thread.

    Given: a DependsWrapper for a dependency shared across threads,
    When: two threads each override it with a different value at the same time,
    Then: each thread should only observe its own override value, and the
    dependency should be restored once both overrides have exited.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = DependsWrapper(dep)
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
