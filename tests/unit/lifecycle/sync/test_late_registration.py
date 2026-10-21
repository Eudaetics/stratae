"""Unit tests for cached functions registered while a Scope activation is already live."""

import contextvars
from collections.abc import Callable

import pytest

from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope, StorageType


@pytest.mark.parametrize("storage", ["dense", "sparse"])
def test_function_registered_outside_live_activation_is_cached(storage: StorageType):
    """
    A cached function registered from another context works in an already-live activation.

    Given: A context scope activation live in the current context,
    When: a new cached function is registered from a context that can't see that activation,
        then called within it,
    Then: the call should compute its value, and a second call should return the cached one.
    """
    # Arrange
    scope = Scope("request", storage=storage)
    calls = 0
    registry: dict[str, Callable[[], object]] = {}

    def register() -> None:
        @scope.cache()
        def get_late() -> object:
            nonlocal calls
            calls += 1
            return object()

        registry["late"] = get_late

    # Act
    with scope.activate():
        contextvars.Context().run(register)
        first = registry["late"]()
        second = registry["late"]()

    # Assert
    assert first is second
    assert calls == 1


def test_resource_registered_outside_live_activation_is_entered_and_cleaned_up():
    """
    A cached resource registered from another context is entered once and cleaned up.

    Given: A context scope activation live in the current context,
    When: a new cached resource is registered from a context that can't see that activation,
        then used within it,
    Then: it should be entered once, and exited when the activation ends.
    """
    # Arrange
    scope = Scope("request")
    events: list[str] = []
    registry: dict[str, Callable[[], object]] = {}

    def register() -> None:
        @scope.cache()
        @resource
        def get_connection():
            events.append("open")
            try:
                yield object()
            finally:
                events.append("close")

        registry["late"] = get_connection

    # Act
    with scope.activate():
        contextvars.Context().run(register)
        first = registry["late"]()
        second = registry["late"]()

    # Assert
    assert first is second
    assert events == ["open", "close"]
