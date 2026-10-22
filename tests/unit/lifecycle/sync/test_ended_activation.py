"""Unit tests for reaching an ended Scope activation from a context copied while it was live."""

import contextvars
import threading
import time

import pytest

from stratae.lifecycle._slots import SharedVar
from stratae.lifecycle.exceptions import ScopeActivationError, ScopeInactiveError
from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope, StorageType


@pytest.mark.parametrize("storage", ["dense", "sparse"])
def test_call_from_copied_context_after_activation_ends_raises(storage: StorageType):
    """
    A cached call from a context that outlived its activation raises instead of reusing it.

    Given: A context copied while a context scope's activation was live, which has since ended,
    When: a cached function of that scope is called within the copied context,
    Then: ScopeInactiveError should be raised, and the scope should read as inactive there.
    """
    # Arrange
    context_scope = Scope("request", storage=storage)

    @context_scope.cache()
    def get_value() -> object:
        return object()

    with context_scope:
        get_value()
        copied = contextvars.copy_context()

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="activation ended"):
        copied.run(get_value)
    assert not copied.run(context_scope.is_active)


def test_keyed_call_from_copied_context_after_activation_ends_raises(context_scope: Scope):
    """
    A keyed cached call from a context that outlived its activation raises.

    Given: A context copied while a context scope's activation was live, which has since ended,
    When: a keyed cached function is called within it, with a key cached during the activation,
    Then: ScopeInactiveError should be raised rather than the ended activation's value returned.
    """

    # Arrange
    @context_scope.cache()
    def get_item(key: str) -> object:
        return object()

    with context_scope:
        get_item("a")
        copied = contextvars.copy_context()

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="activation ended"):
        copied.run(get_item, "a")


def test_activate_requiring_ended_parent_from_copied_context_raises():
    """
    A child can't activate against a parent activation that has already ended.

    Given: A context copied while a parent scope's activation was live, which has since ended,
    When: a scope requiring that parent is activated within the copied context,
    Then: ScopeActivationError should be raised.
    """
    # Arrange
    parent = Scope("session")
    child = Scope("request", requires=parent)
    with parent:
        copied = contextvars.copy_context()

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="required scope 'session' is not active"):
        copied.run(child.activate)


def test_resource_entered_after_activation_ends_is_exited_at_once(context_scope: Scope):
    """
    A resource whose entry finishes only after its activation ended is exited, not leaked.

    Given: A thread, running a context copied from a live activation, part-way through
        entering a cached resource,
    When: the activation ends before that entry finishes,
    Then: the resource should be exited as soon as its entry finishes, and the thread's call
          should raise ScopeInactiveError.
    """
    # Arrange
    entering = threading.Event()
    finish_entering = threading.Event()
    events: list[str] = []
    errors: list[ScopeInactiveError] = []

    @context_scope.cache()
    @resource
    def get_connection():
        entering.set()
        finish_entering.wait(timeout=2)
        events.append("open")
        try:
            yield object()
        finally:
            events.append("close")

    def enter_in_thread(copied: contextvars.Context) -> None:
        try:
            copied.run(get_connection)
        except ScopeInactiveError as exc:
            errors.append(exc)

    # Act
    with context_scope:
        thread = threading.Thread(target=enter_in_thread, args=(contextvars.copy_context(),))
        thread.start()
        entering.wait(timeout=2)
    finish_entering.set()
    thread.join(timeout=2)

    # Assert
    assert events == ["open", "close"]
    assert len(errors) == 1


def test_shared_scope_waiter_raises_when_activation_ends_while_it_waits(scope: Scope):
    """
    A thread waiting on another's computation raises if the activation ends meanwhile.

    Given: One thread computing a shared-scope cached function, and a second thread waiting
        on that same entry,
    When: the scope deactivates before the computation finishes,
    Then: the computing thread should still get its value, and the waiting thread should
          raise ScopeInactiveError rather than reuse the ended activation.
    """
    # Arrange
    computing = threading.Event()
    finish_computing = threading.Event()
    outcomes: dict[str, object] = {}

    @scope.cache()
    def get_value() -> str:
        computing.set()
        finish_computing.wait(timeout=2)
        return "value"

    def owner() -> None:
        outcomes["owner"] = get_value()

    def waiter() -> None:
        try:
            get_value()
        except ScopeInactiveError as exc:
            outcomes["waiter"] = exc

    activation = scope.activate()
    owner_thread = threading.Thread(target=owner)
    owner_thread.start()
    computing.wait(timeout=2)
    waiter_thread = threading.Thread(target=waiter)
    waiter_thread.start()
    time.sleep(0.05)  # let the waiter reach the claimed entry

    # Act
    scope.deactivate(activation)
    finish_computing.set()
    owner_thread.join(timeout=2)
    waiter_thread.join(timeout=2)

    # Assert
    assert outcomes["owner"] == "value"
    assert isinstance(outcomes["waiter"], ScopeInactiveError)


@pytest.mark.parametrize("storage", ["dense", "sparse"])
@pytest.mark.parametrize("keyed", [False, True], ids=["slot", "keyed"])
def test_shared_call_reading_storage_emptied_by_deactivation_raises(
    storage: StorageType, keyed: bool
):
    """
    A shared-scope call holding storage that a concurrent deactivation emptied raises cleanly.

    Given: A shared scope's storage, read just before another thread deactivated the scope
        and emptied it - reproduced by pointing the SharedVar back at the emptied storage,
    When: a cached function of that scope reads its slot from that storage,
    Then: ScopeInactiveError should be raised, not the emptied storage's IndexError/KeyError.
    """
    # Arrange
    scope = Scope("application", "shared", storage)

    @scope.cache(ignore_params=not keyed)
    def get_value(key: str) -> object:
        return object()

    var = scope.activation_var()
    assert isinstance(var, SharedVar)
    activation = scope.activate()
    get_value("k")
    emptied = var.storage
    scope.deactivate(activation)
    var.storage = emptied

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="activation ended"):
        get_value("k")
