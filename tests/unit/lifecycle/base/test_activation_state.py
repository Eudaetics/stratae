"""
Unit tests for BaseScope.is_active, and the activation holder a scope's SlotStore keeps.

These are inherited unchanged by AsyncScope, so testing them through Scope covers both -
there is no separate async test module for this.
"""

from contextvars import ContextVar

from stratae.lifecycle._slots import SharedVar, SlotStore
from stratae.lifecycle.scope import Scope


def test_is_active_false_before_activation():
    """
    is_active returns False before the scope has ever been activated.

    Given: A freshly constructed Scope,
    When: is_active is called,
    Then: it returns False.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    assert not scope.is_active()


def test_is_active_true_while_activated():
    """
    is_active returns True while the scope has a live activation.

    Given: A Scope that has been activated,
    When: is_active is called,
    Then: it returns True.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act
    scope.activate()

    # Assert
    assert scope.is_active()


def test_is_active_false_after_deactivation():
    """
    is_active returns False again once the scope has been deactivated.

    Given: A Scope that was activated and then deactivated,
    When: is_active is called,
    Then: it returns False.
    """
    # Arrange
    scope = Scope("application", "shared")
    activation = scope.activate()

    # Act
    scope.deactivate(activation)

    # Assert
    assert not scope.is_active()


def test_store_holds_shared_var_for_shared_isolation():
    """
    A shared scope's slot store holds its activation in a SharedVar.

    Given: A slot store for a shared scope,
    When: Its activation holder is read,
    Then: it is a SharedVar, with the lock that guards activation.
    """
    # Arrange
    store = SlotStore("application", shared=True, dense=True, threads=False)

    # Act & Assert
    assert isinstance(store.var, SharedVar)
    assert store.lock is store.var.lock


def test_store_holds_context_var_for_context_isolation():
    """
    A context scope's slot store holds its activation in a ContextVar.

    Given: A slot store for a context scope,
    When: Its activation holder is read,
    Then: it is a ContextVar, with no lock.
    """
    # Arrange
    store = SlotStore("request", shared=False, dense=True, threads=False)

    # Act & Assert
    assert isinstance(store.var, ContextVar)
    assert store.lock is None
