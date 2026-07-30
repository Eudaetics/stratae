"""
Unit tests for BaseScope.is_active/is_shared/activation_var, exercised via Scope.

These are inherited unchanged by AsyncScope, so testing them through Scope covers both -
there is no separate async test module for this.
"""

from contextvars import ContextVar

from stratae.lifecycle._scope2 import Scope
from stratae.lifecycle._slots import SharedVar


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


def test_is_shared_true_for_shared_isolation():
    """
    is_shared returns True for a shared-isolation scope.

    Given: A Scope constructed with isolation="shared",
    When: is_shared is called,
    Then: it returns True.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    assert scope.is_shared()


def test_is_shared_false_for_context_isolation():
    """
    is_shared returns False for a context-isolation scope.

    Given: A Scope constructed with isolation="context",
    When: is_shared is called,
    Then: it returns False.
    """
    # Arrange
    scope = Scope("request", "context")

    # Act & Assert
    assert not scope.is_shared()


def test_activation_var_is_shared_var_for_shared_isolation():
    """
    activation_var returns a SharedVar for a shared-isolation scope.

    Given: A Scope constructed with isolation="shared",
    When: activation_var is called,
    Then: it returns a SharedVar.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    assert isinstance(scope.activation_var(), SharedVar)


def test_activation_var_is_context_var_for_context_isolation():
    """
    activation_var returns a ContextVar for a context-isolation scope.

    Given: A Scope constructed with isolation="context",
    When: activation_var is called,
    Then: it returns a ContextVar.
    """
    # Arrange
    scope = Scope("request", "context")

    # Act & Assert
    assert isinstance(scope.activation_var(), ContextVar)
