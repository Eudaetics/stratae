"""
Unit tests for Scope's `requires` parent relationship.

Net-new behavior compared to the old Lifecycle/Scope split: a scope can declare another
scope as its required parent. activate() raises immediately unless that parent is already
active, and the parent tracks a live-dependent count (reserved slot 1) that blocks its own
deactivation while a scope requiring it is still active.
"""

from unittest.mock import Mock

import pytest

from stratae.lifecycle._scope2 import Scope
from stratae.lifecycle.exceptions import ScopeActivationError
from stratae.lifecycle.resource import resource


def test_activate_without_active_parent_raises():
    """
    Activating a scope with an inactive required parent raises.

    Given: A child scope requiring a parent scope that has never been activated,
    When: the child is activated,
    Then: A ScopeActivationError is raised, naming both the child and the required parent.
    """
    # Arrange
    parent = Scope("application", "shared")
    child = Scope("request", requires=parent)

    # Act & Assert
    with pytest.raises(
        ScopeActivationError,
        match="Cannot activate 'request': required scope 'application' is not active.",
    ):
        child.activate()


def test_activate_with_active_parent_succeeds():
    """
    Activating a scope with an active required parent succeeds.

    Given: A parent scope that is active,
    When: a child scope requiring it is activated,
    Then: the child becomes active too.
    """
    # Arrange
    parent = Scope("application", "shared")
    child = Scope("request", requires=parent)

    # Act & Assert
    with parent.activate():
        with child.activate():
            assert child.is_active()


def test_deactivate_parent_while_child_active_raises():
    """
    Deactivating a parent while a scope requiring it is still active raises.

    Given: An active parent scope with an active child scope requiring it,
    When: the parent is deactivated,
    Then: A ScopeActivationError is raised and the parent remains active.
    """
    # Arrange
    parent = Scope("application", "shared")
    child = Scope("request", requires=parent)
    parent_activation = parent.activate()
    child.activate()

    # Act & Assert
    with pytest.raises(
        ScopeActivationError,
        match="Cannot deactivate 'application': a scope requiring it is still active.",
    ):
        parent.deactivate(parent_activation)
    assert parent.is_active()


def test_deactivate_parent_after_child_deactivated_succeeds():
    """
    Deactivating a parent succeeds once the child requiring it has been deactivated first.

    Given: An active parent scope with a child scope that required it, already deactivated,
    When: the parent is deactivated,
    Then: it succeeds and the parent is no longer active.
    """
    # Arrange
    parent = Scope("application", "shared")
    child = Scope("request", requires=parent)
    parent_activation = parent.activate()
    child_activation = child.activate()
    child.deactivate(child_activation)

    # Act
    parent.deactivate(parent_activation)

    # Assert
    assert not parent.is_active()


def test_multiple_children_of_same_parent():
    """
    A parent tracks more than one concurrently-active scope requiring it.

    Given: An active parent scope with two different child scopes requiring it, both active,
    When: only one child is deactivated,
    Then: the parent still cannot be deactivated, since the other child is still active.
    """
    # Arrange
    parent = Scope("application", "shared")
    child_a = Scope("session", requires=parent)
    child_b = Scope("worker", requires=parent)
    parent_activation = parent.activate()
    child_a_activation = child_a.activate()
    child_b.activate()

    # Act
    child_a.deactivate(child_a_activation)

    # Assert
    with pytest.raises(ScopeActivationError, match="a scope requiring it is still active"):
        parent.deactivate(parent_activation)


def test_three_level_chain_activates_and_deactivates_in_order(
    scope_chain: tuple[Scope, Scope, Scope],
):
    """
    A three-level requires chain (application <- session <- request) activates end to end.

    Given: Three scopes chained by requires,
    When: they are activated outer to inner and deactivated inner to outer,
    Then: each activation and deactivation succeeds, and all three end up inactive.
    """
    # Arrange
    application, session, request = scope_chain

    # Act & Assert
    with application.activate():
        with session.activate():
            with request.activate():
                assert application.is_active()
                assert session.is_active()
                assert request.is_active()
    assert not application.is_active()
    assert not session.is_active()
    assert not request.is_active()


def test_three_level_chain_middle_scope_activate_requires_immediate_parent(
    scope_chain: tuple[Scope, Scope, Scope],
):
    """
    The innermost scope in a chain requires its immediate parent, not the whole chain's root.

    Given: Three scopes chained by requires (application <- session <- request),
    When: the outermost (application) and innermost (request) are both never activated,
    Then: activating just the middle scope (session) on its own, without application active,
        still raises - a chain link only checks its own immediate parent, but that parent
        being inactive is enough to block it.
    """
    # Arrange
    _application, session, _request = scope_chain

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="required scope 'application' is not active"):
        session.activate()


def test_three_level_chain_out_of_order_deactivation_blocked(
    scope_chain: tuple[Scope, Scope, Scope],
):
    """
    Deactivating a middle scope while its own dependent is still active raises.

    Given: A three-level requires chain, fully activated,
    When: the middle scope (session) is deactivated while the innermost (request) is still
        active,
    Then: a ScopeActivationError is raised, and session remains active.
    """
    # Arrange
    application, session, request = scope_chain
    application.activate()
    session_activation = session.activate()
    request.activate()

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="a scope requiring it is still active"):
        session.deactivate(session_activation)
    assert session.is_active()


@pytest.mark.parametrize("storage", ["dense", "sparse"])
def test_deactivate_child_with_resource_cleans_up_exit_stack(storage: str):
    """
    Deactivating a scope with a required parent still runs its own exit stack cleanup.

    Given: A child scope requiring a parent, with a cached resource entered during the
        child's activation,
    When: the child is deactivated normally (no exception in flight),
    Then: the resource's cleanup runs - covering the exit-stack-close path a scope takes
        when it has a parent to report its live-dependent count back to, for both dense
        and sparse storage.
    """
    # Arrange
    parent = Scope("application", "shared")
    child = Scope("request", storage=storage, requires=parent)  # pyright: ignore[reportArgumentType]
    mock = Mock()

    @child.cache()
    @resource
    def get_resource():
        try:
            yield object()
        finally:
            mock()

    # Act
    with parent.activate():
        child_activation = child.activate()
        get_resource()
        child.deactivate(child_activation)

    # Assert
    mock.assert_called_once()


def test_deactivate_raises_when_own_sparse_scope_has_active_dependent():
    """
    A sparse-backed scope also blocks its own deactivation while a dependent is active.

    Given: A sparse-backed parent scope with an active child requiring it,
    When: the parent is deactivated,
    Then: A ScopeActivationError is raised - `Activation`'s live-dependent-count guard
        catches this the same way for sparse storage as it does for dense.
    """
    # Arrange
    parent = Scope("application", "shared", "sparse")
    child = Scope("request", requires=parent)
    parent_activation = parent.activate()
    child.activate()

    # Act & Assert
    with pytest.raises(
        ScopeActivationError,
        match="Cannot deactivate 'application': a scope requiring it is still active.",
    ):
        parent.deactivate(parent_activation)
    assert parent.is_active()


@pytest.mark.parametrize("storage", ["dense", "sparse"])
def test_deactivate_child_without_resource_returns_none(storage: str):
    """
    Deactivating a scope with a required parent, but no resource ever entered, is a no-op.

    Given: A child scope requiring a parent, never touching a cached resource,
    When: the child is deactivated normally,
    Then: deactivation succeeds cleanly - covering the exit-stack-absent path a scope
        with a parent takes, for both dense and sparse storage.
    """
    # Arrange
    parent = Scope("application", "shared")
    child = Scope("request", storage=storage, requires=parent)  # pyright: ignore[reportArgumentType]

    # Act
    with parent.activate():
        child_activation = child.activate()
        child.deactivate(child_activation)

    # Assert
    assert not child.is_active()
