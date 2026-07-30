"""Tests for ExitStack and the cleanup behavior of Lifecycle.pop."""

from contextlib import contextmanager
from typing import Generator
from unittest.mock import Mock

import pytest

from stratae.lifecycle import Lifecycle, Scope
from stratae.lifecycle._scope import UNSET
from stratae.lifecycle._stack import ExitStack


def test_context_behavior():
    """
    Test that the exit stack properly manages context managers.

    Given: an ExitStack containing an entered context manager
    When: the stack is closed
    Then: the context manager's cleanup function should be called
    """
    # Arrange
    stack = ExitStack()

    spy_mock = Mock()
    spy_success = Mock()

    @contextmanager
    def generator() -> Generator[None, None, None]:
        try:
            yield
            spy_success()
        finally:
            spy_mock()

    # Act
    stack.enter_context(generator())
    spy_mock.assert_not_called()
    spy_success.assert_not_called()

    # Assert
    stack.close()
    spy_mock.assert_called_once()
    spy_success.assert_called_once()


def test_context_with_failure():
    """
    Test that the exit stack properly handles exceptions within a context.

    Given: an ExitStack containing an entered context manager that raises an exception
    When: the stack is closed
    Then: the exception should be propagated and the cleanup function should be called
    """
    # Arrange
    stack = ExitStack()

    spy_mock = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    spy_except = Mock()

    @contextmanager
    def generator() -> Generator[None, None, None]:
        try:
            yield
            mock_failure()
        except ValueError:
            spy_except()
            raise
        finally:
            spy_mock()

    # Act
    stack.enter_context(generator())
    spy_mock.assert_not_called()
    mock_failure.assert_not_called()
    spy_except.assert_not_called()

    # Assert
    with pytest.raises(ValueError, match="Test Failure"):
        stack.close()
    spy_except.assert_called_once()
    spy_mock.assert_called_once()


def test_pop_closes_stack_and_next_push_starts_fresh():
    """
    Test that popping a scope closes its exit stack and the next activation starts empty.

    Given: an active Lifecycle scope with a populated slot and an entered context manager,
    When: the scope is popped and pushed again,
    Then: cleanup should have run and the new activation's slots, including the reserved
        exit-stack slot 0, should all be UNSET.
    """
    # Arrange
    cleanup = Mock()
    lifecycle = Lifecycle([Scope("application", "shared")])
    index = lifecycle.allocate_slot("application")

    @contextmanager
    def generator():
        try:
            yield
        finally:
            cleanup()

    token = lifecycle.push("application")
    slots = lifecycle.get_slots("application")
    slots[index] = "value"
    lifecycle.get_exit_stack("application").enter_context(generator())

    # Act
    lifecycle.pop(token)

    # Assert
    cleanup.assert_called_once()
    lifecycle.push("application")
    assert lifecycle.get_slots("application") == [UNSET, UNSET]
