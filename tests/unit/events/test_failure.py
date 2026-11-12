"""
Unit tests for deciding a failed message's fate.

This test suite verifies the following behaviors:

failure_action:
- With retries left and a retryable failure, a message is retried after the backoff's next
  delay.
- Once the backoff runs out, or the failure isn't retryable, the give-up action is returned.
- Any FailureAction can be the give-up action, including one the caller defines.

Actions:
- A retry logs at warning level; a reject, dead letter or drop logs at error level.
- Each action words itself.

failure_reason:
- An error is worded as its type name and message.
"""

import logging
from dataclasses import dataclass
from typing import ClassVar, assert_type

import pytest

from stratae.backoff import Backoff, Delays
from stratae.events.failure import (
    DeadLetter,
    Drop,
    FailureAction,
    Reject,
    Retry,
    failure_action,
    failure_reason,
)


@dataclass(frozen=True)
class _Shelve:
    """An action of the caller's own."""

    level: ClassVar[int] = logging.INFO

    def describe(self) -> str:
        return "shelved"


@pytest.mark.parametrize(
    ("backoff", "give_up", "failures", "retryable", "expected"),
    [
        (Delays(0.5, 2), DeadLetter(), 1, True, Retry(0.5)),
        (Delays(0.5, 2), DeadLetter(), 2, True, Retry(2)),
        (Delays(0.5, 2), DeadLetter(), 3, True, DeadLetter()),
        (Delays(0.5, 2), DeadLetter(), 1, False, DeadLetter()),
        (Delays(), Reject(), 1, True, Reject()),
        (Delays(1), Drop(), 2, True, Drop()),
        (Delays(1), _Shelve(), 2, True, _Shelve()),
    ],
    ids=[
        "first retry",
        "last retry",
        "out of retries",
        "not retryable",
        "no retries",
        "dropped",
        "own action",
    ],
)
def test_failure_action_decides_fate(
    backoff: Backoff,
    give_up: FailureAction,
    failures: int,
    retryable: bool,
    expected: FailureAction,
):
    """
    failure_action should retry while the backoff lasts, then give up.

    Given: The case's backoff and give-up action
    When: A message fails for the given time, retryable or not
    Then: It's retried after the backoff's delay, or given up on by the action given
    """
    assert failure_action(backoff, give_up, failures, retryable=retryable) == expected


def test_failure_action_typed_by_give_up():
    """
    failure_action should return a retry or the give-up action's own type.

    Given: A give-up action of the caller's own
    When: failure_action decides a failure
    Then: The result is typed as a retry or that action, and nothing else
    """
    action = failure_action(Delays(1), _Shelve(), 1, retryable=True)

    assert_type(action, Retry | _Shelve)
    assert action == Retry(1)


@pytest.mark.parametrize(
    ("action", "level", "text"),
    [
        (Retry(0.5), logging.WARNING, "retrying in 0.5 s"),
        (Reject(), logging.ERROR, "message rejected"),
        (DeadLetter(), logging.ERROR, "message dead-lettered"),
        (Drop(), logging.ERROR, "message dropped"),
    ],
    ids=["retry", "reject", "dead letter", "drop"],
)
def test_actions_carry_level_and_wording(action: FailureAction, level: int, text: str):
    """
    Each action should carry its log level, and word itself.

    Given: An action
    When: Its level is read, and it's described
    Then: A retry is a warning and the rest are errors, each worded for its outcome
    """
    assert action.level == level
    assert action.describe() == text


def test_failure_reason_names_error_and_message():
    """
    failure_reason should word an error as its type name and message.

    Given: An error with a message
    When: It's worded as a failure's reason
    Then: The reason is its type name, then its message
    """
    assert failure_reason(KeyError("order")) == "KeyError: 'order'"
