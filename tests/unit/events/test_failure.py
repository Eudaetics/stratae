"""
Unit tests for deciding a failed message's fate.

This test suite verifies the following behaviors:

failure_action:
- Without a policy, a failed message is rejected, retryable or not.
- With retries left and a retryable failure, it's retried after the backoff's next delay.
- Once the backoff runs out, or the failure isn't retryable, it's dead-lettered, or dropped
  when the policy doesn't dead-letter.

Actions:
- A retry logs at warning level; a reject, dead letter or drop logs at error level.
- describe words each action, with the attempt count where the message is given up on.
"""

import logging
from dataclasses import dataclass

import pytest

from stratae.backoff import Backoff, Delays
from stratae.events.failure import (
    DeadLetter,
    Drop,
    FailureAction,
    Reject,
    Retry,
    describe,
    failure_action,
)


@dataclass(frozen=True)
class _Policy:
    backoff: Backoff
    dead_letter: bool = True


_RETRYING = _Policy(Delays(0.5, 2))
_DROPPING = _Policy(Delays(1), dead_letter=False)


@pytest.mark.parametrize(
    ("policy", "failures", "retryable", "expected"),
    [
        (None, 1, True, Reject()),
        (None, 1, False, Reject()),
        (_RETRYING, 1, True, Retry(0.5)),
        (_RETRYING, 2, True, Retry(2)),
        (_RETRYING, 3, True, DeadLetter()),
        (_RETRYING, 1, False, DeadLetter()),
        (_Policy(Delays()), 1, True, DeadLetter()),
        (_DROPPING, 2, True, Drop()),
        (_DROPPING, 1, False, Drop()),
    ],
    ids=[
        "no policy",
        "no policy, not retryable",
        "first retry",
        "last retry",
        "out of retries",
        "not retryable",
        "no retries",
        "dropped",
        "dropped, not retryable",
    ],
)
def test_failure_action_decides_fate(
    policy: _Policy | None, failures: int, retryable: bool, expected: FailureAction
):
    """
    failure_action should decide a failed message's fate from its policy alone.

    Given: The case's failure policy, or none
    When: A message fails for the given time, retryable or not
    Then: The action is to reject, retry, dead-letter or drop it, as the policy says
    """
    assert failure_action(policy, failures, retryable=retryable) == expected


@pytest.mark.parametrize(
    ("action", "level", "text"),
    [
        (Reject(), logging.ERROR, "message rejected"),
        (Retry(0.5), logging.WARNING, "retrying in 0.5 s"),
        (DeadLetter(), logging.ERROR, "message dead-lettered after 3 attempt(s)"),
        (Drop(), logging.ERROR, "message dropped after 3 attempt(s)"),
    ],
    ids=["reject", "retry", "dead letter", "drop"],
)
def test_actions_carry_level_and_wording(action: FailureAction, level: int, text: str):
    """
    Each action should carry its log level, and describe should word it.

    Given: An action
    When: Its level is read, and it's described after a third failure
    Then: A retry is a warning and the rest are errors, each worded for its outcome
    """
    assert action.level == level
    assert describe(action, 3) == text
