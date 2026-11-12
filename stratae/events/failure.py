"""
What to do with a message whose handling failed, as a value.

{py:func}`failure_action` is the rule. A failed message is retried after each delay of a
{py:class}`Backoff <stratae.backoff.Backoff>`. Once the backoff runs out, it's given up on, by
whichever action the caller gives. A failure that can't succeed on another attempt, such as
a body that fails to decode, is given up on straight away. The rule does no I/O. An adapter
carries the action out in its own terms. For example, the RabbitMQ consumer routes a retry
through a queue whose message TTL is the delay.

An action is any {py:class}`FailureAction`: a value with a logging `level` and a `describe`
method that words it for a log line. An adapter logs the failure at that level, with the
attempt count and the action's wording. So each adapter reports its outcomes the same way,
and can add actions of its own. This module has {py:class}`Retry`, and three ways to give up
that brokers commonly offer: {py:class}`Reject`, {py:class}`DeadLetter` and
{py:class}`Drop`. The outbox relay gives up by parking a record, with an action of its own.

{py:func}`failure_reason` words the error itself, for an adapter to store with the message.
So a failure reads the same wherever it's kept.

````{example} Decide a failed message's fate
```{code-block} python
from stratae.backoff import Delays
from stratae.events.failure import DeadLetter, failure_action

for failures in (1, 2, 3):
    action = failure_action(Delays(1, 10), DeadLetter(), failures, retryable=True)
    print(f"failed after {failures} attempt(s); {action.describe()}")
print(failure_action(Delays(1, 10), DeadLetter(), 1, retryable=False).describe())
```
```{output}
failed after 1 attempt(s); retrying in 1 s
failed after 2 attempt(s); retrying in 10 s
failed after 3 attempt(s); message dead-lettered
message dead-lettered
```
````
"""

import logging
from dataclasses import dataclass
from typing import ClassVar, Protocol

from stratae.backoff import Backoff


class FailureAction(Protocol):
    """What to do with a message whose handling failed, and how to log it."""

    level: ClassVar[int]
    """The logging level to report the failure at."""

    def describe(self) -> str:
        """Word the action for a log line about the failure that led to it."""
        ...


@dataclass(frozen=True, slots=True)
class Retry:
    """Deliver the message again after `delay` seconds."""

    delay: float
    level: ClassVar[int] = logging.WARNING

    def describe(self) -> str:
        """Word the retry for a log line."""
        return f"retrying in {self.delay:g} s"


@dataclass(frozen=True, slots=True)
class Reject:
    """Reject the message, leaving the broker to dead-letter or discard it."""

    level: ClassVar[int] = logging.ERROR

    def describe(self) -> str:
        """Word the rejection for a log line."""
        return "message rejected"


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """Move the message to the dead letters."""

    level: ClassVar[int] = logging.ERROR

    def describe(self) -> str:
        """Word the dead letter for a log line."""
        return "message dead-lettered"


@dataclass(frozen=True, slots=True)
class Drop:
    """Discard the message."""

    level: ClassVar[int] = logging.ERROR

    def describe(self) -> str:
        """Word the drop for a log line."""
        return "message dropped"


def failure_action[G: FailureAction](
    backoff: Backoff, give_up: G, failures: int, *, retryable: bool
) -> Retry | G:
    """
    Return what to do with a message whose handling failed.

    :param backoff: The delays before each retry.
    :param give_up: The action once the backoff runs out, or for a failure that can't succeed
        on another attempt.
    :param failures: How many times the message has failed, counting this failure.
    :param retryable: Whether another attempt could succeed. `False` skips the retries.
    :returns: A {py:class}`Retry` after the backoff's next delay, or `give_up`.

    ````{example} A failure that can't succeed again is given up on at once
    ```{code-block} python
    from stratae.backoff import Delays
    from stratae.events.failure import Drop, Retry, failure_action

    assert failure_action(Delays(5), Drop(), 1, retryable=True) == Retry(5)
    assert failure_action(Delays(5), Drop(), 2, retryable=True) == Drop()
    assert failure_action(Delays(5), Drop(), 1, retryable=False) == Drop()
    ```
    ````
    """
    if retryable and (delay := backoff.delay(failures)) is not None:
        return Retry(delay)
    return give_up


def failure_reason(exc: Exception) -> str:
    """
    Word an error for an adapter to store as the reason a message failed.

    :param exc: The error the message's handling raised.
    :returns: The error's type name and message.

    ````{example} Word an error as a failure's reason
    ```{code-block} python
    from stratae.events.failure import failure_reason

    assert failure_reason(ValueError("bad order")) == "ValueError: bad order"
    ```
    ````
    """
    return f"{type(exc).__name__}: {exc}"
