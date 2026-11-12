"""
What a consumer does with a message whose handling failed, as a value.

{py:func}`failure_action` is the rule. It takes a {py:class}`FailurePolicy`, or `None` for no
policy, and returns an action: {py:class}`Reject`, {py:class}`Retry`, {py:class}`DeadLetter`
or {py:class}`Drop`. The rule does no I/O. A broker adapter carries the action out in its own
terms. For example, the RabbitMQ consumer nacks a rejected message, and routes a retry through
a queue whose message TTL is the delay.

Without a policy, the message is rejected. With one, it's retried after each delay of the
policy's {py:class}`Backoff <stratae.backoff.Backoff>`. Once the backoff runs out, it's
dead-lettered, or dropped if the policy doesn't dead-letter. A failure that can't succeed on
another attempt, such as a body that fails to decode, skips the retries.

Each action has a logging `level`, and {py:func}`describe` words it for a log line. So every
adapter reports the same outcome the same way.

````{example} Decide a failed message's fate under a policy
```{code-block} python
from dataclasses import dataclass

from stratae.backoff import Backoff, Delays
from stratae.events.failure import describe, failure_action

@dataclass(frozen=True)
class Policy:
    backoff: Backoff
    dead_letter: bool

policy = Policy(Delays(1, 10), dead_letter=True)
for failures in (1, 2, 3):
    print(describe(failure_action(policy, failures, retryable=True), failures))
print(describe(failure_action(policy, 1, retryable=False), 1))
print(describe(failure_action(None, 1, retryable=True), 1))
```
```{output}
retrying in 1 s
retrying in 10 s
message dead-lettered after 3 attempt(s)
message dead-lettered after 1 attempt(s)
message rejected
```
````
"""

import logging
from dataclasses import dataclass
from typing import ClassVar, Protocol

from stratae.backoff import Backoff


class FailurePolicy(Protocol):
    """What a consumer does with a message whose handling keeps failing."""

    @property
    def backoff(self) -> Backoff:
        """The delays before each retry. Once it runs out, the message is given up on."""
        ...

    @property
    def dead_letter(self) -> bool:
        """Whether a message given up on is dead-lettered; `False` drops it."""
        ...


@dataclass(frozen=True, slots=True)
class Reject:
    """Reject the message, without a policy to retry or dead-letter it."""

    level: ClassVar[int] = logging.ERROR


@dataclass(frozen=True, slots=True)
class Retry:
    """Deliver the message again after `delay` seconds."""

    delay: float
    level: ClassVar[int] = logging.WARNING


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """Move the message to the dead letters."""

    level: ClassVar[int] = logging.ERROR


@dataclass(frozen=True, slots=True)
class Drop:
    """Discard the message."""

    level: ClassVar[int] = logging.ERROR


type FailureAction = Reject | Retry | DeadLetter | Drop
"""What a consumer does with a message whose handling failed."""


def failure_action(
    policy: FailurePolicy | None, failures: int, *, retryable: bool
) -> FailureAction:
    """
    Return what to do with a message whose handling failed.

    :param policy: The message's {py:class}`FailurePolicy`, or `None` to reject it.
    :param failures: How many times the message has failed, counting this failure.
    :param retryable: Whether another attempt could succeed. `False` skips the retries.
    :returns: The {py:class}`Reject`, {py:class}`Retry`, {py:class}`DeadLetter` or
        {py:class}`Drop` to carry out.

    ````{example} A failure that can't succeed again skips the retries
    ```{code-block} python
    from dataclasses import dataclass

    from stratae.backoff import Backoff, Delays
    from stratae.events.failure import DeadLetter, Drop, Retry, failure_action

    @dataclass(frozen=True)
    class Policy:
        backoff: Backoff
        dead_letter: bool

    retrying = Policy(Delays(5), dead_letter=True)
    assert failure_action(retrying, 1, retryable=True) == Retry(5)
    assert failure_action(retrying, 1, retryable=False) == DeadLetter()
    assert failure_action(Policy(Delays(), dead_letter=False), 1, retryable=True) == Drop()
    ```
    ````
    """
    if policy is None:
        return Reject()
    if retryable and (delay := policy.backoff.delay(failures)) is not None:
        return Retry(delay)
    return DeadLetter() if policy.dead_letter else Drop()


def describe(action: FailureAction, failures: int) -> str:
    """
    Word an action for a log line about the failure that led to it.

    :param action: The action carried out.
    :param failures: How many times the message has failed, counting this failure.
    :returns: A phrase such as `"retrying in 5 s"` or `"message dropped after 3 attempt(s)"`.

    ````{example} Log a failure at its action's level
    ```{code-block} python
    import logging

    from stratae.events.failure import Drop, describe

    action = Drop()
    assert action.level == logging.ERROR
    assert describe(action, 3) == "message dropped after 3 attempt(s)"
    ```
    ````
    """
    match action:
        case Reject():
            return "message rejected"
        case Retry(delay):
            return f"retrying in {delay:g} s"
        case DeadLetter():
            return f"message dead-lettered after {failures} attempt(s)"
        case _:
            return f"message dropped after {failures} attempt(s)"
