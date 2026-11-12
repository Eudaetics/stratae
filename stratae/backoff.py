"""
How long to wait after each failure, as a value.

A {py:class}`Backoff` is a rule: `delay(n)` is the wait after the nth failure in a row, or
`None` once there are no retries left. The same `n` always gives the same delay, so a backoff
holds no progress of its own. Whatever retries keeps the count of failures, in one place, and
asks for the next delay. What running out means is that caller's decision. The outbox relay
parks the record. A RabbitMQ failure policy dead-letters or drops the message.

{py:class}`Exponential` multiplies the delay by `factor` after each failure, up to `cap`.
{py:class}`Delays` lists each delay explicitly. Both are immutable, and compare equal when
their fields are.

````{example} An exponential backoff, and an explicit one
```{code-block} python
from stratae.backoff import Delays, Exponential

retry = Exponential(30, cap=3600, retries=9)
print([retry.delay(n) for n in range(1, 11)])

tiers = Delays(1, 10, 60)
print([tiers.delay(n) for n in range(1, 5)], tiers.retries)
```
```{output}
[30.0, 60.0, 120.0, 240.0, 480.0, 960.0, 1920.0, 3600.0, 3600.0, None]
[1.0, 10.0, 60.0, None] 3
```
````
"""

from dataclasses import dataclass
from math import inf
from typing import Protocol

_NEGATIVE_DELAY = "a backoff's delays can't be negative, got {!r}"
_SMALL_FACTOR = "an exponential backoff's factor must be at least 1, got {!r}"
_CAP_BELOW_FIRST = "an exponential backoff's cap {!r} is below its first delay {!r}"
_NEGATIVE_RETRIES = "a backoff's retries can't be negative, got {!r}"
_NO_FAILURES = "a delay follows a failure, so failures must be at least 1, got {!r}"


class Backoff(Protocol):
    """How long to wait after each failure in a row, and how many retries there are."""

    @property
    def retries(self) -> int | None:
        """How many failures get a delay; `None` if it never runs out."""
        ...

    def delay(self, failures: int) -> float | None:
        """
        Return the seconds to wait after the `failures`-th failure in a row.

        :param failures: How many failures there have been, counting this one; at least 1.
        :returns: The delay, or `None` once `failures` is past `retries`.
        """
        ...


def _check_failures(failures: int) -> None:
    """Refuse a failure count below one."""
    if failures < 1:
        raise ValueError(_NO_FAILURES.format(failures))


@dataclass(frozen=True, slots=True)
class Exponential:
    """
    A delay of `first` seconds, multiplied by `factor` after each failure, up to `cap`.

    :raises ValueError: If `first` is negative, `factor` is below 1, `cap` is below `first`,
        or `retries` is negative.
    """

    first: float
    """Seconds to wait after the first failure."""
    factor: float = 2.0
    """What each further failure multiplies the delay by."""
    cap: float = inf
    """The longest delay."""
    retries: int | None = None
    """How many failures get a delay; `None` never runs out."""

    def __post_init__(self) -> None:
        """Refuse a backoff that can't produce sensible delays."""
        if self.first < 0:
            raise ValueError(_NEGATIVE_DELAY.format(self.first))
        if self.factor < 1:
            raise ValueError(_SMALL_FACTOR.format(self.factor))
        if self.cap < self.first:
            raise ValueError(_CAP_BELOW_FIRST.format(self.cap, self.first))
        if self.retries is not None and self.retries < 0:
            raise ValueError(_NEGATIVE_RETRIES.format(self.retries))

    def delay(self, failures: int) -> float | None:
        """
        Return `first * factor ** (failures - 1)`, capped, or `None` past `retries`.

        :param failures: How many failures there have been, counting this one; at least 1.
        :raises ValueError: If `failures` is below 1.
        """
        _check_failures(failures)
        if self.retries is not None and failures > self.retries:
            return None
        try:
            return float(min(self.first * self.factor ** (failures - 1), self.cap))
        except OverflowError:
            return float(self.cap)


@dataclass(frozen=True, slots=True, init=False)
class Delays:
    """
    Each delay given explicitly, in order: one retry per delay.

    :raises ValueError: If a delay is negative.
    """

    seconds: tuple[float, ...]
    """Seconds to wait after each failure, in order."""

    def __init__(self, *seconds: float) -> None:
        """
        List the delays.

        :param seconds: Seconds to wait after each failure, in order, e.g. `1, 10, 60`.
        :raises ValueError: If a delay is negative.
        """
        for delay in seconds:
            if delay < 0:
                raise ValueError(_NEGATIVE_DELAY.format(delay))
        object.__setattr__(self, "seconds", tuple(float(delay) for delay in seconds))

    @property
    def retries(self) -> int:
        """How many delays there are."""
        return len(self.seconds)

    def delay(self, failures: int) -> float | None:
        """
        Return the `failures`-th delay, or `None` past the last.

        :param failures: How many failures there have been, counting this one; at least 1.
        :raises ValueError: If `failures` is below 1.
        """
        _check_failures(failures)
        if failures > len(self.seconds):
            return None
        return self.seconds[failures - 1]
