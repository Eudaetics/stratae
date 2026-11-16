"""
How long to wait after each failure, as a value.

A {py:class}`Backoff` is a rule: `delay(n)` is the wait after the nth failure in a row, or
`None` once there are no retries left. The same `n` always gives the same delay, so a backoff
holds no progress of its own. Whatever retries keeps the count of failures, in one place, and
asks for the next delay. What running out means is that caller's decision. The outbox relay
parks the record. A RabbitMQ failure policy dead-letters or drops the message.

Some callers need to know more. An {py:class}`Endless` backoff never runs out, as the outbox
relay's polling needs. A {py:class}`Finite` one lists every delay it gives, as a RabbitMQ
failure policy needs, since each delay names a retry queue.

{py:class}`Exponential` multiplies the delay by `factor` after each failure, up to `cap`, and
never runs out. {py:class}`Limited` gives an endless backoff a number of retries.
{py:class}`Delays` lists each delay explicitly. All are immutable, and compare equal when their
fields are.

````{example} An exponential backoff, limited, and an explicit one
```{code-block} python
from stratae.backoff import Delays, Exponential, Limited

retry = Limited(Exponential(30, cap=3600), retries=9)
print([retry.delay(n) for n in range(1, 11)])

tiers = Delays(1, 10, 60)
print([tiers.delay(n) for n in range(1, 5)], tiers.delays)
```
```{output}
[30.0, 60.0, 120.0, 240.0, 480.0, 960.0, 1920.0, 3600.0, 3600.0, None]
[1.0, 10.0, 60.0, None] (1.0, 10.0, 60.0)
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
    """How long to wait after each failure in a row, until there are no retries left."""

    def delay(self, failures: int) -> float | None:
        """
        Return the seconds to wait after the `failures`-th failure in a row.

        :param failures: How many failures there have been, counting this one; at least 1.
        :returns: The delay, or `None` once there are no retries left.
        """
        ...


class Endless(Protocol):
    """A {py:class}`Backoff` that never runs out: every failure gets a delay."""

    def delay(self, failures: int) -> float:
        """
        Return the seconds to wait after the `failures`-th failure in a row.

        :param failures: How many failures there have been, counting this one; at least 1.
        """
        ...


class Finite(Protocol):
    """A {py:class}`Backoff` that runs out, and lists every delay it gives."""

    @property
    def delays(self) -> tuple[float, ...]:
        """Seconds to wait after each failure, in order; one retry per delay."""
        ...

    def delay(self, failures: int) -> float | None:
        """
        Return the seconds to wait after the `failures`-th failure in a row.

        :param failures: How many failures there have been, counting this one; at least 1.
        :returns: The `failures`-th of `delays`, or `None` past the last.
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

    It never runs out. {py:class}`Limited` gives it a number of retries.

    :raises ValueError: If `first` is negative, `factor` is below 1, or `cap` is below `first`.
    """

    first: float
    """Seconds to wait after the first failure."""
    factor: float = 2.0
    """What each further failure multiplies the delay by."""
    cap: float = inf
    """The longest delay."""

    def __post_init__(self) -> None:
        """Refuse a backoff that can't produce sensible delays."""
        if self.first < 0:
            raise ValueError(_NEGATIVE_DELAY.format(self.first))
        if self.factor < 1:
            raise ValueError(_SMALL_FACTOR.format(self.factor))
        if self.cap < self.first:
            raise ValueError(_CAP_BELOW_FIRST.format(self.cap, self.first))

    def delay(self, failures: int) -> float:
        """
        Return `first * factor ** (failures - 1)`, capped.

        :param failures: How many failures there have been, counting this one; at least 1.
        :raises ValueError: If `failures` is below 1.
        """
        _check_failures(failures)
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

    delays: tuple[float, ...]
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
        object.__setattr__(self, "delays", tuple(float(delay) for delay in seconds))

    def delay(self, failures: int) -> float | None:
        """
        Return the `failures`-th delay, or `None` past the last.

        :param failures: How many failures there have been, counting this one; at least 1.
        :raises ValueError: If `failures` is below 1.
        """
        _check_failures(failures)
        if failures > len(self.delays):
            return None
        return self.delays[failures - 1]


@dataclass(frozen=True, slots=True)
class Limited:
    """
    An endless backoff's delays, for the first `retries` failures.

    :raises ValueError: If `retries` is negative.
    """

    backoff: Endless
    """The backoff giving each delay."""
    retries: int
    """How many failures get a delay."""

    def __post_init__(self) -> None:
        """Refuse a negative number of retries."""
        if self.retries < 0:
            raise ValueError(_NEGATIVE_RETRIES.format(self.retries))

    @property
    def delays(self) -> tuple[float, ...]:
        """Seconds to wait after each failure, in order."""
        return tuple(self.backoff.delay(failures) for failures in range(1, self.retries + 1))

    def delay(self, failures: int) -> float | None:
        """
        Return the backoff's delay, or `None` past `retries`.

        :param failures: How many failures there have been, counting this one; at least 1.
        :raises ValueError: If `failures` is below 1.
        """
        _check_failures(failures)
        if failures > self.retries:
            return None
        return self.backoff.delay(failures)
