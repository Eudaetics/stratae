"""
Test suite for backoff values.

Exponential:
- Its delays start at first, multiply by factor, and stop at cap; None past retries.
- Without retries, it never runs out, and a huge failure count gives the cap.
- It refuses a negative first delay, a factor below 1, a cap below first, and negative
  retries.

Delays:
- Its delays are the ones given, in order, then None; retries is how many were given.
- It refuses a negative delay.

Both:
- The same failure count always gives the same delay.
- Equal fields compare equal, and the delays are floats whatever they were given as.
- A failure count below one is refused.
"""

from typing import Any

import pytest

from stratae.backoff import Backoff, Delays, Exponential


def test_exponential_delays_grow_to_cap_then_run_out():
    """
    An exponential backoff should multiply its delay up to the cap, then run out.

    Given: A backoff starting at 1 second, doubling, capped at 5, with four retries
    When: Its delays after one to five failures are read
    Then: They're 1, 2, 4 and 5 seconds, then None
    """
    backoff = Exponential(1, cap=5, retries=4)

    assert [backoff.delay(n) for n in range(1, 6)] == [1.0, 2.0, 4.0, 5.0, None]


def test_exponential_without_retries_never_runs_out():
    """
    An exponential backoff without retries should never run out.

    Given: A backoff starting at half a second, capped at 30, with no retry limit
    When: Its delay after a huge number of failures is read
    Then: It's the cap, not None, and not an overflow
    """
    backoff = Exponential(0.5, cap=30)

    assert backoff.retries is None
    assert backoff.delay(10_000) == 30.0


@pytest.mark.parametrize(
    ("fields", "match"),
    [
        ({"first": -1}, "negative"),
        ({"first": 1, "factor": 0.5}, "factor"),
        ({"first": 2, "cap": 1}, "cap"),
        ({"first": 1, "retries": -1}, "retries"),
    ],
)
def test_exponential_refuses_bad_fields(fields: dict[str, Any], match: str):
    """
    An exponential backoff should refuse fields that can't make sensible delays.

    Given: A negative first delay, a shrinking factor, a cap below the first delay, or
           negative retries
    When: The backoff is built
    Then: ValueError names the problem
    """
    with pytest.raises(ValueError, match=match):
        Exponential(**fields)


def test_delays_are_given_ones_then_run_out():
    """
    An explicit backoff should give its delays in order, then run out.

    Given: Delays of 1, 10 and 60 seconds
    When: Its delays after one to four failures are read
    Then: They're 1, 10 and 60 seconds, then None, and it has three retries
    """
    backoff = Delays(1, 10, 60)

    assert [backoff.delay(n) for n in range(1, 5)] == [1.0, 10.0, 60.0, None]
    assert backoff.retries == 3


def test_delays_refuse_negative_delay():
    """
    An explicit backoff should refuse a negative delay.

    Given: A negative delay among positive ones
    When: The backoff is built
    Then: ValueError says delays can't be negative
    """
    with pytest.raises(ValueError, match="negative"):
        Delays(1, -1)


@pytest.mark.parametrize("backoff", [Exponential(1, cap=8), Delays(1, 2, 3)])
def test_same_failure_count_gives_same_delay(backoff: Backoff):
    """
    A backoff should hold no progress: the same failure count always gives the same delay.

    Given: An exponential and an explicit backoff
    When: Their delays are read in order twice, and the second one read again first
    Then: Every read of the same count gives the same delay
    """
    first = [backoff.delay(n) for n in (1, 2, 3)]

    assert backoff.delay(2) == first[1]
    assert [backoff.delay(n) for n in (1, 2, 3)] == first


def test_equal_fields_compare_equal():
    """
    Backoffs with equal fields should be equal, whether delays are given as ints or floats.

    Given: Pairs of backoffs built from the same numbers
    When: They're compared
    Then: Each pair is equal, and different fields aren't
    """
    assert Exponential(30, cap=3600, retries=9) == Exponential(30.0, cap=3600.0, retries=9)
    assert Delays(1, 10) == Delays(1.0, 10.0)
    assert Exponential(1) != Exponential(2)


@pytest.mark.parametrize("backoff", [Exponential(1), Delays(1)])
def test_failure_count_below_one_refused(backoff: Backoff):
    """
    A backoff should refuse a failure count below one, since a delay follows a failure.

    Given: An exponential and an explicit backoff
    When: Their delay after zero failures is asked for
    Then: ValueError says failures must be at least 1
    """
    with pytest.raises(ValueError, match="at least 1"):
        backoff.delay(0)
