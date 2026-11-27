"""
Unit tests for ByPattern, a value for each event pattern found by an event's class.

This test suite verifies the following behaviors:

- An event gets the value of its pattern.
- A subclass of a pattern gets that pattern's value, unless given a value of its own.
- A later pair for the same pattern replaces an earlier one.
- An event of any other pattern raises TypeError naming the owner and the pattern.
- A reason, when given, is added to the refusal.
- A None value is refused with ValueError when the table is built.
"""

from typing import Literal

import pytest

from stratae.events import NoPayload, PubSub, Request
from stratae.events.by_pattern import ByPattern


class _Broadcast(PubSub[NoPayload, Literal[True]]):
    """A PubSub subclass, a pattern of its own."""


_tick = PubSub(name="tick")
_ping = Request(name="ping")
_broadcast = _Broadcast(name="broadcast")


def test_event_gets_its_patterns_value() -> None:
    """
    Calling the table with an event should return its pattern's value.

    Given: A table with a value for PubSub and for Request
    When: It's called with an event of each
    Then: Each gets its own pattern's value
    """
    # Arrange
    table = ByPattern("Owner", [(PubSub, "publish"), (Request, "ask")])

    # Act / Assert
    assert table(_tick) == "publish"
    assert table(_ping) == "ask"


def test_subclass_gets_its_base_patterns_value() -> None:
    """
    A subclass of a pattern should be that pattern.

    Given: A table with a value for PubSub only
    When: It's called with an event of a PubSub subclass
    Then: It gets PubSub's value
    """
    # Arrange
    table = ByPattern("Owner", [(PubSub, "publish")])

    # Act / Assert
    assert table(_broadcast) == "publish"


def test_subclass_given_its_own_value_gets_it() -> None:
    """
    A subclass given a value of its own should get that value, not its base's.

    Given: A table with a value for PubSub and another for its subclass
    When: It's called with an event of each
    Then: Each gets its own value
    """
    # Arrange
    table = ByPattern("Owner", [(PubSub, "publish"), (_Broadcast, "broadcast")])

    # Act / Assert
    assert table(_tick) == "publish"
    assert table(_broadcast) == "broadcast"


def test_later_pair_wins() -> None:
    """
    A later pair for a pattern should replace an earlier one.

    Given: A table with two values for PubSub
    When: It's called with a PubSub event
    Then: It gets the later value
    """
    # Arrange
    table = ByPattern("Owner", [(PubSub, "first"), (PubSub, "second")])

    # Act / Assert
    assert table(_tick) == "second"


def test_unsupported_pattern_refused() -> None:
    """
    An event of a pattern with no value should be refused, naming the owner and pattern.

    Given: A table with a value for PubSub only
    When: It's called with a Request event
    Then: TypeError names the owner and the pattern, with nothing after it
    """
    # Arrange
    table = ByPattern("Owner", [(PubSub, "publish")])

    # Act / Assert
    with pytest.raises(TypeError, match=r"^Owner doesn't support Request events$"):
        table(_ping)


def test_refusal_carries_reason() -> None:
    """
    A table given a reason should add it to its refusal.

    Given: A table with a value for PubSub only, and a reason
    When: It's called with a Request event
    Then: TypeError names the owner and the pattern, followed by the reason
    """
    # Arrange
    table = ByPattern("Owner", [(PubSub, "publish")], reason="a request needs its reply now")

    # Act / Assert
    with pytest.raises(
        TypeError,
        match=r"^Owner doesn't support Request events: a request needs its reply now$",
    ):
        table(_ping)


def test_none_value_refused_when_built() -> None:
    """
    A None value should be refused when the table is built.

    Given: A pair whose value is None
    When: A table is built from it
    Then: ValueError is raised
    """
    # Act / Assert
    with pytest.raises(ValueError, match="can't be None"):
        ByPattern("Owner", [(PubSub, None)])
