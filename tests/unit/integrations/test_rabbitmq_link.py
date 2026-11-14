"""
Tests for the moves between a RabbitMQ link's connection states, with no connection.

This test suite verifies the following behaviors:

Allowed moves:
- Opening a closed link connects it, and reconnecting a reconnecting link connects it.
- A lost channel closes a connected link, or starts it reconnecting.
- A channel lost while closed leaves the link closed, since `close` closed it.
- Giving up reconnecting closes the link, and closing works from every state.

Refused moves:
- Every other move raises, naming the event and the state.

A link:
- A channel closed by `close` doesn't count as lost, even if its close callback runs after.
"""

import asyncio
from itertools import product
from unittest.mock import AsyncMock, Mock

import pytest
from pytest_mock import MockerFixture

from stratae.backoff import Exponential
from stratae.integrations.rabbitmq._link import (
    _MOVES,  # pyright: ignore[reportPrivateUsage]
    Link,
    _Event,  # pyright: ignore[reportPrivateUsage]
    _next,  # pyright: ignore[reportPrivateUsage]
    _RefusedMoveError,  # pyright: ignore[reportPrivateUsage]
)
from stratae.integrations.rabbitmq.connection import ConnectionState

_CLOSED, _CONNECTED, _RECONNECTING = (
    ConnectionState.CLOSED,
    ConnectionState.CONNECTED,
    ConnectionState.RECONNECTING,
)

_ALLOWED = [
    (_CLOSED, _Event.OPENED, _CONNECTED),
    (_RECONNECTING, _Event.RECONNECTED, _CONNECTED),
    (_CONNECTED, _Event.LOST, _CLOSED),
    (_CONNECTED, _Event.LOST_RECONNECTING, _RECONNECTING),
    (_CLOSED, _Event.LOST, _CLOSED),
    (_CLOSED, _Event.LOST_RECONNECTING, _CLOSED),
    (_RECONNECTING, _Event.GAVE_UP, _CLOSED),
    (_CLOSED, _Event.CLOSED, _CLOSED),
    (_CONNECTED, _Event.CLOSED, _CLOSED),
    (_RECONNECTING, _Event.CLOSED, _CLOSED),
]


@pytest.mark.parametrize(
    ("state", "event", "reached"),
    _ALLOWED,
    ids=[f"{state}-{event.name}" for state, event, _ in _ALLOWED],
)
def test_allowed_move(state: ConnectionState, event: _Event, reached: ConnectionState):
    """
    Each allowed move should lead to its state.

    Given: A state, and an event it allows
    When: The event happens
    Then: The link moves to the state the table gives
    """
    assert _next(state, event) is reached


_REFUSED = [
    (state, event)
    for state, event in product(ConnectionState, _Event)
    if (state, event) not in {(s, e) for s, e, _ in _ALLOWED}
]


@pytest.mark.parametrize(
    ("state", "event"), _REFUSED, ids=[f"{state}-{event.name}" for state, event in _REFUSED]
)
def test_refused_move(state: ConnectionState, event: _Event):
    """
    Every move not allowed should be refused, naming the event and the state.

    Given: A state, and an event it doesn't allow
    When: The event happens
    Then: The move raises, naming both
    """
    with pytest.raises(_RefusedMoveError, match=rf"{event.name} while {state}"):
        _next(state, event)


def test_table_lists_exactly_the_allowed_moves():
    """
    The table should hold the allowed moves and nothing else.

    Given: The table of moves
    When: It's compared with the moves this suite allows
    Then: They're the same
    """
    assert dict(_MOVES) == {(state, event): reached for state, event, reached in _ALLOWED}


async def test_channel_closed_by_close_is_not_a_lost_connection(mocker: MockerFixture):
    """
    A channel close that `close` caused should leave the link closed, not lose it.

    Given: An open link with reconnecting turned on, whose channel has just closed, its
        close callback scheduled but not yet run
    When: `close` runs before that callback does
    Then: The link stays closed, and it neither reports a lost connection nor reconnects
    """
    # Arrange
    closing = asyncio.get_running_loop().create_future()
    connection = AsyncMock()
    connection.channel.return_value = Mock(closing=closing)
    mocker.patch("stratae.integrations.rabbitmq._link.connect", AsyncMock(return_value=connection))
    lost = Mock()
    link = Link("test", "amqp://broker", Exponential(0.01), AsyncMock(), lost)
    await link.open()
    closing.set_exception(ConnectionError("gone"))

    # Act
    await link.close()
    await asyncio.sleep(0)

    # Assert
    assert link.state is ConnectionState.CLOSED
    lost.assert_not_called()
    assert isinstance(closing.exception(), ConnectionError)
