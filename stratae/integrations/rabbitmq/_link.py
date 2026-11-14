"""A connection and its channel, reopened with backoff after an unexpected close."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from enum import Enum, auto
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

from aiormq import connect

from stratae.backoff import Backoff, Exponential
from stratae.events.exceptions import AlreadyConnectedError
from stratae.integrations.rabbitmq.connection import ConnectionState

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection


_log = logging.getLogger(__name__)
_ALREADY_CONNECTED = "{} is already connected; exit it before entering it again"

RECONNECT = Exponential(0.5, cap=30.0)
"""How long publishers and consumers wait before each attempt to reconnect, by default."""

_CHANNEL_OPTIONS: dict[str, Any] = {"publisher_confirms": True, "on_return_raises": True}
"""Every channel waits for the broker's confirm on publish, and raises for a returned message."""


class _Event(Enum):
    """Something that happens to a link's connection."""

    OPENED = auto()
    """`open` connected."""
    RECONNECTED = auto()
    """An attempt to reconnect connected."""
    LOST = auto()
    """The channel closed unexpectedly, with reconnecting turned off."""
    LOST_RECONNECTING = auto()
    """The channel closed unexpectedly, with reconnecting turned on."""
    GAVE_UP = auto()
    """Reconnecting ran out of attempts."""
    CLOSED = auto()
    """`close` was called."""


_CLOSED, _CONNECTED, _RECONNECTING = (
    ConnectionState.CLOSED,
    ConnectionState.CONNECTED,
    ConnectionState.RECONNECTING,
)

_MOVES: Mapping[tuple[ConnectionState, _Event], ConnectionState] = MappingProxyType(
    {
        (_CLOSED, _Event.OPENED): _CONNECTED,
        (_RECONNECTING, _Event.RECONNECTED): _CONNECTED,
        (_CONNECTED, _Event.LOST): _CLOSED,
        (_CONNECTED, _Event.LOST_RECONNECTING): _RECONNECTING,
        (_CLOSED, _Event.LOST): _CLOSED,
        (_CLOSED, _Event.LOST_RECONNECTING): _CLOSED,
        (_RECONNECTING, _Event.GAVE_UP): _CLOSED,
        (_CLOSED, _Event.CLOSED): _CLOSED,
        (_CONNECTED, _Event.CLOSED): _CLOSED,
        (_RECONNECTING, _Event.CLOSED): _CLOSED,
    }
)
"""
Each state a link can move to, from a state and what happened.

A channel lost while closed stays closed. The channel closed because `close` closed it,
after its close callback was already scheduled. Any move not listed is refused.
"""


class _RefusedMoveError(RuntimeError):
    """Raised when something happens to a link that its state doesn't allow."""

    def __init__(self, state: ConnectionState, event: _Event) -> None:
        super().__init__(f"a link can't move on {event.name} while {state}")


def _next(state: ConnectionState, event: _Event) -> ConnectionState:
    """
    Return the state a link moves to when `event` happens in `state`.

    :raises _RefusedMoveError: If the move isn't allowed.
    """
    try:
        return _MOVES[state, event]
    except KeyError:
        raise _RefusedMoveError(state, event) from None


def _closing(channel: Any) -> asyncio.Future[Any]:
    """Return a channel's closing future, typed: aiormq leaves its result unknown."""
    return channel.closing


class Link:
    """A connection and its channel, reopened with backoff after an unexpected close."""

    def __init__(
        self,
        name: str,
        url: str,
        reconnect: Backoff | None,
        on_open: Callable[[AbstractChannel], Awaitable[None]],
        on_lost: Callable[[], None],
    ) -> None:
        self._name = name
        self._url = url
        self._reconnect = reconnect
        self._on_open = on_open
        self._on_lost = on_lost
        self._connection: AbstractConnection | None = None
        self._watch: asyncio.Future[Any] | None = None
        self._reconnecting: asyncio.Task[None] | None = None
        self.channel: AbstractChannel | None = None
        self._state = ConnectionState.CLOSED
        self._waiting: dict[ConnectionState, set[asyncio.Event]] = defaultdict(set)

    @property
    def state(self) -> ConnectionState:
        """Where the connection is."""
        return self._state

    def _move(self, event: _Event) -> ConnectionState:
        """Move to the state `event` leads to, waking whoever waits for it, and return it."""
        state = self._state = _next(self._state, event)
        for reached in self._waiting.pop(state, ()):
            reached.set()
        return state

    async def wait_for(self, state: ConnectionState) -> None:
        """Return once the connection is in `state`, now or next."""
        if self._state is state:
            return
        reached = asyncio.Event()
        waiting = self._waiting[state]
        waiting.add(reached)
        try:
            await reached.wait()
        finally:
            waiting.discard(reached)

    async def open(self) -> None:
        """Connect and run `on_open`, raising if already open or if either step fails."""
        if (self._state, _Event.OPENED) not in _MOVES:
            raise AlreadyConnectedError(_ALREADY_CONNECTED.format(self._name))
        await self._open(_Event.OPENED)

    async def _open(self, event: _Event) -> None:
        """Connect, open a channel and run `on_open`, closing the connection if any step fails."""
        connection = await connect(self._url)
        try:
            channel = await connection.channel(**_CHANNEL_OPTIONS)
            await self._on_open(channel)
        except BaseException:
            await connection.close()
            raise
        self._connection, self.channel = connection, channel
        self._move(event)
        self._watch = _closing(channel)
        self._watch.add_done_callback(self._on_closed)

    def _on_closed(self, closing: asyncio.Future[Any]) -> None:
        """Start reconnecting when the channel closes, unless `close` closed it."""
        if closing.cancelled():
            return
        was = self._state
        event = _Event.LOST if self._reconnect is None else _Event.LOST_RECONNECTING
        if self._move(event) is was:
            return
        stale, reason = cast("AbstractConnection", self._connection), closing.exception()
        self._connection, self.channel, self._watch = None, None, None
        self._on_lost()
        if event is _Event.LOST:
            _log.error("%s lost its broker connection: %s", self._name, reason)
        else:
            _log.warning("%s lost its broker connection (%s); reconnecting", self._name, reason)
        self._reconnecting = asyncio.get_running_loop().create_task(self._recover(stale))

    async def _recover(self, stale: AbstractConnection) -> None:
        """Close what's left of the old connection, then reconnect with backoff if enabled."""
        with suppress(Exception):
            await stale.close()
        reconnect = self._reconnect
        if reconnect is None:
            return
        failures = 1
        while (delay := reconnect.delay(failures)) is not None:
            await asyncio.sleep(delay)
            try:
                await self._open(_Event.RECONNECTED)
            except Exception:
                _log.warning("%s failed to reconnect", self._name, exc_info=True)
                failures += 1
                continue
            _log.info("%s reconnected to the broker", self._name)
            return
        _log.error("%s gave up reconnecting after %d attempt(s)", self._name, failures - 1)
        self._move(_Event.GAVE_UP)

    async def close(self) -> None:
        """Stop any reconnecting and close the connection, if open."""
        self._move(_Event.CLOSED)
        recovering, self._reconnecting = self._reconnecting, None
        if recovering is not None:
            recovering.cancel()
            with suppress(asyncio.CancelledError):
                await recovering
        if self._watch is not None:
            self._watch.cancel()
        connection = self._connection
        # Cleared before closing, so a failed close leaves no stale state.
        self._connection, self.channel, self._watch = None, None, None
        if connection is not None:
            await connection.close()
