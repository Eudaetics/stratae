"""A connection and its channel, reopened with backoff after an unexpected close."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextlib import suppress
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

    @state.setter
    def state(self, state: ConnectionState) -> None:
        """Move to `state`, waking whoever waits for it."""
        self._state = state
        for reached in self._waiting.pop(state, ()):
            reached.set()

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
        if self.state is not ConnectionState.CLOSED:
            raise AlreadyConnectedError(_ALREADY_CONNECTED.format(self._name))
        await self._open()

    async def _open(self) -> None:
        """Connect, open a channel and run `on_open`, closing the connection if any step fails."""
        connection = await connect(self._url)
        try:
            channel = await connection.channel(**_CHANNEL_OPTIONS)
            await self._on_open(channel)
        except BaseException:
            await connection.close()
            raise
        self._connection, self.channel = connection, channel
        self.state = ConnectionState.CONNECTED
        self._watch = _closing(channel)
        self._watch.add_done_callback(self._on_closed)

    def _on_closed(self, closing: asyncio.Future[Any]) -> None:
        """Start reconnecting when the channel closes, unless it was closed on purpose."""
        if closing.cancelled() or self.state is not ConnectionState.CONNECTED:
            return
        stale, reason = cast("AbstractConnection", self._connection), closing.exception()
        self._connection, self.channel, self._watch = None, None, None
        self._on_lost()
        if self._reconnect is None:
            _log.error("%s lost its broker connection: %s", self._name, reason)
            self.state = ConnectionState.CLOSED
        else:
            _log.warning("%s lost its broker connection (%s); reconnecting", self._name, reason)
            self.state = ConnectionState.RECONNECTING
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
                await self._open()
            except Exception:
                _log.warning("%s failed to reconnect", self._name, exc_info=True)
                failures += 1
                continue
            _log.info("%s reconnected to the broker", self._name)
            return
        _log.error("%s gave up reconnecting after %d attempt(s)", self._name, failures - 1)
        self.state = ConnectionState.CLOSED

    async def close(self) -> None:
        """Stop any reconnecting and close the connection, if open."""
        self.state = ConnectionState.CLOSED
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
