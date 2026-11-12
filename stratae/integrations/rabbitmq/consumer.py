"""The RabbitMQ consumer."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, overload

from stratae.backoff import Backoff
from stratae.events import (
    AnyEvent,
    Deserializer,
    Envelope,
    EnvelopeConfig,
    Handler,
    NoPayload,
    PubSub,
)
from stratae.events.exceptions import AlreadyConnectedError
from stratae.events.handler import invoke
from stratae.events.hooks import Hooks
from stratae.events.middleware import Middleware
from stratae.events.pipeline import receiving, wrap_handler
from stratae.integrations.rabbitmq._link import RECONNECT, Link
from stratae.integrations.rabbitmq._topology import declare
from stratae.integrations.rabbitmq._wire import (
    envelope_from,
    failed_attempts,
    failed_properties,
    headers_from,
)
from stratae.integrations.rabbitmq.config import RabbitMQConsumeConfig
from stratae.integrations.rabbitmq.connection import ConnectionState
from stratae.lifecycle import AsyncScope

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, DeliveredMessage


_log = logging.getLogger(__name__)
_PUBSUB_ONLY = "RabbitMQConsumer handles PubSub events only, got {!r}"


class _Registration:
    """Pairs a registered handler with its deserializer and live consumer tag."""

    __slots__ = ("consumer_tag", "deserializer", "event", "handler")

    def __init__(
        self,
        event: PubSub[Any, Any],
        handler: Handler[[Any], RabbitMQConsumeConfig, Any],
        deserializer: Deserializer | None,
    ) -> None:
        self.event = event
        self.handler = handler
        self.deserializer = deserializer
        self.consumer_tag: str | None = None


type _Returns = Awaitable[None] | None
"""What a pub/sub handler returns: `None`, or an awaitable resolving to it."""


class _ConsumeDecorator[S: Any](Protocol):
    """Decorator form of `handle`, returning the Handler."""

    def __call__[R: _Returns](
        self, fn: Callable[[S], R]
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...


class _SignalConsumeDecorator(Protocol):
    """Decorator form of `handle` for a schema-less event, returning the Handler."""

    def __call__[R: _Returns](
        self, fn: Callable[[], R]
    ) -> Handler[[], RabbitMQConsumeConfig, R]: ...


class RabbitMQConsumer:
    """
    Async RabbitMQ consumer for pub/sub events.

    Each registration is its own AMQP consumer, so handlers on one queue compete. Use an
    `exchange` config to fan out. Bodies are decoded with `deserializer(body, type=schema)`.

    A message is acked once its handler, its hooks and its middleware return, and its scope, if
    any, has closed. If decoding, the handler, a `before` hook, a middleware or closing the
    scope raises, it's logged and nacked without requeue, or, if its registration has a
    {py:class}`RabbitMQFailurePolicy <stratae.integrations.rabbitmq.config.RabbitMQFailurePolicy>`,
    retried, dead-lettered or dropped under it. A failure inside the scope closes it with that
    error first, so its resources roll back. A delivery cancelled at shutdown isn't a failure:
    it stays unacked. Observing hooks in a slot run concurrently, so the ack waits for the
    slowest of them, not their sum; one that raises is logged and the message is still acked.
    Sync handlers run on the event loop and block other deliveries.

    Exiting the context shuts down gracefully: it cancels every AMQP consumer so no new
    messages arrive, waits up to `drain_timeout` for running handlers to finish and ack, then
    closes the connection. A handler still running at the timeout is cancelled by the close,
    its scope closes with the cancellation, and the broker redelivers its unacked message.
    Handlers running when the connection drops are cancelled the same way, and their messages
    redelivered once the consumer reconnects.
    """

    def __init__(
        self,
        url: str,
        deserializer: Deserializer,
        prefetch_count: int | None = None,
        drain_timeout: float | None = 30.0,
        *,
        scope: AsyncScope | None = None,
        reconnect: Backoff | None = RECONNECT,
        middleware: Middleware = Middleware(),
        envelope: EnvelopeConfig = EnvelopeConfig(),
    ) -> None:
        """
        Create a consumer.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param deserializer: Default body decoder, such as `msgspec.json.decode`. A
            registration's own deserializer overrides it.
        :param prefetch_count: Unacked messages delivered ahead; `None` for no limit.
        :param drain_timeout: Seconds exiting waits for running handlers before closing;
            `None` to wait for them indefinitely.
        :param scope: Activated for each message, around its middleware, hooks and handler,
            and closed before the ack or nack. A message that fails to decode never activates
            it.
        :param reconnect: The delays before each attempt to reconnect after the connection or
            channel closes unexpectedly, resuming every registration. Once it runs out, the
            consumer stays closed. `None` to stay closed at once. By default 0.5 s, doubling up
            to 30 s, without end.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `handle` half runs around every handler call, inside the message's
            envelope; a message that fails to decode never reaches it. Its `publish` half is a
            publisher's, so one `Middleware` can be shared with a publisher.
        :param envelope: How a delivered message's envelope is completed: its `new_id` makes
            any id the message is missing, or all of them if its envelope can't be read. Random
            by default.
        :raises TypeError: If any `handle` middleware is a class that isn't a context manager.
        """
        middleware.require(type(self).__name__, asynchronous=True, publish=False)
        self._new_id = envelope.new_id
        self._handle_middleware = middleware.handle
        self._scope = scope
        self._deserializer = deserializer
        self._prefetch_count = prefetch_count
        self._drain_timeout = drain_timeout
        self._link = Link("consumer", url, reconnect, self._resume, self._forget_consumers)
        self._registrations: dict[Handler[[Any], RabbitMQConsumeConfig, Any], _Registration] = {}
        self._starting: set[asyncio.Task[None]] = set()
        self._in_flight: set[asyncio.Task[Any]] = set()

    @property
    def state(self) -> ConnectionState:
        """
        The consumer's connection state, e.g. for a health check.

        A {py:class}`ConnectionState <stratae.integrations.rabbitmq.connection.ConnectionState>`.
        """
        return self._link.state

    async def wait_for_state(self, state: ConnectionState) -> None:
        """
        Wait until the consumer's connection is in `state`.

        Returns at once if it's already there. Otherwise returns when it next gets there, even
        if it has moved on by the time the caller resumes. Bound the wait with
        `asyncio.timeout`.

        :param state: The
            {py:class}`ConnectionState <stratae.integrations.rabbitmq.connection.ConnectionState>`
            to wait for.
        """
        await self._link.wait_for(state)

    async def __aenter__(self) -> RabbitMQConsumer:
        """
        Open the connection and start consuming for every registration.

        :returns: `self`.
        :raises AlreadyConnectedError: If already connected.
        """
        try:
            await self._link.open()
        except AlreadyConnectedError:
            raise
        except BaseException:
            self._forget_consumers()
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop consuming, wait for running handlers, and close the connection, if opened."""
        channel = self._link.channel
        try:
            if channel is not None:
                await self._drain(channel)
        finally:
            self._forget_consumers()
            await self._link.close()

    async def _resume(self, channel: AbstractChannel) -> None:
        """Set QoS on a newly opened channel and start consuming for every registration."""
        if self._prefetch_count is not None:
            await channel.basic_qos(prefetch_count=self._prefetch_count)
        for registration in tuple(self._registrations.values()):
            await self._start(channel, registration)

    def _forget_consumers(self) -> None:
        """Cancel pending starts and clear consumer tags, whose channel is gone or going."""
        for task in tuple(self._starting):
            task.cancel()
        for registration in self._registrations.values():
            registration.consumer_tag = None

    async def _drain(self, channel: AbstractChannel) -> None:
        """Cancel every AMQP consumer, then wait up to the drain timeout for running handlers."""
        try:
            for registration in self._registrations.values():
                if registration.consumer_tag is not None:
                    await channel.basic_cancel(registration.consumer_tag)
        except Exception:
            _log.warning("failed to cancel AMQP consumers on exit", exc_info=True)
        # A handler exiting its own consumer would otherwise wait on itself.
        running = self._in_flight - {asyncio.current_task()}
        if not running:
            return
        _, pending = await asyncio.wait(running, timeout=self._drain_timeout)
        if pending:
            _log.warning(
                "%d handler(s) still running after the drain timeout; their messages will be"
                " redelivered",
                len(pending),
            )

    @overload
    def handle[R: _Returns](
        self,
        event: PubSub[NoPayload, Literal[True]],
        fn: Callable[[], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[NoPayload, None] | None = None,
    ) -> Handler[[], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle(
        self,
        event: PubSub[NoPayload, Literal[True]],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[NoPayload, None] | None = None,
    ) -> _SignalConsumeDecorator: ...

    @overload
    def handle[S, R: _Returns](
        self,
        event: PubSub[S, Literal[False]],
        fn: Callable[[S], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[S, None] | None = None,
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle[S](
        self,
        event: PubSub[S, Literal[False]],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[S, None] | None = None,
    ) -> _ConsumeDecorator[S]: ...

    def handle(
        self,
        event: AnyEvent,
        fn: Callable[..., Any] | None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[Any, Any] | None = None,
    ) -> (
        Handler[Any, RabbitMQConsumeConfig, Any] | _ConsumeDecorator[Any] | _SignalConsumeDecorator
    ):
        """
        Register a handler for an event, as a decorator or direct call.

        Consumption starts now if connected, otherwise on entering the context.

        :param event: The {py:class}`PubSub <stratae.events.patterns.PubSub>` event to handle.
        :param fn: The handler, returning `None`. If omitted, returns a decorator.
        :param config: The queue or exchange to consume from.
        :param deserializer: Overrides the consumer's deserializer for this registration.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this handler on
            each delivery. A message that fails to decode never reaches the handler, so its
            hooks don't see it.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        """
        if not isinstance(event, PubSub):
            raise TypeError(_PUBSUB_ONLY.format(event))
        if fn is not None:
            return self._register(event, fn, config, deserializer, hooks)

        def decorator(fn: Callable[..., Any]) -> Handler[Any, RabbitMQConsumeConfig, Any]:
            return self._register(event, fn, config, deserializer, hooks)

        return decorator

    async def remove(self, handler: Handler[[Any], RabbitMQConsumeConfig, Any]) -> None:
        """
        Cancel and remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`.
        """
        registration = self._registrations.pop(handler, None)
        if registration is None:
            return
        channel = self._link.channel
        if registration.consumer_tag is not None and channel is not None:
            await channel.basic_cancel(registration.consumer_tag)
            registration.consumer_tag = None

    def _register(
        self,
        event: PubSub[Any, Any],
        fn: Callable[..., Any],
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None,
        hooks: Hooks[Any, Any] | None,
    ) -> Handler[[Any], RabbitMQConsumeConfig, Any]:
        """Wrap fn and its hooks as a Handler, consuming immediately if connected."""
        call, is_async = wrap_handler(
            event, config, fn, hooks, self._handle_middleware, asynchronous=True
        )
        handler: Handler[[Any], RabbitMQConsumeConfig, Any] = Handler(call, config, is_async)
        registration = _Registration(event, handler, deserializer)
        self._registrations[handler] = registration
        channel = self._link.channel
        if channel is not None:
            task = asyncio.get_running_loop().create_task(self._start(channel, registration))
            self._starting.add(task)
            task.add_done_callback(self._discard_start)
        return handler

    async def _start(self, channel: AbstractChannel, registration: _Registration) -> None:
        """Declare the registration's queue and start consuming, unless removed mid-start."""
        queue = await declare(channel, registration.handler.config)
        ok = await channel.basic_consume(queue, self._on_message(registration))
        registration.consumer_tag = ok.consumer_tag
        if registration.handler not in self._registrations and ok.consumer_tag is not None:
            registration.consumer_tag = None
            await channel.basic_cancel(ok.consumer_tag)

    def _discard_start(self, task: asyncio.Task[None]) -> None:
        """Drop a finished start task, logging any failure."""
        self._starting.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _log.error("failed to start AMQP consumer", exc_info=exc)

    def _deserialize(self, registration: _Registration, body: bytes) -> Any:
        """Decode a body with the registration's deserializer or the default."""
        if registration.event.is_signal:
            return NoPayload()
        deserializer = registration.deserializer or self._deserializer
        return deserializer(body, type=registration.event.schema)

    def _on_message(
        self, registration: _Registration
    ) -> Callable[[DeliveredMessage], Coroutine[Any, Any, None]]:
        """Return the delivery callback, tracked as in flight until it acks or nacks."""

        async def on_message(message: DeliveredMessage) -> None:
            task = cast(asyncio.Task[Any], asyncio.current_task())
            self._in_flight.add(task)
            try:
                await self._deliver(registration, message)
            finally:
                self._in_flight.discard(task)

        return on_message

    async def _deliver(self, registration: _Registration, message: DeliveredMessage) -> None:
        """Decode a message and dispatch it in its scope, then ack it, or settle its failure."""
        try:
            payload = self._deserialize(registration, message.body)
        except Exception as exc:
            await self._fail(registration, message, exc, retry=False)
            return
        try:
            with Envelope.scope(envelope_from(message, self._new_id), new_id=self._new_id):
                if self._handle_middleware:
                    with receiving(headers_from(message)):
                        await self._run_handler(registration, payload)
                else:
                    await self._run_handler(registration, payload)
        except Exception as exc:
            await self._fail(registration, message, exc, retry=True)
            return
        if message.delivery_tag is not None:
            await message.channel.basic_ack(message.delivery_tag)

    async def _run_handler(self, registration: _Registration, payload: Any) -> None:
        """Call the registration's handler with a decoded payload, in the scope if one is set."""
        if self._scope is None:
            await invoke(registration.handler, payload)
        else:
            async with self._scope:
                await invoke(registration.handler, payload)

    async def _fail(
        self,
        registration: _Registration,
        message: DeliveredMessage,
        exc: Exception,
        *,
        retry: bool,
    ) -> None:
        """Nack a failed message, or retry, dead-letter or drop it under its failure policy."""
        policy = registration.handler.config.on_failure
        name = registration.event.name
        if policy is None:
            _log.error(
                "handler for event '%s' failed; message nacked without requeue", name, exc_info=exc
            )
            if message.delivery_tag is not None:
                await message.channel.basic_nack(message.delivery_tag, requeue=False)
            return
        queue = cast(str, registration.handler.config.queue)
        attempts = failed_attempts(message) + 1
        target = policy.retry_queue(queue, attempts) if retry else None
        if target is not None:
            _log.warning(
                "handler for event '%s' failed; retrying via '%s'", name, target, exc_info=exc
            )
        else:
            target = policy.dead_letter_target(queue)
            _log.error(
                "handler for event '%s' failed after %d attempt(s); message %s",
                name,
                attempts,
                f"dead-lettered to '{target}'" if target is not None else "dropped",
                exc_info=exc,
            )
        if message.delivery_tag is None:
            return
        if target is not None:
            await message.channel.basic_publish(
                message.body,
                exchange="",
                routing_key=target,
                properties=failed_properties(message, attempts, exc),
            )
        await message.channel.basic_ack(message.delivery_tag)
