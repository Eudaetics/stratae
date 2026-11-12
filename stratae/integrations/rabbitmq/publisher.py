"""The RabbitMQ publisher."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from aiormq.exceptions import PublishError

from stratae.backoff import Backoff
from stratae.events import (
    AnyEvent,
    Encoded,
    Envelope,
    EnvelopeConfig,
    NoPayload,
    PubSub,
    Serializer,
)
from stratae.events.binding import abind
from stratae.events.exceptions import NotConnectedError, UnroutableError
from stratae.events.hooks import Hooks
from stratae.events.middleware import Middleware
from stratae.events.pipeline import Outgoing, Publishing, publish_async
from stratae.integrations.rabbitmq._link import RECONNECT, Link
from stratae.integrations.rabbitmq._topology import declare
from stratae.integrations.rabbitmq._wire import RESERVED, stamp
from stratae.integrations.rabbitmq.config import RabbitMQConfig, RabbitMQConsumeConfig
from stratae.integrations.rabbitmq.connection import ConnectionState

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel


_NOT_CONNECTED = "publisher is not connected; open it with 'async with' before emitting"

_RECONNECTING = "publisher lost its broker connection and is reconnecting; retry the publish"

_REQUEST_REPLY_NOT_IMPLEMENTED = (
    "RabbitMQ publishes PubSub events only; request/reply is not yet implemented"
)

_UNROUTABLE = "message to exchange {!r} with routing key {!r} reached no queue"

_NO_ADDRESS = "{!r} is routed by a function of its payload, so an Encoded payload needs its address"

_NO_ROUTE = "no route for {!r}; route or bind it to a RabbitMQConfig before emitting it"

_ROUTE_TAKEN = "{!r} is already routed to another RabbitMQConfig; give it one route per publisher"

_TOPOLOGY_NEEDS_QUEUE = (
    "a publisher's topology declares queues for consumers, so each config needs a named queue"
)


def _require_pubsub(event: AnyEvent) -> None:
    """Raise NotImplementedError for an event that isn't PubSub."""
    if not isinstance(event, PubSub):
        raise NotImplementedError(_REQUEST_REPLY_NOT_IMPLEMENTED)


class RabbitMQPublisher:
    """
    Async RabbitMQ publisher for pub/sub events.

    Holds one connection and channel for the `async with` block. Each event is routed to one
    {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.config.RabbitMQConfig>`, by `bind`
    or by `route`, and `emit` publishes on that route. It implements
    {py:class}`PubSub <stratae.events.patterns.PubSub>` and its subclasses only, so it matches
    {py:class}`PubSubProducer <stratae.events.protocols.PubSubProducer>`. Binding or emitting
    any other event is a type error, and raises `NotImplementedError`.

    Publishing uses publisher confirms: `emit` returns only once the broker has taken
    responsibility for the message, and raises if the broker refuses it or the channel closes
    first. A message that reaches an exchange but matches no queue is still confirmed, and
    dropped; set `mandatory` on its
    {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.config.RabbitMQConfig>` to raise
    {py:exc}`UnroutableError <stratae.events.exceptions.UnroutableError>` instead.

    A message routed to no queue is dropped by the broker, so one published before its consumers
    have ever run is lost. Pass the consumers'
    {py:class}`RabbitMQConsumeConfig <stratae.integrations.rabbitmq.config.RabbitMQConsumeConfig>`s
    as `topology` to declare their queues, exchanges, bindings and failure queues when the
    publisher connects, exactly as the consumers will, so the broker keeps the messages until
    they start.
    """

    def __init__(
        self,
        url: str,
        serializer: Serializer[object],
        *,
        topology: Iterable[RabbitMQConsumeConfig] = (),
        reconnect: Backoff | None = RECONNECT,
        middleware: Middleware = Middleware(),
        envelope: EnvelopeConfig = EnvelopeConfig(),
    ) -> None:
        """
        Create a publisher.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param serializer: Default payload serializer, such as `msgspec.json.encode`. A route's
            own serializer, on its
            {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.config.RabbitMQConfig>`,
            overrides it.
        :param topology: Consume configs whose queues, exchanges, bindings and failure queues
            are declared on entering the context, before anything is published. Each needs a
            named `queue`. They're declared again after each reconnect.
        :param reconnect: The delays before each attempt to reconnect after the connection or
            channel closes unexpectedly. Once it runs out, the publisher stays closed. `None`
            to stay closed at once. By default 0.5 s, doubling up to 30 s, without end.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `publish` half runs around every publish, inside the message's
            envelope; its `handle` half is a consumer's, so one `Middleware` can be shared with
            a consumer.
        :param envelope: How the envelope of a message published without a given one is made.
            Its `new_id` makes random ids by default.
        :raises TypeError: If any `publish` middleware is a class that isn't a context manager.
        :raises ValueError: If a `topology` config has no named `queue`.
        """
        middleware.require(type(self).__name__, asynchronous=True, handle=False)
        self._topology = tuple(topology)
        for config in self._topology:
            if config.queue is None:
                raise ValueError(_TOPOLOGY_NEEDS_QUEUE)
        self._publishing = Publishing(middleware.publish, envelope.new_id, RESERVED)
        self._serializer = serializer
        self._routes: dict[AnyEvent, RabbitMQConfig] = {}
        self._declared: set[str] = set()
        self._link = Link("publisher", url, reconnect, self._declare_topology, self._declared.clear)

    @property
    def state(self) -> ConnectionState:
        """
        The publisher's connection state, e.g. for a health check.

        A {py:class}`ConnectionState <stratae.integrations.rabbitmq.connection.ConnectionState>`.
        """
        return self._link.state

    async def wait_for_state(self, state: ConnectionState) -> None:
        """
        Wait until the publisher's connection is in `state`.

        Returns at once if it's already there. Otherwise returns when it next gets there, even
        if it has moved on by the time the caller resumes. Bound the wait with
        `asyncio.timeout`.

        :param state: The
            {py:class}`ConnectionState <stratae.integrations.rabbitmq.connection.ConnectionState>`
            to wait for.
        """
        await self._link.wait_for(state)

    async def __aenter__(self) -> RabbitMQPublisher:
        """
        Open the connection and channel, and declare the topology.

        :returns: `self`.
        :raises AlreadyConnectedError: If already connected.
        :raises aiormq.exceptions.ChannelPreconditionFailed: If a topology declaration
            conflicts with one the broker already holds. The publisher is left disconnected.
        """
        await self._link.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop reconnecting and close the AMQP connection, if one was opened."""
        self._declared.clear()
        await self._link.close()

    async def _declare_topology(self, channel: AbstractChannel) -> None:
        """Declare every topology config on a newly opened channel."""
        for config in self._topology:
            await declare(channel, config)

    def route(self, event: PubSub[Any, Any], config: RabbitMQConfig) -> None:
        """
        Route an event's messages to an exchange and routing key.

        `bind` routes the event it binds. Routing it again to an equal config does nothing.

        :param event: The PubSub event to route.
        :param config: Where its messages go.
        :raises NotImplementedError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event is already routed to a different config.
        """
        _require_pubsub(event)
        if self._routes.setdefault(event, config) != config:
            raise ValueError(_ROUTE_TAKEN.format(event.name))

    async def emit[S](
        self,
        event: PubSub[S, Any],
        payload: S | Encoded,
        *,
        hooks: Hooks[S, None] | None = None,
        envelope: Envelope | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """
        Serialize a payload and publish it on the event's route.

        :param event: The PubSub event being emitted.
        :param payload: The payload to publish.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around the publish,
            inside the message's envelope, so they see the ids it's sent with.
        :param envelope: Sends the message with this envelope's ids and timestamp, unchanged,
            instead of a child of the active one. Pass the envelope a message was first sent
            with to re-publish it, so consumers can deduplicate on its message id.
        :param headers: Headers to send the message with, which publish middleware sees and
            can add to. Pass a message's headers to re-publish it with them.
        :raises ValueError: If a header is one the publisher sets itself: an envelope header,
            {py:data}`ATTEMPTS_HEADER <stratae.integrations.rabbitmq.config.ATTEMPTS_HEADER>` or
            {py:data}`FAILURE_HEADER <stratae.integrations.rabbitmq.config.FAILURE_HEADER>`.
        :raises NotConnectedError: If the publisher isn't connected.
        :raises LookupError: If the event has no route.
        :raises NotImplementedError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises UnroutableError: If `config.mandatory` is set and the message reached no queue.
        :raises aiormq.exceptions.DeliveryError: If the broker refuses the message.
        :raises aiormq.exceptions.ChannelClosed: If the channel closes before the broker
            confirms, e.g. because the exchange doesn't exist.
        """
        _require_pubsub(event)
        config = self._routes.get(event)
        if config is None:
            raise LookupError(_NO_ROUTE.format(event.name))
        if payload.__class__ is Encoded:
            address = cast(Encoded, payload).address
            if address is not None:
                config = config.at(address)
            elif config.routing_key.__class__ is not str:
                raise ValueError(_NO_ADDRESS.format(event.name))
        elif config.routing_key.__class__ is not str:
            config = config.at(cast(Callable[[Any], str], config.routing_key)(payload))
        await publish_async(
            self._publishing, self._send, event, payload, config, hooks, envelope, headers
        )

    async def _send(self, message: Outgoing) -> None:
        """Serialize a message's payload and publish it to its config's exchange."""
        event, config, payload = message.event, message.config, message.payload
        envelope, headers = cast(Envelope, message.envelope), message.headers
        channel = self._link.channel
        if channel is None:
            reconnecting = self._link.state is ConnectionState.RECONNECTING
            raise NotConnectedError(_RECONNECTING if reconnecting else _NOT_CONNECTED)
        await self._declare_exchange(channel, config)
        body = self._encode(event, payload, config)
        try:
            await channel.basic_publish(
                body,
                exchange=config.exchange,
                routing_key=cast(str, config.routing_key),
                properties=stamp(config.properties, envelope, headers),
                mandatory=config.mandatory,
            )
        except PublishError as exc:
            raise UnroutableError(_UNROUTABLE.format(config.exchange, config.routing_key)) from exc

    @overload
    def bind[**P, S](
        self,
        event: PubSub[S, Literal[False]],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: RabbitMQConfig,
        hooks: Hooks[S, None] | None = None,
    ) -> Callable[P, Awaitable[None]]: ...
    @overload
    def bind(
        self,
        event: PubSub[NoPayload, Literal[True]],
        *,
        factory: None = None,
        config: RabbitMQConfig,
        hooks: Hooks[NoPayload, None] | None = None,
    ) -> Callable[[], Awaitable[None]]: ...
    @overload
    def bind[S](
        self,
        event: PubSub[S, Literal[False]],
        *,
        factory: None = None,
        config: RabbitMQConfig,
        hooks: Hooks[S, None] | None = None,
    ) -> Callable[[S], Awaitable[None]]: ...

    def bind(
        self,
        event: PubSub[Any, Any],
        *,
        factory: Callable[..., Any] | None = None,
        config: RabbitMQConfig,
        hooks: Hooks[Any, None] | None = None,
    ) -> Callable[..., Awaitable[None]]:
        """
        Route a PubSub event to `config`, and bind it to this publisher.

        See {py:func}`abind <stratae.events.binding.abind>`.

        :param event: The PubSub event to bind.
        :param factory: Builds the payload from the call's arguments, sync or async.
        :param config: Where its messages go.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each publish.
        :returns: An async callable that publishes the event.
        :raises NotImplementedError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event is already routed to a different config.
        """
        self.route(event, config)
        return abind(event, self.emit, factory=factory, hooks=hooks)

    async def _declare_exchange(self, channel: AbstractChannel, config: RabbitMQConfig) -> None:
        """Declare config's exchange once, if it carries a type."""
        if config.exchange_type is None or config.exchange in self._declared:
            return
        await channel.exchange_declare(
            config.exchange, exchange_type=config.exchange_type, durable=config.exchange_durable
        )
        self._declared.add(config.exchange)

    def _encode(self, event: AnyEvent, payload: Any, config: RabbitMQConfig) -> bytes:
        """Serialize payload with the route's serializer or the publisher's, or pass bytes on."""
        if payload.__class__ is Encoded:
            return payload.body
        if event.is_signal:
            return b""
        return (config.serializer or self._serializer)(payload)
