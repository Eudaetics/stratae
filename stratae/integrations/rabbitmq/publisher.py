"""The RabbitMQ publisher."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, overload

from aiormq.exceptions import PublishError

from stratae.backoff import Backoff
from stratae.events import (
    AnyEvent,
    EmitCallable,
    Encoded,
    Envelope,
    EnvelopeFactory,
    NoPayload,
    PubSub,
    Serializer,
    reserved_for,
)
from stratae.events.binding import bind
from stratae.events.by_pattern import ByPattern
from stratae.events.exceptions import NotConnectedError, UnroutableError
from stratae.events.hooks import Hooks
from stratae.events.kind import ASYNC
from stratae.events.middleware import Middleware
from stratae.events.pipeline import Outgoing, Publishing, publish_enveloped_async
from stratae.integrations.rabbitmq._link import RECONNECT, Link
from stratae.integrations.rabbitmq._topology import declare
from stratae.integrations.rabbitmq._wire import stamp
from stratae.integrations.rabbitmq.config import (
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQPublish,
)
from stratae.integrations.rabbitmq.connection import ConnectionState

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel


_NOT_CONNECTED = "publisher is not connected; open it with 'async with' before emitting"

_RECONNECTING = "publisher lost its broker connection and is reconnecting; retry the publish"


_UNROUTABLE = "message to exchange {!r} with routing key {!r} reached no queue"

_NO_ADDRESS = "{!r} is routed by a function of its payload, so an Encoded payload needs its address"

_NO_ROUTE = "no route for {!r}; route or bind it to a RabbitMQConfig before emitting it"

_ROUTE_TAKEN = "{!r} is already routed to another RabbitMQConfig; give it one route per publisher"

_TOPOLOGY_NEEDS_QUEUE = (
    "a publisher's topology declares queues for consumers, so each config needs a named queue"
)


@dataclass(frozen=True, slots=True)
class _Keyed:
    """A route whose messages each get their routing key from their payload."""

    route: RabbitMQConfig
    """The route."""
    key: Callable[[Any], str]
    """Gives each message's routing key from its payload."""


type _Route = RabbitMQPublish | _Keyed
"""
An event's route: what each of its messages is published as, when they share a routing key,
or how to make it from the payload.
"""


def _route(config: RabbitMQConfig) -> _Route:
    """Return the route a config describes, settling a fixed routing key once."""
    key = config.routing_key
    return RabbitMQPublish(config, key) if isinstance(key, str) else _Keyed(config, key)


_PUBLISHES: ByPattern[bool] = ByPattern(
    "RabbitMQPublisher", [(PubSub, True)], reason="it publishes PubSub events only"
)
"""The patterns a publisher publishes; calling it with any other event raises `TypeError`."""


class RabbitMQPublisher:
    """
    Async RabbitMQ publisher for pub/sub events.

    Holds one connection and channel for the `async with` block. Each event is routed to one
    {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.config.RabbitMQConfig>`, by `bind`,
    `emitter` or `route`, and `emit` publishes on that route. It implements
    {py:class}`PubSub <stratae.events.patterns.PubSub>` and its subclasses only, so it matches
    {py:class}`PubSubProducer <stratae.events.protocols.PubSubProducer>`. Binding or emitting
    any other event is a type error, and raises `TypeError`.

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
        envelope_factory: EnvelopeFactory = EnvelopeFactory(),
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
        :param envelope_factory: Makes the envelope of a message published without a given one,
            and names the headers every message's envelope is sent in. Its `new_id` makes random ids
            by default. A message is refused if it's given, or middleware adds, one of its
            `headers`' names. Its consumers need the same `headers`.
        :raises MiddlewareKindError: If any `publish` middleware isn't an `async def`.
        :raises ValueError: If a `topology` config has no named `queue`.
        """
        middleware.require(type(self).__name__, ASYNC, handle=False)
        self._topology = tuple(topology)
        for config in self._topology:
            if config.queue is None:
                raise ValueError(_TOPOLOGY_NEEDS_QUEUE)
        self._publishing: Publishing[RabbitMQPublish, Envelope] = Publishing(
            middleware.publish, asynchronous=True
        )
        self._envelope_factory = envelope_factory
        self._serializer = serializer
        self._routes: dict[AnyEvent, tuple[_Route, frozenset[str]]] = {}
        self._declared: set[str] = set()
        self._link = Link("publisher", url, reconnect, self._declare_topology, self._declared.clear)

    @property
    def envelope_factory(self) -> EnvelopeFactory:
        """Makes the publisher's envelopes, and names the headers it sends them in."""
        return self._envelope_factory

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
        :raises TypeError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event is already routed to a different config.
        """
        _PUBLISHES(event)
        reserved = reserved_for(config, self._envelope_factory.headers)
        routed = self._routes.setdefault(event, (_route(config), reserved))
        if routed[0].route != config:
            raise ValueError(_ROUTE_TAKEN.format(event.name))

    async def emit[Payload](
        self,
        event: PubSub[Payload, Any],
        payload: Payload | Encoded,
        *,
        hooks: Hooks[Payload, None] | None = None,
        envelope: Envelope | None = None,
        headers: Mapping[str, object] | None = None,
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
        :raises TypeError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises UnroutableError: If `config.mandatory` is set and the message reached no queue.
        :raises aiormq.exceptions.DeliveryError: If the broker refuses the message.
        :raises aiormq.exceptions.ChannelClosed: If the channel closes before the broker
            confirms, e.g. because the exchange doesn't exist.
        """
        routed = self._routes.get(event)
        if routed is None:
            _PUBLISHES(event)
            raise LookupError(_NO_ROUTE.format(event.name))
        route, reserved = routed
        if isinstance(payload, Encoded):
            address = payload.address
            if address is not None:
                publish = RabbitMQPublish(route.route, address)
            elif isinstance(route, _Keyed):
                raise ValueError(_NO_ADDRESS.format(event.name))
            else:
                publish = route
        elif isinstance(route, _Keyed):
            publish = RabbitMQPublish(route.route, route.key(payload))
        else:
            publish = route
        await publish_enveloped_async(
            self._publishing,
            self._envelope_factory,
            self._send,
            reserved,
            event,
            payload,
            publish,
            hooks,
            envelope,
            headers,
        )

    async def _send(self, message: Outgoing[RabbitMQPublish, Envelope]) -> None:
        """Serialize a message's payload and publish it to its route's exchange."""
        event, publish, payload = message.event, message.config, message.payload
        envelope, headers, route = message.envelope, message.headers, publish.route
        channel = self._link.channel
        if channel is None:
            reconnecting = self._link.state is ConnectionState.RECONNECTING
            raise NotConnectedError(_RECONNECTING if reconnecting else _NOT_CONNECTED)
        await self._declare_exchange(channel, route)
        body = self._encode(event, payload, route)
        try:
            await channel.basic_publish(
                body,
                exchange=route.exchange,
                routing_key=publish.routing_key,
                properties=stamp(
                    route.properties, envelope, headers, self._envelope_factory.headers
                ),
                mandatory=route.mandatory,
            )
        except PublishError as exc:
            raise UnroutableError(_UNROUTABLE.format(route.exchange, publish.routing_key)) from exc

    def emitter[Payload](
        self, event: PubSub[Payload, Any], config: RabbitMQConfig, /
    ) -> EmitCallable[PubSub[Payload, Any], Payload, Hooks[Payload, None], Awaitable[None]]:
        """
        Route a PubSub event to `config`, and return an emitter of it.

        Binding it with {py:func}`bind <stratae.events.binding.bind>` gives what its `bind` method
        does.

        :param event: The PubSub event.
        :param config: Where its messages go.
        :returns: An emitter taking the event, a payload and `hooks=`, returning an awaitable.
        :raises TypeError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event is already routed to a different config.
        """
        self.route(event, config)
        return self.emit

    @overload
    def bind[**P, Payload](
        self,
        event: PubSub[Payload, Literal[False]],
        *,
        factory: Callable[P, Payload] | Callable[P, Awaitable[Payload]],
        config: RabbitMQConfig,
        hooks: Hooks[Payload, None] | None = None,
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
    def bind[Payload](
        self,
        event: PubSub[Payload, Literal[False]],
        *,
        factory: None = None,
        config: RabbitMQConfig,
        hooks: Hooks[Payload, None] | None = None,
    ) -> Callable[[Payload], Awaitable[None]]: ...

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

        See {py:func}`bind <stratae.events.binding.bind>`.

        :param event: The PubSub event to bind.
        :param factory: Builds the payload from the call's arguments, sync or async.
        :param config: Where its messages go.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each publish.
        :returns: An async callable that publishes the event.
        :raises TypeError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event is already routed to a different config.
        """
        return bind(event, self.emitter(event, config), factory=factory, hooks=hooks)

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
