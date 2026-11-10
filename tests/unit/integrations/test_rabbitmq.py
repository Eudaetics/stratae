"""
Unit tests for the RabbitMQ publish adapter.

This test suite verifies the following behaviors:

RabbitMQConfig:
- The exchange and routing_key are stored on initialization.
- The exchange_type is stored on initialization, defaulting to None.

RabbitMQPublisher:
- Constructing a publisher without a serializer is a type error and raises TypeError.
- Entering the context opens the connection and allocates a channel.
- Exiting the context closes the connection.
- Exiting without a connection closes nothing.
- Entering the context while already connected raises AlreadyConnectedError.
- A failed close on exit still leaves the publisher disconnected.
- emit raises NotConnectedError before the context is entered.
- emit publishes the serialized payload to the configured exchange
  and routing key.
- emit uses the binding's serializer when one is provided.
- Awaiting a factory-bound callable produced by bind constructs and publishes the payload.
- emit declares an exchange_type-carrying config's exchange once, before
  its first publish.
- emit declares no exchange for configs without an exchange_type.
- emit publishes the config's AMQP properties with each message, without
  mutating the config's properties object.
- emit stamps envelope ids onto every message as x- headers and native
  fields, chaining a child of the active envelope scope.
- emit raises NotImplementedError for a request/reply event.
- bind raises NotImplementedError for a request/reply event.
- emit publishes an empty body for a schema-less event.
- emit runs the hooks passed to it around the publish, with a reply of None.
- A failed publish reaches the failure hook, and still propagates.
- Publish hooks run inside the message's envelope, seeing the ids it's sent with.
- Publish middleware runs around the publish and its hooks, inside the message's envelope.
- Publish middleware sees the emit's RabbitMQConfig as call.config.
- A publish middleware class that isn't a context manager is refused when the publisher is
  built; handle middleware isn't checked, so one Middleware can be shared with a consumer.
- emit given an envelope sends its ids and timestamp unchanged, as x- headers and native
  fields, instead of a child of the active envelope.
- Emitting twice with one envelope sends the same message id both times.
- Publish middleware and hooks see the given envelope.
- Headers publish middleware adds are sent with the config's and the envelope's; without
  middleware, a message carries the envelope's headers alone.
- emit given headers sends them, and publish middleware sees them and can add to them.
- A reserved header, set by middleware or given to emit, raises ValueError, and nothing is
  published: the envelope's, the attempts header and the failure header.
- A topology config without a named queue is refused when the publisher is built.
- Entering the context declares each topology config's queue, failure queues, exchange and
  bindings, as a consumer would.
- A failed topology declaration closes the connection and leaves the publisher disconnected.
- A publisher without topology declares no queues on entering.
- RabbitMQConfig doesn't require routing unless mandatory is set.
- The publisher opens its channel with publisher confirms and raising returns.
- emit publishes with the config's mandatory flag.
- A returned mandatory message raises UnroutableError naming its exchange and routing key.
- emit doesn't return until the broker's confirm arrives.

Connection health and reconnecting:
- RabbitMQReconnect refuses inconsistent delays, and its waits grow by backoff up to the cap.
- The publisher reports closed before entering and after exiting, and connected inside.
- When the channel closes unexpectedly, the publisher reports reconnecting, emit raises
  NotConnectedError at once, and after reconnecting it's connected, has declared its topology
  again, and publishes on the new channel.
- A failed reconnect attempt is logged and retried.
- Exiting while reconnecting stops trying and leaves the publisher closed.
- With reconnecting turned off, a lost channel leaves the publisher closed, and emit raises
  NotConnectedError.
- Closing the publisher on purpose doesn't start reconnecting.
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast
from unittest.mock import AsyncMock, call
from uuid import UUID

import pytest
from aiormq.exceptions import PublishError
from pamqp.commands import Basic
from pytest_mock import MockerFixture

from stratae.events import (
    CAUSATION_ID_HEADER,
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    Envelope,
    EnvelopeConfig,
    Hooks,
    Middleware,
    NoPayload,
    PublishCall,
    PubSub,
    Request,
)
from stratae.events.exceptions import AlreadyConnectedError, NotConnectedError, UnroutableError
from stratae.integrations.rabbitmq import (
    ConnectionState,
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQFailurePolicy,
    RabbitMQPublisher,
    RabbitMQReconnect,
)

_URL = "amqp://guest:guest@localhost/"


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


def _encode(payload: object) -> bytes:
    """Serialize a test payload's attributes as JSON."""
    return json.dumps(vars(payload)).encode()


_order_placed = PubSub(_OrderPlaced)
_config = RabbitMQConfig("events", "order.placed")


@pytest.fixture
def connection(channel: AsyncMock) -> AsyncMock:
    """Return a mock AMQP connection that allocates the mock channel."""
    connection = AsyncMock()
    connection.channel.return_value = channel
    return connection


@pytest.fixture
def connect_mock(mocker: MockerFixture, connection: AsyncMock) -> AsyncMock:
    """Patch aiormq's connect in the adapter module and return the mock."""
    mock = AsyncMock(return_value=connection)
    mocker.patch("stratae.integrations.rabbitmq.connect", mock)
    return mock


@pytest.fixture
def publisher(connect_mock: AsyncMock) -> RabbitMQPublisher:
    """Return a RabbitMQPublisher wired to the mocked connection."""
    return RabbitMQPublisher(_URL, serializer=_encode)


def test_config_stores_exchange_and_routing_key():
    """
    RabbitMQConfig should store its routing fields.

    Given: An exchange and a routing key
    When: A RabbitMQConfig is created
    Then: Both fields should be stored as attributes
    """
    config = RabbitMQConfig("events", "order.placed")

    assert config.exchange == "events"
    assert config.routing_key == "order.placed"


def test_config_stores_exchange_type():
    """
    RabbitMQConfig should store its exchange type.

    Given: An exchange type
    When: A RabbitMQConfig is created with and without one
    Then: The exchange type should be stored, defaulting to None
    """
    declared = RabbitMQConfig("events", "order.placed", exchange_type="fanout")
    plain = RabbitMQConfig("events", "order.placed")

    assert declared.exchange_type == "fanout"
    assert plain.exchange_type is None


async def test_context_opens_connection_and_channel(
    publisher: RabbitMQPublisher, connect_mock: AsyncMock, connection: AsyncMock
):
    """
    Entering the context should connect and allocate a channel.

    Given: A publisher with a mocked connection
    When: The async context is entered
    Then: connect should be awaited with the URL and a channel allocated
    """
    # Act
    async with publisher as entered:
        pass

    # Assert
    assert entered is publisher
    connect_mock.assert_awaited_once_with(_URL)
    connection.channel.assert_awaited_once()


async def test_context_closes_connection_on_exit(
    publisher: RabbitMQPublisher, connection: AsyncMock
):
    """
    Exiting the context should close the connection.

    Given: A publisher whose context has been entered
    When: The context exits
    Then: The connection should be closed
    """
    # Act
    async with publisher:
        pass

    # Assert
    connection.close.assert_awaited_once()


async def test_exit_without_connection_is_noop(
    publisher: RabbitMQPublisher, connect_mock: AsyncMock, connection: AsyncMock
):
    """
    Exiting a publisher that never connected should close nothing.

    Given: A publisher whose context was never entered
    When: The context exits
    Then: No connection should be opened or closed
    """
    # Act
    await publisher.__aexit__(None, None, None)

    # Assert
    connect_mock.assert_not_awaited()
    connection.close.assert_not_awaited()


async def test_context_reentered_while_connected_raises(
    publisher: RabbitMQPublisher, connect_mock: AsyncMock
):
    """
    Entering the context again while connected should raise instead of orphaning the connection.

    Given: A publisher whose context has been entered
    When: The context is entered again
    Then: AlreadyConnectedError should be raised without opening a second connection
    """
    # Act & Assert
    async with publisher:
        with pytest.raises(AlreadyConnectedError):
            await publisher.__aenter__()

    connect_mock.assert_awaited_once()


async def test_failed_close_leaves_publisher_disconnected(
    publisher: RabbitMQPublisher, connection: AsyncMock
):
    """
    A close that raises on exit should still leave the publisher disconnected.

    Given: A publisher whose connection's close raises
    When: The context exits
    Then: The error propagates, and emitting afterwards raises NotConnectedError
    """
    # Arrange
    connection.close.side_effect = ConnectionError("broker gone")

    # Act
    with pytest.raises(ConnectionError):
        async with publisher:
            pass

    # Assert
    with pytest.raises(NotConnectedError):
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))


async def test_emit_without_connection_raises(publisher: RabbitMQPublisher):
    """
    ``emit`` should raise NotConnectedError before the context is entered.

    Given: A publisher whose context has not been entered
    When: emit is awaited
    Then: A NotConnectedError should be raised
    """
    with pytest.raises(NotConnectedError):
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))


async def test_emit_raises_not_implemented_for_request(publisher: RabbitMQPublisher):
    """
    ``emit`` should raise NotImplementedError for a request/reply event.

    Given: A publisher and an event using the Request dispatch pattern
    When: emit is awaited with that event
    Then: A NotImplementedError should be raised
    """
    request_event = Request(_OrderPlaced, reply=_OrderPlaced)

    with pytest.raises(NotImplementedError):
        await publisher.emit(request_event, _config, _OrderPlaced(1))


def test_bind_raises_not_implemented_for_request(publisher: RabbitMQPublisher):
    """
    ``bind`` should refuse a request/reply event before anything emits it.

    Given: A publisher and a Request event
    When: The event is bound
    Then: A NotImplementedError should be raised
    """
    # Arrange
    request_event = Request(_OrderPlaced, reply=_OrderPlaced)

    # Act / Assert
    with pytest.raises(NotImplementedError, match="PubSub events only"):
        publisher.bind(request_event, factory=_OrderPlaced, config=_config)


async def test_emit_publishes_packed_payload(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should publish the serialized payload to the configured target.

    Given: A connected publisher and a payload
    When: emit is awaited without a serializer
    Then: The payload should be serialized and published with the config's
          exchange and routing key
    """
    # Arrange
    payload = _OrderPlaced(7)

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, payload)

    # Assert
    call = channel.basic_publish.await_args
    assert call.args[0] == _encode(payload)
    assert call.kwargs["exchange"] == "events"
    assert call.kwargs["routing_key"] == "order.placed"


async def test_emit_publishes_empty_body_for_signal_event(
    publisher: RabbitMQPublisher, channel: AsyncMock
):
    """
    ``emit`` should publish an empty body for a schema-less event.

    Given: A connected publisher and a schema-less event
    When: emit is awaited with that event
    Then: An empty body should be published, without invoking the serializer
    """
    # Arrange
    heartbeat_event = PubSub(name="heartbeat")

    # Act
    async with publisher:
        await publisher.emit(heartbeat_event, _config, NoPayload())

    # Assert
    call = channel.basic_publish.await_args
    assert call.args[0] == b""


async def test_emit_uses_custom_serializer(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should use the supplied serializer instead of the publisher's.

    Given: A connected publisher and a custom serializer
    When: emit is awaited with the serializer
    Then: The serializer's bytes should be published
    """

    # Arrange
    def to_bytes(_: object) -> bytes:
        return b"custom"

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(7), serializer=to_bytes)

    # Assert
    call = channel.basic_publish.await_args
    assert call.args[0] == b"custom"
    assert call.kwargs["exchange"] == "events"
    assert call.kwargs["routing_key"] == "order.placed"


async def test_emit_declares_exchange_once(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should declare an exchange_type-carrying config's exchange once.

    Given: A connected publisher and a config carrying an exchange type
    When: emit is awaited twice with the config
    Then: The exchange should be declared once, and both payloads published
    """
    # Arrange
    config = RabbitMQConfig("events", "order.placed", exchange_type="fanout")

    # Act
    async with publisher:
        await publisher.emit(_order_placed, config, _OrderPlaced(1))
        await publisher.emit(_order_placed, config, _OrderPlaced(2))

    # Assert
    channel.exchange_declare.assert_awaited_once_with(
        "events", exchange_type="fanout", durable=False
    )
    assert channel.basic_publish.await_count == 2


async def test_emit_skips_declaration_without_exchange_type(
    publisher: RabbitMQPublisher, channel: AsyncMock
):
    """
    ``emit`` should not declare exchanges for configs without an exchange type.

    Given: A connected publisher and a config without an exchange type
    When: emit is awaited
    Then: No exchange should be declared
    """
    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))

    # Assert
    channel.exchange_declare.assert_not_awaited()


async def test_emit_publishes_with_properties(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should publish the config's AMQP properties with the message.

    Given: A connected publisher and a config carrying properties and headers
    When: emit is awaited
    Then: The published properties should keep the config's values alongside
          the envelope stamp, and the config's object should not be mutated
    """
    # Arrange
    properties = Basic.Properties(delivery_mode=2, headers={"custom": "v"})
    config = RabbitMQConfig("events", "order.placed", properties=properties)

    # Act
    async with publisher:
        await publisher.emit(_order_placed, config, _OrderPlaced(7))

    # Assert
    sent = channel.basic_publish.await_args.kwargs["properties"]
    assert sent.delivery_mode == 2
    assert sent.headers["custom"] == "v"
    assert MESSAGE_ID_HEADER in sent.headers
    assert properties.headers == {"custom": "v"}


async def test_emit_stamps_envelope(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should stamp a child of the active envelope onto the message.

    Given: A connected publisher inside an envelope scope
    When: emit is awaited
    Then: The message should carry x- headers and native fields for a child
          envelope; same correlation chain, caused by the active envelope
    """
    # Act
    with Envelope.scope() as envelope:
        async with publisher:
            await publisher.emit(_order_placed, _config, _OrderPlaced(7))

    # Assert
    sent = channel.basic_publish.await_args.kwargs["properties"]
    assert sent.headers[CORRELATION_ID_HEADER] == str(envelope.correlation_id)
    assert sent.headers[CAUSATION_ID_HEADER] == str(envelope.message_id)
    assert sent.headers[MESSAGE_ID_HEADER] != str(envelope.message_id)
    assert sent.message_id == sent.headers[MESSAGE_ID_HEADER]
    assert sent.correlation_id == str(envelope.correlation_id)
    assert sent.timestamp is not None


async def test_bound_event_publishes(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    Awaiting a factory-bound callable produced by bind should construct the payload and publish it.

    Given: A connected publisher and a callable bound with a factory
    When: The callable is awaited with factory arguments
    Then: The constructed payload should be serialized and published
    """
    # Arrange
    order_placed = publisher.bind(_order_placed, factory=_OrderPlaced, config=_config)

    # Act
    async with publisher:
        await order_placed(order_id=7)

    # Assert
    call = channel.basic_publish.await_args
    assert call.args[0] == _encode(_OrderPlaced(7))
    assert call.kwargs["exchange"] == "events"
    assert call.kwargs["routing_key"] == "order.placed"


async def test_failed_channel_allocation_closes_connection(
    publisher: RabbitMQPublisher, connection: AsyncMock
):
    """
    A failure while entering the context should close the connection it opened.

    Given: A publisher whose connection fails to allocate a channel
    When: The context is entered
    Then: The error propagates, the connection is closed, and emit reports not connected
    """
    # Arrange
    connection.channel.side_effect = RuntimeError("no channel")

    # Act
    with pytest.raises(RuntimeError, match="no channel"):
        async with publisher:
            pass

    # Assert
    connection.close.assert_awaited_once()
    with pytest.raises(NotConnectedError):
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))


def _logging_hooks(log: list[str]) -> Hooks[Any, Any]:
    """Return publish hooks logging what they see."""

    def before(event: Any, payload: Any) -> None:
        log.append(f"before {type(payload).__name__}")

    def success(event: Any, payload: Any, reply: Any) -> None:
        log.append(f"success {reply!r}")

    def failure(event: Any, payload: Any, error: Exception) -> None:
        log.append(f"failure {type(error).__name__}")

    return Hooks(before=(before,), success=(success,), failure=(failure,))


async def test_emit_runs_publish_hooks_around_publish(
    publisher: RabbitMQPublisher, channel: AsyncMock
):
    """
    ``emit`` should run the hooks passed to it around the publish.

    Given: A connected publisher and a pub/sub event
    When: emit is awaited with hooks
    Then: before runs, then the publish, then success with a reply of None
    """
    # Arrange
    log: list[str] = []

    def record_publish(*_: object, **__: object) -> None:
        log.append("publish")

    channel.basic_publish.side_effect = record_publish

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(1), hooks=_logging_hooks(log))

    # Assert
    assert log == ["before _OrderPlaced", "publish", "success None"]


async def test_failed_publish_reaches_failure_hook(publisher: RabbitMQPublisher):
    """
    A publish that fails should reach the failure hook, and still raise.

    Given: A disconnected publisher and a pub/sub event
    When: emit is awaited with hooks
    Then: The failure hook sees NotConnectedError, which propagates
    """
    # Arrange
    log: list[str] = []

    # Act
    with pytest.raises(NotConnectedError):
        await publisher.emit(_order_placed, _config, _OrderPlaced(1), hooks=_logging_hooks(log))

    # Assert
    assert log == ["before _OrderPlaced", "failure NotConnectedError"]


async def test_publish_hooks_see_sent_envelope(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    Publish hooks should run inside the message's envelope, so they see the ids it carries.

    Given: A connected publisher, inside a caller's envelope scope
    When: An event is emitted with a before hook recording the current envelope
    Then: The hook's envelope is the message's: a child of the caller's, whose message id
        and correlation id are the ones stamped on the message
    """
    # Arrange
    seen: list[Envelope | None] = []

    def record(event: Any, payload: Any) -> None:
        seen.append(Envelope.current())

    # Act
    async with publisher:
        with Envelope.scope() as caller:
            await publisher.emit(
                _order_placed, _config, _OrderPlaced(1), hooks=Hooks(before=(record,))
            )

    # Assert
    (envelope,) = seen
    assert envelope is not None
    assert envelope.causation_id == caller.message_id
    properties = channel.basic_publish.await_args.kwargs["properties"]
    assert properties.message_id == str(envelope.message_id)
    assert properties.correlation_id == str(envelope.correlation_id)


async def test_publish_middleware_wraps_hooks_inside_envelope(
    connect_mock: AsyncMock, channel: AsyncMock
):
    """
    Publish middleware should wrap the publish and its hooks, inside the message's envelope.

    Given: A publisher with async publish middleware recording the current envelope
    When: An event is emitted with a before hook
    Then: The middleware runs outside the hook, and sees the envelope stamped on the message
    """
    # Arrange
    log: list[str] = []
    seen: list[Envelope | None] = []

    @asynccontextmanager
    async def record(call: PublishCall) -> AsyncGenerator[None, None]:
        log.append("mw in")
        seen.append(Envelope.current())
        yield
        log.append("mw out")

    def before(event: Any, payload: Any) -> None:
        log.append("hook")

    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, middleware=Middleware(publish=(record,))
    )

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(1), hooks=Hooks(before=(before,)))

    # Assert
    assert log == ["mw in", "hook", "mw out"]
    (envelope,) = seen
    assert envelope is not None
    properties = channel.basic_publish.await_args.kwargs["properties"]
    assert properties.message_id == str(envelope.message_id)


async def test_publish_middleware_without_hooks(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Publish middleware should wrap a publish that has no hooks.

    Given: A publisher with async publish middleware
    When: An event is emitted without hooks
    Then: The middleware runs around the publish
    """
    # Arrange
    log: list[str] = []

    @asynccontextmanager
    async def record(call: PublishCall) -> AsyncGenerator[None, None]:
        log.append("mw in")
        yield
        log.append("mw out")

    def record_publish(*_: object, **__: object) -> None:
        log.append("publish")

    channel.basic_publish.side_effect = record_publish
    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, middleware=Middleware(publish=(record,))
    )

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))

    # Assert
    assert log == ["mw in", "publish", "mw out"]


@asynccontextmanager
async def _trace(call: PublishCall) -> AsyncGenerator[None, None]:
    yield


class _NotAManager:
    """A middleware class whose instances aren't context managers."""

    def __init__(self, call: Any) -> None: ...


def test_publisher_refuses_non_manager_middleware():
    """
    The publisher should refuse a middleware class that isn't a context manager when built.

    Given: Publish middleware that's a plain class
    When: A RabbitMQPublisher is built with it
    Then: A TypeError says the publisher can't enter it
    """
    # Act / Assert
    with pytest.raises(TypeError, match="RabbitMQPublisher can't enter"):
        RabbitMQPublisher(
            _URL, serializer=_encode, middleware=Middleware(publish=(cast(Any, _NotAManager),))
        )


def test_publisher_accepts_shared_middleware():
    """
    The publisher should accept a Middleware shared with a consumer, checking only its half.

    Given: Middleware with a valid publish middleware, and a handle middleware class that
        isn't a context manager
    When: A RabbitMQPublisher is built with it
    Then: It's accepted, since the publisher never runs the handle half
    """
    # Arrange
    middleware = Middleware(publish=(_trace,), handle=(cast(Any, _NotAManager),))

    # Act
    publisher = RabbitMQPublisher(_URL, serializer=_encode, middleware=middleware)

    # Assert
    assert isinstance(publisher, RabbitMQPublisher)


async def test_publish_middleware_sees_config(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Publish middleware should see the emit's routing config.

    Given: A publisher with publish middleware recording call.config
    When: An event is emitted with a RabbitMQConfig
    Then: The middleware sees that config, exchange and routing key included
    """
    # Arrange
    seen: list[RabbitMQConfig] = []

    @asynccontextmanager
    async def record(call: PublishCall) -> AsyncGenerator[None, None]:
        seen.append(call.config)
        yield

    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, middleware=Middleware(publish=(record,))
    )

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))

    # Assert
    assert seen == [_config]
    assert (seen[0].exchange, seen[0].routing_key) == ("events", "order.placed")


async def test_emit_sends_given_envelope(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` given an envelope should send its ids and timestamp unchanged.

    Given: A connected publisher inside an envelope scope, and a held envelope from an earlier
           message
    When: emit is awaited with the held envelope
    Then: The message carries the held envelope's ids and timestamp as x- headers and native
          fields, not a child of the active envelope
    """
    # Arrange
    held = Envelope().child()

    # Act
    with Envelope.scope():
        async with publisher:
            await publisher.emit(_order_placed, _config, _OrderPlaced(7), envelope=held)

    # Assert
    sent = channel.basic_publish.await_args.kwargs["properties"]
    assert sent.headers[MESSAGE_ID_HEADER] == str(held.message_id)
    assert sent.headers[CORRELATION_ID_HEADER] == str(held.correlation_id)
    assert sent.headers[CAUSATION_ID_HEADER] == str(held.causation_id)
    assert sent.message_id == str(held.message_id)
    assert sent.correlation_id == str(held.correlation_id)
    assert sent.timestamp == held.timestamp


async def test_republishing_keeps_message_id(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    Emitting twice with one envelope should send the same message id both times.

    Given: A connected publisher and a held envelope
    When: The same event is emitted twice with that envelope
    Then: Both messages carry the held envelope's message id
    """
    # Arrange
    held = Envelope()

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(7), envelope=held)
        await publisher.emit(_order_placed, _config, _OrderPlaced(7), envelope=held)

    # Assert
    sent = [call.kwargs["properties"].message_id for call in channel.basic_publish.await_args_list]
    assert sent == [str(held.message_id), str(held.message_id)]


async def test_middleware_and_hooks_see_given_envelope(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Publish middleware and hooks should see the envelope given to emit.

    Given: A publisher with publish middleware, and a before hook, each recording the current
           envelope
    When: An event is emitted with a held envelope
    Then: Both see the held envelope
    """
    # Arrange
    held = Envelope()
    seen: list[Envelope | None] = []

    @asynccontextmanager
    async def record(call: PublishCall) -> AsyncGenerator[None, None]:
        seen.append(Envelope.current())
        yield

    def before(event: Any, payload: Any) -> None:
        seen.append(Envelope.current())

    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, middleware=Middleware(publish=(record,))
    )

    # Act
    async with publisher:
        await publisher.emit(
            _order_placed,
            _config,
            _OrderPlaced(1),
            hooks=Hooks(before=(before,)),
            envelope=held,
        )

    # Assert
    assert seen == [held, held]


@asynccontextmanager
async def _tag_tenant(call: PublishCall) -> AsyncGenerator[None, None]:
    call.headers["x-tenant"] = "acme"
    yield


async def test_middleware_headers_sent(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Headers publish middleware adds should be sent with the config's and the envelope's.

    Given: A publisher whose middleware adds a tenant header, and a config carrying a header
    When: An event is emitted with the config, then without middleware
    Then: The first message carries all three kinds of header; the second the envelope's alone
    """
    # Arrange
    config = RabbitMQConfig(
        "events", "order.placed", properties=Basic.Properties(headers={"custom": "v"})
    )
    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, middleware=Middleware(publish=(_tag_tenant,))
    )
    plain = RabbitMQPublisher(_URL, serializer=_encode)

    # Act
    async with publisher:
        await publisher.emit(_order_placed, config, _OrderPlaced(1))
    async with plain:
        await plain.emit(_order_placed, _config, _OrderPlaced(2))

    # Assert
    tagged, untagged = (c.kwargs["properties"] for c in channel.basic_publish.await_args_list)
    envelope = Envelope.from_headers(tagged.headers)
    assert tagged.headers == {"custom": "v", "x-tenant": "acme"} | envelope.to_headers()
    assert untagged.headers == Envelope.from_headers(untagged.headers).to_headers()


async def test_emit_given_headers(connect_mock: AsyncMock, channel: AsyncMock):
    """
    ``emit`` given headers should send them, and middleware should see and add to them.

    Given: A publisher whose middleware records the headers it sees, then adds a tenant
    When: An event is emitted with a trace header
    Then: The middleware saw the trace header, and the message carries both
    """
    # Arrange
    seen: list[dict[str, str]] = []

    @asynccontextmanager
    async def record(call: PublishCall) -> AsyncGenerator[None, None]:
        seen.append(dict(call.headers))
        yield

    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, middleware=Middleware(publish=(record, _tag_tenant))
    )

    # Act
    async with publisher:
        await publisher.emit(
            _order_placed, _config, _OrderPlaced(1), headers={"traceparent": "00-trace"}
        )

    # Assert
    sent = channel.basic_publish.await_args.kwargs["properties"]
    assert seen == [{"traceparent": "00-trace"}]
    assert (sent.headers["traceparent"], sent.headers["x-tenant"]) == ("00-trace", "acme")


@pytest.mark.parametrize("name", [MESSAGE_ID_HEADER, "x-attempts", "X-Failure"])
async def test_reserved_header_refused(connect_mock: AsyncMock, channel: AsyncMock, name: str):
    """
    A reserved header should raise ValueError, from middleware or given to emit.

    Given: A publisher whose middleware sets a reserved header, and one without middleware
    When: Each emits, the second given the reserved header
    Then: Both raise ValueError naming it, and nothing is published
    """

    # Arrange
    @asynccontextmanager
    async def forge(call: PublishCall) -> AsyncGenerator[None, None]:
        call.headers[name] = "forged"
        yield

    publisher = RabbitMQPublisher(_URL, serializer=_encode, middleware=Middleware(publish=(forge,)))
    plain = RabbitMQPublisher(_URL, serializer=_encode)
    message = f"'{name}' is a header the bus sets itself"

    # Act / Assert
    async with publisher, plain:
        with pytest.raises(ValueError, match=message):
            await publisher.emit(_order_placed, _config, _OrderPlaced(1))
        with pytest.raises(ValueError, match=message):
            await plain.emit(_order_placed, _config, _OrderPlaced(1), headers={name: "forged"})
    channel.basic_publish.assert_not_awaited()


def test_topology_needs_named_queue(connect_mock: AsyncMock):
    """
    A publisher should refuse a topology config without a named queue.

    Given: A consume config binding a private queue to an exchange
    When: A publisher is built with it as topology
    Then: A ValueError should be raised
    """
    with pytest.raises(ValueError, match="named queue"):
        RabbitMQPublisher(
            _URL, serializer=_encode, topology=[RabbitMQConsumeConfig(exchange="events")]
        )


async def test_entering_declares_topology(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Entering the context should declare the topology as a consumer would.

    Given: A publisher whose topology is a queue with a failure policy bound to a topic
           exchange, and a plain queue
    When: The context is entered
    Then: Each queue, its retry and dead-letter queues, the exchange and its binding are
          declared before the context body runs
    """
    # Arrange
    jobs = RabbitMQConsumeConfig(
        "jobs",
        exchange="work",
        binding_key="job.*",
        exchange_type="topic",
        exchange_durable=True,
        on_failure=RabbitMQFailurePolicy(retry_delays=(5,)),
    )
    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, topology=[jobs, RabbitMQConsumeConfig("audit")]
    )

    # Act
    async with publisher:
        # Assert
        assert channel.queue_declare.await_args_list == [
            call("jobs", durable=True, exclusive=False, auto_delete=False, arguments=None),
            call(
                "jobs.retry.5000",
                durable=True,
                arguments={
                    "x-message-ttl": 5000,
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": "jobs",
                },
            ),
            call("jobs.dead-letter", durable=True),
            call("audit", durable=True, exclusive=False, auto_delete=False, arguments=None),
        ]
        channel.exchange_declare.assert_awaited_once_with(
            "work", exchange_type="topic", durable=True
        )
        channel.queue_bind.assert_awaited_once_with("jobs", "work", routing_key="job.*")


async def test_failed_topology_declaration_disconnects(
    connect_mock: AsyncMock, channel: AsyncMock, connection: AsyncMock
):
    """
    A topology declaration the broker refuses should leave the publisher disconnected.

    Given: A publisher with topology, whose queue declaration fails
    When: The context is entered
    Then: The error propagates, the connection is closed, and emit reports not connected
    """
    # Arrange
    channel.queue_declare.side_effect = RuntimeError("inequivalent arg 'durable'")
    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, topology=[RabbitMQConsumeConfig("jobs")]
    )

    # Act
    with pytest.raises(RuntimeError, match="inequivalent"):
        async with publisher:
            pass

    # Assert
    connection.close.assert_awaited_once()
    with pytest.raises(NotConnectedError):
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))


async def test_no_topology_declares_nothing(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    A publisher without topology should declare no queues on entering.

    Given: A publisher built without topology
    When: The context is entered
    Then: No queue, exchange or binding is declared
    """
    # Act
    async with publisher:
        pass

    # Assert
    channel.queue_declare.assert_not_awaited()
    channel.exchange_declare.assert_not_awaited()
    channel.queue_bind.assert_not_awaited()


def test_config_mandatory_defaults_off():
    """
    RabbitMQConfig should not require routing unless asked to.

    Given: Configs with and without mandatory
    When: They're created
    Then: mandatory is stored, defaulting to False
    """
    assert RabbitMQConfig("events", "order.placed").mandatory is False
    assert RabbitMQConfig("events", "order.placed", mandatory=True).mandatory is True


async def test_channel_opened_with_confirms(publisher: RabbitMQPublisher, connection: AsyncMock):
    """
    The publisher should open its channel with publisher confirms, raising on returns.

    Given: A publisher
    When: The context is entered
    Then: The channel is opened with publisher_confirms and on_return_raises set
    """
    # Act
    async with publisher:
        pass

    # Assert
    connection.channel.assert_awaited_once_with(publisher_confirms=True, on_return_raises=True)


async def test_emit_passes_mandatory(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should publish with its config's mandatory flag.

    Given: A config requiring routing
    When: An event is emitted with it
    Then: The message is published with mandatory=True
    """
    # Arrange
    config = RabbitMQConfig("events", "order.placed", mandatory=True)

    # Act
    async with publisher:
        await publisher.emit(_order_placed, config, _OrderPlaced(1))

    # Assert
    assert channel.basic_publish.await_args.kwargs["mandatory"] is True


async def test_returned_message_raises_unroutable(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    A mandatory message the broker returns should raise UnroutableError.

    Given: A channel whose publish raises aiormq's PublishError, as a returned message does
    When: An event is emitted with a mandatory config
    Then: UnroutableError is raised, naming the exchange and routing key, from the PublishError
    """
    # Arrange
    returned = PublishError.__new__(PublishError)
    channel.basic_publish.side_effect = returned
    config = RabbitMQConfig("events", "order.lost", mandatory=True)

    # Act
    async with publisher:
        with pytest.raises(UnroutableError, match=r"'events'.*'order\.lost'") as raised:
            await publisher.emit(_order_placed, config, _OrderPlaced(1))

    # Assert
    assert raised.value.__cause__ is returned


async def test_emit_waits_for_confirm(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should not return until the broker's confirm arrives.

    Given: A channel whose publish resolves only once the broker confirms
    When: An event is emitted
    Then: emit is still waiting before the confirm, and returns after it
    """
    # Arrange
    confirmed = asyncio.Event()

    async def publish(*args: Any, **kwargs: Any) -> None:
        await confirmed.wait()

    channel.basic_publish.side_effect = publish

    async with publisher:
        emitting = asyncio.ensure_future(publisher.emit(_order_placed, _config, _OrderPlaced(1)))
        await asyncio.sleep(0.01)
        waiting = not emitting.done()

        # Act
        confirmed.set()
        await emitting

    # Assert
    assert waiting
    assert emitting.done()


_FAST = RabbitMQReconnect(initial_delay=0.001, max_delay=0.001)


def _new_channels(connection: AsyncMock) -> list[AsyncMock]:
    """Make the connection open a fresh mock channel each time, returning those opened."""
    opened: list[AsyncMock] = []

    async def open_channel(**_: Any) -> AsyncMock:
        await asyncio.sleep(0)
        channel = AsyncMock()
        channel.closing = asyncio.get_running_loop().create_future()
        opened.append(channel)
        return channel

    connection.channel.side_effect = open_channel
    return opened


def test_reconnect_refuses_inconsistent_delays():
    """
    RabbitMQReconnect should refuse delays that can't make a backoff.

    Given: A zero initial delay, a cap below the initial delay, and a shrinking backoff
    When: Each policy is created
    Then: A ValueError should be raised
    """
    for kwargs in ({"initial_delay": 0}, {"initial_delay": 2, "max_delay": 1}, {"backoff": 0.5}):
        with pytest.raises(ValueError, match="initial_delay"):
            RabbitMQReconnect(**kwargs)


def test_reconnect_delays_grow_to_cap():
    """
    RabbitMQReconnect's waits should grow by its backoff, up to its cap.

    Given: A policy starting at 1 second, doubling, capped at 5
    When: Its first five delays are taken
    Then: They're 1, 2, 4, 5, 5
    """
    delays = RabbitMQReconnect(initial_delay=1, max_delay=5).delays()

    assert [next(delays) for _ in range(5)] == [1, 2, 4, 5, 5]


async def test_publisher_reports_state(publisher: RabbitMQPublisher):
    """
    The publisher should report its connection state.

    Given: A publisher
    When: It's created, entered and exited
    Then: It's closed, connected, then closed
    """
    # Act / Assert
    assert publisher.state is ConnectionState.CLOSED
    async with publisher:
        assert publisher.state is ConnectionState.CONNECTED
    assert publisher.state is ConnectionState.CLOSED


async def test_wait_for_state_returns_at_once_when_there(publisher: RabbitMQPublisher):
    """
    Waiting for the state a publisher is already in should return at once.

    Given: A publisher that hasn't been entered
    When: Its caller waits for it to be closed
    Then: The wait returns without the loop running anything else
    """
    # Act / Assert
    async with asyncio.timeout(0):
        await publisher.wait_for_state(ConnectionState.CLOSED)


async def test_wait_for_state_wakes_when_reached(publisher: RabbitMQPublisher):
    """
    Waiting for a state should return once the publisher reaches it, even after a waiter gave up.

    Given: One waiter for the connected state that times out, and another still waiting
    When: The publisher is entered
    Then: The remaining waiter returns
    """
    # Arrange
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0):
            await publisher.wait_for_state(ConnectionState.CONNECTED)
    waiting = asyncio.create_task(publisher.wait_for_state(ConnectionState.CONNECTED))
    await asyncio.sleep(0)

    # Act
    async with publisher:
        async with asyncio.timeout(1):
            await waiting

    # Assert
    assert waiting.done()


async def test_publisher_reconnects_after_channel_loss(
    connect_mock: AsyncMock, connection: AsyncMock
):
    """
    A publisher whose channel closes unexpectedly should reconnect and publish again.

    Given: A connected publisher with topology
    When: Its channel closes with an error
    Then: It reports reconnecting and emit raises NotConnectedError at once; then it's
          connected again, has declared its topology on the new channel, and publishes there
    """
    # Arrange
    channels = _new_channels(connection)
    publisher = RabbitMQPublisher(
        _URL,
        serializer=_encode,
        topology=[RabbitMQConsumeConfig("jobs")],
        reconnect=RabbitMQReconnect(0.05, 0.05),
    )

    async with publisher:
        # Act
        channels[0].closing.set_exception(ConnectionError("broker gone"))
        await asyncio.sleep(0)

        # Assert
        assert publisher.state is ConnectionState.RECONNECTING
        with pytest.raises(NotConnectedError, match="reconnecting"):
            await publisher.emit(_order_placed, _config, _OrderPlaced(1))
        async with asyncio.timeout(1):
            await publisher.wait_for_state(ConnectionState.CONNECTED)
        await publisher.emit(_order_placed, _config, _OrderPlaced(2))

    assert connect_mock.await_count == 2
    channels[1].queue_declare.assert_awaited()
    channels[0].basic_publish.assert_not_awaited()
    channels[1].basic_publish.assert_awaited_once()
    assert connection.close.await_count == 2  # the stale connection, then on exit


async def test_failed_reconnect_attempt_retried(
    connect_mock: AsyncMock, connection: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    A failed reconnect attempt should be logged and retried.

    Given: A connected publisher, and a broker refusing the first reconnect attempt
    When: Its channel closes unexpectedly
    Then: The failed attempt is logged, and the next one reconnects
    """
    # Arrange
    channels = _new_channels(connection)
    publisher = RabbitMQPublisher(_URL, serializer=_encode, reconnect=_FAST)

    async with publisher:
        connect_mock.side_effect = [OSError("refused"), connection]

        # Act
        channels[0].closing.set_exception(ConnectionError("gone"))
        async with asyncio.timeout(1):
            await publisher.wait_for_state(ConnectionState.RECONNECTING)
            await publisher.wait_for_state(ConnectionState.CONNECTED)

        # Assert
        assert len(channels) == 2

    assert "publisher failed to reconnect" in caplog.text
    assert connect_mock.await_count == 3


async def test_exit_while_reconnecting_stops(connect_mock: AsyncMock, connection: AsyncMock):
    """
    Exiting while reconnecting should stop trying and leave the publisher closed.

    Given: A connected publisher, and a broker that never answers a reconnect
    When: Its channel closes, then its context exits
    Then: The exit returns promptly, and the publisher is closed
    """
    # Arrange
    channels = _new_channels(connection)
    publisher = RabbitMQPublisher(_URL, serializer=_encode, reconnect=_FAST)

    attempted = asyncio.Event()

    async def never(*_: Any) -> None:
        attempted.set()
        await asyncio.Event().wait()

    await publisher.__aenter__()
    connect_mock.side_effect = never
    channels[0].closing.set_exception(ConnectionError("gone"))
    async with asyncio.timeout(1):
        await attempted.wait()

    # Act
    async with asyncio.timeout(1):
        await publisher.__aexit__(None, None, None)

    # Assert
    assert publisher.state is ConnectionState.CLOSED


async def test_lost_channel_without_reconnect_closes(
    connect_mock: AsyncMock, channel: AsyncMock, connection: AsyncMock
):
    """
    With reconnecting turned off, a lost channel should leave the publisher closed.

    Given: A connected publisher with reconnect=None
    When: Its channel closes unexpectedly
    Then: It's closed, emit raises NotConnectedError, the old connection is closed, and it
          never reconnects
    """
    # Arrange
    publisher = RabbitMQPublisher(_URL, serializer=_encode, reconnect=None)

    async with publisher:
        # Act
        closed = asyncio.Event()
        connection.close.side_effect = lambda: closed.set()
        channel.closing.set_exception(ConnectionError("gone"))
        async with asyncio.timeout(1):
            await closed.wait()

        # Assert
        assert publisher.state is ConnectionState.CLOSED
        with pytest.raises(NotConnectedError, match="not connected"):
            await publisher.emit(_order_placed, _config, _OrderPlaced(1))

    connect_mock.assert_awaited_once()


async def test_closing_on_purpose_does_not_reconnect(
    publisher: RabbitMQPublisher, connect_mock: AsyncMock, channel: AsyncMock
):
    """
    Closing the publisher on purpose shouldn't start reconnecting.

    Given: A connected publisher
    When: Its context exits
    Then: It stops watching the channel, stays closed, and never reconnects
    """
    # Act
    async with publisher:
        pass
    await asyncio.sleep(0.01)

    # Assert
    assert channel.closing.cancelled()
    assert publisher.state is ConnectionState.CLOSED
    connect_mock.assert_awaited_once()


def test_publisher_requires_a_serializer():
    """
    A publisher built without a serializer should be refused.

    Given: no serializer
    When: a publisher is constructed
    Then: it's a type error, and raises TypeError
    """
    # Act / Assert
    with pytest.raises(TypeError, match="serializer"):
        RabbitMQPublisher(_URL)  # pyright: ignore[reportCallIssue]


class _Ids:
    """An id source handing out UUID(int=1), UUID(int=2), and so on, in order."""

    def __init__(self) -> None:
        self.made: list[UUID] = []

    def __call__(self) -> UUID:
        made = UUID(int=len(self.made) + 1)
        self.made.append(made)
        return made


async def test_emit_draws_envelope_ids_from_publisher_source(
    connect_mock: AsyncMock, channel: AsyncMock
):
    """
    A publisher should draw a message's envelope ids from its id source.

    Given: A publisher with a counting id source, outside any envelope
    When: A payload is emitted without a given envelope
    Then: The message carries the source's first two ids as its message and correlation ids
    """
    # Arrange
    publisher = RabbitMQPublisher(_URL, serializer=_encode, envelope=EnvelopeConfig(new_id=_Ids()))

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, _OrderPlaced(7))

    # Assert
    sent = channel.basic_publish.await_args.kwargs["properties"]
    assert (sent.message_id, sent.correlation_id) == (str(UUID(int=1)), str(UUID(int=2)))
