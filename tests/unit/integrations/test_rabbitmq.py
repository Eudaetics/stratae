"""
Unit tests for the RabbitMQ publish adapter.

This test suite verifies the following behaviors:

RabbitMQConfig:
- The exchange and routing_key are stored on initialization.
- The exchange_type is stored on initialization, defaulting to None.

RabbitMQPublisher:
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
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from pamqp.commands import Basic
from pytest_mock import MockerFixture

from stratae.events import (
    CAUSATION_ID_HEADER,
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    Envelope,
    Hooks,
    NoPayload,
    PubSub,
    Request,
)
from stratae.events.exceptions import AlreadyConnectedError, NotConnectedError
from stratae.integrations.rabbitmq import RabbitMQConfig, RabbitMQPublisher
from stratae.serde import encode, serialize

_URL = "amqp://guest:guest@localhost/"


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


@encode.register
def encode_order_placed(obj: _OrderPlaced) -> dict[str, int]:
    """Define the encoder for _OrderPlaced."""
    return {"order_id": obj.order_id}


_order_placed = PubSub(_OrderPlaced)
_config = RabbitMQConfig("events", "order.placed")


@pytest.fixture
def channel() -> AsyncMock:
    """Return a mock AMQP channel."""
    return AsyncMock()


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
    return RabbitMQPublisher(_URL)


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
    assert call.args[0] == serialize(payload)
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
    ``emit`` should use the supplied serializer instead of the default serialize.

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
    assert call.args[0] == serialize(_OrderPlaced(7))
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
