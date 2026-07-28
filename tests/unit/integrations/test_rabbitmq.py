"""
Unit tests for the RabbitMQ publish adapter.

This test suite verifies the following behaviors:

RabbitMQConfig:
- The exchange and routing_key are stored on initialization.
- The exchange_type is stored on initialization, defaulting to None.

RabbitMQPublisher:
- Entering the context opens the connection and allocates a channel.
- Exiting the context closes the connection.
- emit raises NotConnectedError before the context is entered.
- emit publishes the pack-serialized payload to the configured exchange
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
"""

from unittest.mock import AsyncMock

import pytest
from pamqp.commands import Basic
from pytest_mock import MockerFixture

from stratae.events import (
    CAUSATION_ID_HEADER,
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    Envelope,
    Event,
    PubSub,
)
from stratae.events.exceptions import NotConnectedError
from stratae.integrations.rabbitmq import RabbitMQConfig, RabbitMQPublisher
from stratae.serde import pack

_URL = "amqp://guest:guest@localhost/"


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

    def to_dict(self) -> dict[str, int]:
        return {"order_id": self.order_id}


_order_placed = Event(PubSub, _OrderPlaced)
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


async def test_emit_without_connection_raises(publisher: RabbitMQPublisher):
    """
    ``emit`` should raise NotConnectedError before the context is entered.

    Given: A publisher whose context has not been entered
    When: emit is awaited
    Then: A NotConnectedError should be raised
    """
    with pytest.raises(NotConnectedError):
        await publisher.emit(_order_placed, _config, _OrderPlaced(1))


async def test_emit_publishes_packed_payload(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should publish the pack-serialized payload to the configured target.

    Given: A connected publisher and a payload
    When: emit is awaited without a serializer
    Then: The payload should be packed and published with the config's
          exchange and routing key
    """
    # Arrange
    payload = _OrderPlaced(7)

    # Act
    async with publisher:
        await publisher.emit(_order_placed, _config, payload)

    # Assert
    call = channel.basic_publish.await_args
    assert call.args[0] == pack(payload)
    assert call.kwargs["exchange"] == "events"
    assert call.kwargs["routing_key"] == "order.placed"


async def test_emit_uses_custom_serializer(publisher: RabbitMQPublisher, channel: AsyncMock):
    """
    ``emit`` should use the supplied serializer instead of the default pack.

    Given: A connected publisher and a custom serializer
    When: emit is awaited with the serializer
    Then: The serializer's bytes should be published
    """

    # Arrange
    def to_bytes(_: _OrderPlaced) -> bytes:
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
    Then: The constructed payload should be packed and published
    """
    # Arrange
    order_placed = publisher.bind(_order_placed, factory=_OrderPlaced, config=_config)

    # Act
    async with publisher:
        await order_placed(order_id=7)

    # Assert
    call = channel.basic_publish.await_args
    assert call.args[0] == pack(_OrderPlaced(7))
    assert call.kwargs["exchange"] == "events"
    assert call.kwargs["routing_key"] == "order.placed"
