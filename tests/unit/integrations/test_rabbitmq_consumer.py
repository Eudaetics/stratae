"""
Unit tests for the RabbitMQ consume adapter.

This test suite verifies the following behaviors:

RabbitMQConsumeConfig:
- The queue is stored on initialization.
- Exchange, binding key, and exchange type are stored on initialization.
- Omitting both the queue and the exchange raises ValueError.

RabbitMQConsumer:
- Entering the context opens the connection and starts consuming for
  registrations made beforehand.
- Registrations made while connected start consuming immediately.
- Exiting the context closes the connection.
- Exiting without a connection closes nothing.
- Entering the context while already connected raises AlreadyConnectedError.
- A failed close on exit still leaves the consumer disconnected.
- handle registers directly or as a decorator, returning the Handler.
- handle raises TypeError for an event that isn't PubSub.
- Delivered messages are decoded with the default deserialize and the
  message is acked after the handler returns.
- Async handlers are awaited.
- The consumer's deserializer is called as deserializer(body, type=schema).
- A per-registration deserializer overrides the consumer's.
- A failing handler nacks the message without requeue.
- A failing deserialization nacks the message without requeue.
- A message without a delivery tag is neither acked nor nacked, whether its
  handler returns or raises.
- remove cancels the live AMQP consumer.
- Removing a handler twice cancels its consumer once.
- A consumer started for a handler removed mid-start is cancelled.
- Exiting the context cancels start tasks that have not yet run.
- A start task failure is logged.
- Queue-only configs declare their queue durable and consume it without
  exchange topology.
- An exchange config declares the exchange, declares and binds a private
  server-named queue, and consumes it.
- A named queue with an exchange is declared durable and bound.
- The consumer sets channel QoS when a prefetch count is given, and sets
  none otherwise.
- An iterable binding key binds the queue once per key.
- Explicit declaration flags and arguments override the inferred queue
  defaults.
- Handlers run inside the delivered message's envelope scope.
- Messages without envelope headers still scope a fresh envelope.
- Native AMQP property fields rebuild the envelope when x- headers are
  absent.
- Malformed envelope headers log a warning and scope a fresh envelope
  without blocking the handler.
- A partial identifying pair is logged before the missing id is minted.
- A schema-less event's handler is called with no arguments.
- Exiting the context cancels each live consumer before closing the
  connection.
- Exiting the context waits for running handlers to finish and ack before
  closing.
- Exiting stops waiting at the drain timeout, logging the handlers still
  running.
- A failed consumer cancel on exit is logged and the connection still closes.
- A handler exiting its own consumer doesn't wait on itself.
- Hooks passed to handle run around each delivery's handler, before the ack.
- A before hook that raises nacks the message without requeue.
- A success hook that raises is logged, and the handled message is still acked.
- Handle middleware runs around each handler and its hooks, before the ack.
- Handle middleware sees the event as call.event, and the consume config as call.config.
- Handle middleware that times out a handler nacks the message, and one that swallows the
  timeout does too, logging a MiddlewareError.
- Handle middleware raising on entry nacks the message without running the handler.
- A cancellation handle middleware tries to swallow still cancels the delivery, without a nack.
- A handle middleware class that isn't a context manager is refused when the consumer is
  built; publish middleware isn't checked, so one Middleware can be shared with a publisher.
- A schema-less event's hooks receive a NoPayload instance.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

from stratae.events import (
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    Envelope,
    HandleCall,
    Handler,
    Hooks,
    Middleware,
    NoPayload,
    PubSub,
    Request,
)
from stratae.events.exceptions import AlreadyConnectedError
from stratae.integrations.rabbitmq import RabbitMQConsumeConfig, RabbitMQConsumer
from stratae.serde import deserialize

_URL = "amqp://guest:guest@localhost/"


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


@deserialize.register(_OrderPlaced)
def _(value: dict[str, Any]) -> _OrderPlaced:
    return _OrderPlaced(**value)


_order_placed = PubSub(_OrderPlaced)
_config = RabbitMQConsumeConfig("orders")

_heartbeat = PubSub(name="heartbeat")


def _message(body: bytes, headers: dict[str, Any] | None = None) -> AsyncMock:
    """Return a mock DeliveredMessage carrying the body and headers."""
    message = AsyncMock()
    message.body = body
    message.delivery_tag = 11
    message.header.properties.headers = headers
    message.header.properties.message_id = None
    message.header.properties.correlation_id = None
    message.header.properties.timestamp = None
    return message


def _delivery_callback(channel: AsyncMock) -> Any:
    """Return the on-message callback the consumer registered on the channel."""
    return channel.basic_consume.await_args.args[1]


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
def consumer(connect_mock: AsyncMock) -> RabbitMQConsumer:
    """Return a RabbitMQConsumer wired to the mocked connection."""
    return RabbitMQConsumer(_URL)


def test_consume_config_stores_queue():
    """
    RabbitMQConsumeConfig should store its queue.

    Given: A queue name
    When: A RabbitMQConsumeConfig is created
    Then: The queue should be stored as an attribute
    """
    config = RabbitMQConsumeConfig("orders")

    assert config.queue == "orders"


def test_consume_config_stores_binding_fields():
    """
    RabbitMQConsumeConfig should store its exchange binding fields.

    Given: An exchange, a binding key, and an exchange type
    When: A RabbitMQConsumeConfig is created
    Then: All fields should be stored as attributes
    """
    config = RabbitMQConsumeConfig(
        "audit", exchange="events", binding_key="order.*", exchange_type="topic"
    )

    assert config.queue == "audit"
    assert config.exchange == "events"
    assert config.binding_keys == ("order.*",)
    assert config.exchange_type == "topic"


def test_consume_config_requires_queue_or_exchange():
    """
    RabbitMQConsumeConfig should reject a config with no consume target.

    Given: Neither a queue nor an exchange
    When: A RabbitMQConsumeConfig is created
    Then: A ValueError should be raised
    """
    with pytest.raises(ValueError, match="queue"):
        RabbitMQConsumeConfig()


async def test_context_starts_prior_registrations(
    consumer: RabbitMQConsumer, connect_mock: AsyncMock, channel: AsyncMock
):
    """
    Entering the context should connect and start consuming for registrations.

    Given: A consumer with a handler registered before connecting
    When: The async context is entered
    Then: connect should be awaited with the URL and the registration's
          queue consumed
    """
    # Arrange
    consumer.handle(_order_placed, lambda payload: None, config=_config)

    # Act
    async with consumer:
        # Assert
        connect_mock.assert_awaited_once_with(_URL)
        assert channel.basic_consume.await_args.args[0] == "orders"


async def test_handle_while_connected_starts_immediately(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    Registering while connected should start consuming without re-entering.

    Given: A consumer whose context has been entered
    When: A handler is registered
    Then: The registration's queue should be consumed immediately
    """
    async with consumer:
        # Act
        consumer.handle(_order_placed, lambda payload: None, config=_config)
        await asyncio.sleep(0)

        # Assert
        channel.basic_consume.assert_awaited_once()


async def test_context_closes_connection_on_exit(consumer: RabbitMQConsumer, connection: AsyncMock):
    """
    Exiting the context should close the connection.

    Given: A consumer whose context has been entered
    When: The context exits
    Then: The connection should be closed
    """
    # Act
    async with consumer:
        pass

    # Assert
    connection.close.assert_awaited_once()


async def test_exit_without_connection_is_noop(
    consumer: RabbitMQConsumer, connect_mock: AsyncMock, connection: AsyncMock
):
    """
    Exiting a consumer that never connected should close nothing.

    Given: A consumer whose context was never entered
    When: The context exits
    Then: No connection should be opened or closed
    """
    # Act
    await consumer.__aexit__(None, None, None)

    # Assert
    connect_mock.assert_not_awaited()
    connection.close.assert_not_awaited()


async def test_context_reentered_while_connected_raises(
    consumer: RabbitMQConsumer, connect_mock: AsyncMock
):
    """
    Entering the context again while connected should raise instead of orphaning the connection.

    Given: A consumer whose context has been entered
    When: The context is entered again
    Then: AlreadyConnectedError should be raised without opening a second connection
    """
    # Act & Assert
    async with consumer:
        with pytest.raises(AlreadyConnectedError):
            await consumer.__aenter__()

    connect_mock.assert_awaited_once()


async def test_failed_close_leaves_consumer_disconnected(
    consumer: RabbitMQConsumer, connect_mock: AsyncMock, connection: AsyncMock
):
    """
    A close that raises on exit should still leave the consumer disconnected.

    Given: A consumer whose connection's close raises
    When: The context exits
    Then: The error propagates, and the consumer can be entered again
    """
    # Arrange
    connection.close.side_effect = ConnectionError("broker gone")

    # Act
    with pytest.raises(ConnectionError):
        async with consumer:
            pass
    connection.close.side_effect = None
    async with consumer:
        pass

    # Assert
    assert connect_mock.await_count == 2


def test_handle_returns_handler_directly(consumer: RabbitMQConsumer):
    """
    ``handle`` should register a directly supplied function and return its Handler.

    Given: A consumer
    When: handle is called with a function
    Then: A Handler carrying the consume config should be returned
    """
    handler = consumer.handle(_order_placed, lambda payload: None, config=_config)

    assert isinstance(handler, Handler)
    assert handler.config is _config


def test_handle_as_decorator_returns_handler(consumer: RabbitMQConsumer):
    """
    ``handle`` without a function should act as a registering decorator.

    Given: A consumer
    When: handle is used as a decorator on a function
    Then: The decorated name should be the registered Handler
    """

    @consumer.handle(_order_placed, config=_config)
    def on_order(payload: _OrderPlaced) -> None: ...

    assert isinstance(on_order, Handler)


def test_handle_refuses_non_pubsub_event(consumer: RabbitMQConsumer):
    """
    ``handle`` should refuse an event that isn't PubSub, since a consumer can't reply.

    Given: A consumer, and a request event passed where a PubSub event is expected
    When: handle is called with the request event
    Then: A TypeError should be raised, and nothing is registered
    """
    # Arrange
    request_event = cast(Any, Request(_OrderPlaced, reply=_OrderPlaced))

    # Act / Assert
    with pytest.raises(TypeError, match="PubSub events only"):
        consumer.handle(request_event, lambda payload: None, config=_config)


async def test_message_decoded_and_acked(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    A delivered message should be decoded with deserialize and acked.

    Given: A connected consumer with a sync handler and a JSON message body
    When: The registered callback receives the message
    Then: The handler should receive the reconstructed payload and the
          message should be acked with its delivery tag
    """
    # Arrange
    received: list[_OrderPlaced] = []
    consumer.handle(_order_placed, received.append, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert received[0].order_id == 7
    message.channel.basic_ack.assert_awaited_once_with(11)
    message.channel.basic_nack.assert_not_awaited()


async def test_async_handler_awaited(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    An async handler should be awaited with the decoded payload.

    Given: A connected consumer with an async handler
    When: The registered callback receives a message
    Then: The handler should be awaited and the message acked
    """
    # Arrange
    received: list[_OrderPlaced] = []

    async def on_order(payload: _OrderPlaced) -> None:
        received.append(payload)

    consumer.handle(_order_placed, on_order, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert received[0].order_id == 7
    message.channel.basic_ack.assert_awaited_once_with(11)


async def test_signal_handler_called_with_no_arguments(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A schema-less event's handler should be called with no arguments.

    Given: A connected consumer with a zero-parameter handler registered to a schema-less event
    When: The registered callback receives a message
    Then: The handler should run and the message should be acked
    """
    # Arrange
    calls: list[bool] = []

    def on_heartbeat() -> None:
        calls.append(True)

    consumer.handle(_heartbeat, on_heartbeat, config=_config)
    message = _message(b"{}")

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert calls == [True]
    message.channel.basic_ack.assert_awaited_once_with(11)
    message.channel.basic_nack.assert_not_awaited()


async def test_constructor_deserializer_used(connect_mock: AsyncMock, channel: AsyncMock):
    """
    The consumer's deserializer should decode bodies against the event's schema.

    Given: A consumer constructed with a custom Deserializer
    When: The registered callback receives a message
    Then: The deserializer should be called with the body and the event's
          schema, and the handler should receive its result
    """
    # Arrange
    calls: list[tuple[bytes, type[Any]]] = []

    def decoder(data: bytes, /, *, type: type[Any]) -> Any:
        calls.append((data, type))
        return "decoded"

    consumer = RabbitMQConsumer(_URL, deserializer=decoder)
    received: list[Any] = []
    consumer.handle(_order_placed, received.append, config=_config)
    message = _message(b"body")

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert calls == [(b"body", _OrderPlaced)]
    assert received == ["decoded"]


async def test_registration_deserializer_overrides(connect_mock: AsyncMock, channel: AsyncMock):
    """
    A per-registration deserializer should override the consumer's.

    Given: A consumer with its own deserializer and a registration
           supplying another
    When: The registered callback receives a message
    Then: Only the registration's deserializer should decode the body
    """
    # Arrange
    adapter_calls: list[bytes] = []

    def adapter_decoder(data: bytes, /, *, type: type[Any]) -> Any:
        adapter_calls.append(data)
        return "adapter"

    def override(data: bytes, /, *, type: type[Any]) -> Any:
        return "override"

    consumer = RabbitMQConsumer(_URL, deserializer=adapter_decoder)
    received: list[Any] = []
    consumer.handle(_order_placed, received.append, config=_config, deserializer=override)
    message = _message(b"body")

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert received == ["override"]
    assert adapter_calls == []


async def test_failing_handler_nacks_without_requeue(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A raising handler should nack the message without requeue.

    Given: A connected consumer whose handler raises
    When: The registered callback receives a message
    Then: The message should be nacked without requeue and never acked
    """

    # Arrange
    def on_order(payload: _OrderPlaced) -> None:
        raise RuntimeError("boom")

    consumer.handle(_order_placed, on_order, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()


async def test_failing_deserialization_nacks_without_requeue(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A body the deserializer rejects should nack the message without requeue.

    Given: A connected consumer and a message body that is not valid JSON
    When: The registered callback receives the message
    Then: The message should be nacked without requeue and never acked
    """
    # Arrange
    received: list[_OrderPlaced] = []
    consumer.handle(_order_placed, received.append, config=_config)
    message = _message(b"not json")

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert received == []
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()


async def test_message_without_delivery_tag_not_acked(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A handled message without a delivery tag should not be acked.

    Given: A connected consumer with a sync handler and a message without a delivery tag
    When: The registered callback receives the message
    Then: The handler should receive the payload and the message should be neither
          acked nor nacked
    """
    # Arrange
    received: list[_OrderPlaced] = []
    consumer.handle(_order_placed, received.append, config=_config)
    message = _message(b'{"order_id": 7}')
    message.delivery_tag = None

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert received[0].order_id == 7
    message.channel.basic_ack.assert_not_awaited()
    message.channel.basic_nack.assert_not_awaited()


async def test_failed_message_without_delivery_tag_not_nacked(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A failed message without a delivery tag should not be nacked.

    Given: A connected consumer whose handler raises, and a message without a delivery tag
    When: The registered callback receives the message
    Then: The message should be neither acked nor nacked
    """

    # Arrange
    def on_order(payload: _OrderPlaced) -> None:
        raise RuntimeError("boom")

    consumer.handle(_order_placed, on_order, config=_config)
    message = _message(b'{"order_id": 7}')
    message.delivery_tag = None

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_nack.assert_not_awaited()
    message.channel.basic_ack.assert_not_awaited()


async def test_remove_cancels_live_consumer(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    ``remove`` should cancel the live AMQP consumer for the handler.

    Given: A connected consumer with a registered handler
    When: remove is awaited with the handler
    Then: The consumer tag returned by basic_consume should be cancelled
    """
    # Arrange
    handler = consumer.handle(_order_placed, lambda payload: None, config=_config)

    # Act
    async with consumer:
        await consumer.remove(handler)

        # Assert
        tag = channel.basic_consume.return_value.consumer_tag
        channel.basic_cancel.assert_awaited_once_with(tag)


async def test_remove_unknown_handler_is_noop(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    ``remove`` should ignore handlers that are no longer registered.

    Given: A connected consumer whose handler has already been removed
    When: remove is awaited with the same handler again
    Then: Only the first removal should cancel the consumer
    """
    # Arrange
    handler = consumer.handle(_order_placed, lambda payload: None, config=_config)

    # Act
    async with consumer:
        await consumer.remove(handler)
        await consumer.remove(handler)

        # Assert
        channel.basic_cancel.assert_awaited_once()


async def test_start_cancels_consumer_removed_mid_start(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A consumer started for an already-removed handler should be cancelled.

    Given: A connected consumer and a handler removed before its start
           task has run
    When: The start task completes
    Then: The consumer tag it obtained should be cancelled
    """
    async with consumer:
        # Arrange
        handler = consumer.handle(_order_placed, lambda payload: None, config=_config)
        await consumer.remove(handler)

        # Act
        await asyncio.sleep(0)

        # Assert
        tag = channel.basic_consume.return_value.consumer_tag
        channel.basic_cancel.assert_awaited_once_with(tag)


async def test_exit_cancels_pending_start(
    consumer: RabbitMQConsumer, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    Exiting the context should cancel start tasks that have not yet run.

    Given: A handler registered while connected
    When: The context exits before the start task runs
    Then: The task should be cancelled without consuming or logging an error
    """
    # Arrange
    async with consumer:
        consumer.handle(_order_placed, lambda payload: None, config=_config)

    # Act
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # Assert
    channel.basic_consume.assert_not_awaited()
    assert "failed to start AMQP consumer" not in caplog.text


async def test_failed_start_logs_error(
    consumer: RabbitMQConsumer, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    A start task that raises should log the failure and be discarded.

    Given: A connected consumer whose channel refuses basic_consume
    When: A handler is registered while connected
    Then: The start failure should be logged
    """
    # Arrange
    channel.basic_consume.side_effect = RuntimeError("boom")

    async with consumer:
        # Act
        consumer.handle(_order_placed, lambda payload: None, config=_config)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        # Assert
        assert "failed to start AMQP consumer" in caplog.text


async def test_queue_only_config_declares_durable_queue(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A queue-only config should declare its queue durable and consume it.

    Given: A consumer registered with a queue-only config
    When: The async context is entered
    Then: The queue should be declared durable, no exchange declared, and
          the queue consumed
    """
    # Arrange
    consumer.handle(_order_placed, lambda payload: None, config=_config)

    # Act
    async with consumer:
        # Assert
        channel.exchange_declare.assert_not_awaited()
        channel.queue_declare.assert_awaited_once_with(
            "orders", durable=True, exclusive=False, auto_delete=False, arguments=None
        )
        assert channel.basic_consume.await_args.args[0] == "orders"


async def test_exchange_config_declares_and_binds_private_queue(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    An exchange config should declare and bind a private server-named queue.

    Given: A consumer registered with an exchange config and no queue
    When: The async context is entered
    Then: The exchange should be declared, an exclusive auto-delete queue
          declared and bound with the binding key, and the server-named
          queue consumed
    """
    # Arrange
    channel.queue_declare.return_value.queue = "amq.gen-1"
    config = RabbitMQConsumeConfig(exchange="events", binding_key="order.*")
    consumer.handle(_order_placed, lambda payload: None, config=config)

    # Act
    async with consumer:
        # Assert
        channel.exchange_declare.assert_awaited_once_with(
            "events", exchange_type="fanout", durable=False
        )
        channel.queue_declare.assert_awaited_once_with(
            "", durable=False, exclusive=True, auto_delete=True, arguments=None
        )
        channel.queue_bind.assert_awaited_once_with("amq.gen-1", "events", routing_key="order.*")
        assert channel.basic_consume.await_args.args[0] == "amq.gen-1"


async def test_named_queue_with_exchange_is_durable(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    A named queue with an exchange should be declared durable and bound.

    Given: A consumer registered with both a queue name and an exchange
    When: The async context is entered
    Then: The named queue should be declared durable, bound to the
          exchange, and consumed
    """
    # Arrange
    channel.queue_declare.return_value.queue = "audit"
    config = RabbitMQConsumeConfig("audit", exchange="events", binding_key="#")
    consumer.handle(_order_placed, lambda payload: None, config=config)

    # Act
    async with consumer:
        # Assert
        channel.queue_declare.assert_awaited_once_with(
            "audit", durable=True, exclusive=False, auto_delete=False, arguments=None
        )
        channel.queue_bind.assert_awaited_once_with("audit", "events", routing_key="#")
        assert channel.basic_consume.await_args.args[0] == "audit"


async def test_prefetch_count_sets_channel_qos(connect_mock: AsyncMock, channel: AsyncMock):
    """
    A prefetch count should set channel QoS before consuming starts.

    Given: A consumer constructed with a prefetch count
    When: The async context is entered
    Then: basic_qos should be awaited with the prefetch count
    """
    # Arrange
    consumer = RabbitMQConsumer(_URL, prefetch_count=1)

    # Act
    async with consumer:
        # Assert
        channel.basic_qos.assert_awaited_once_with(prefetch_count=1)


async def test_no_prefetch_count_skips_qos(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    Without a prefetch count, no channel QoS should be set.

    Given: A consumer constructed without a prefetch count
    When: The async context is entered
    Then: basic_qos should not be awaited
    """
    async with consumer:
        channel.basic_qos.assert_not_awaited()


async def test_multiple_binding_keys_bind_each(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    An iterable binding key should bind the queue once per key.

    Given: A consumer registered with two binding keys
    When: The async context is entered
    Then: The declared queue should be bound once for each key
    """
    # Arrange
    channel.queue_declare.return_value.queue = "amq.gen-1"
    config = RabbitMQConsumeConfig(
        exchange="logs", binding_key=["info", "warning"], exchange_type="direct"
    )
    consumer.handle(_order_placed, lambda payload: None, config=config)

    # Act
    async with consumer:
        # Assert
        assert channel.queue_bind.await_count == 2
        channel.queue_bind.assert_any_await("amq.gen-1", "logs", routing_key="info")
        channel.queue_bind.assert_any_await("amq.gen-1", "logs", routing_key="warning")


async def test_declaration_flags_override_inference(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    Explicit declaration flags should override the inferred defaults.

    Given: A named-queue config with explicit flags and arguments
    When: The async context is entered
    Then: The queue should be declared with the explicit values
    """
    # Arrange
    channel.queue_declare.return_value.queue = "burst"
    config = RabbitMQConsumeConfig(
        "burst",
        exchange="events",
        durable=False,
        auto_delete=True,
        arguments={"x-message-ttl": 1000},
    )
    consumer.handle(_order_placed, lambda payload: None, config=config)

    # Act
    async with consumer:
        # Assert
        channel.queue_declare.assert_awaited_once_with(
            "burst",
            durable=False,
            exclusive=False,
            auto_delete=True,
            arguments={"x-message-ttl": 1000},
        )


async def test_handler_runs_in_delivered_envelope_scope(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    The handler should observe the delivered message's envelope as current.

    Given: A message carrying envelope headers
    When: The registered callback receives it
    Then: Envelope.current() inside the handler should carry those ids
    """
    # Arrange
    seen: list[Envelope | None] = []
    consumer.handle(_order_placed, lambda payload: seen.append(Envelope.current()), config=_config)
    envelope = Envelope()
    message = _message(b'{"order_id": 7}', headers=envelope.to_headers())

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert seen[0] is not None
    assert seen[0].message_id == envelope.message_id
    assert seen[0].correlation_id == envelope.correlation_id


async def test_handler_scopes_fresh_envelope_without_headers(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A message without envelope headers should still scope a fresh envelope.

    Given: A message carrying no envelope headers
    When: The registered callback receives it
    Then: Envelope.current() inside the handler should be a new envelope
    """
    # Arrange
    seen: list[Envelope | None] = []
    consumer.handle(_order_placed, lambda payload: seen.append(Envelope.current()), config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert seen[0] is not None
    assert seen[0].causation_id is None


async def test_native_properties_rebuild_envelope(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    Native AMQP fields should rebuild the envelope when x- headers are absent.

    Given: A message carrying native message_id, correlation_id, and
           timestamp fields but no x- headers
    When: The registered callback receives it
    Then: The handler's current envelope should carry the native values
    """
    # Arrange
    seen: list[Envelope | None] = []
    consumer.handle(_order_placed, lambda payload: seen.append(Envelope.current()), config=_config)
    envelope = Envelope()
    message = _message(b'{"order_id": 7}')
    message.header.properties.message_id = str(envelope.message_id)
    message.header.properties.correlation_id = str(envelope.correlation_id)
    message.header.properties.timestamp = envelope.timestamp

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert seen[0] is not None
    assert seen[0].message_id == envelope.message_id
    assert seen[0].correlation_id == envelope.correlation_id
    assert seen[0].timestamp == envelope.timestamp


async def test_malformed_envelope_headers_log_and_scope_fresh(
    consumer: RabbitMQConsumer, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    Malformed envelope headers should be surfaced without blocking handling.

    Given: A message whose envelope headers are not valid UUIDs
    When: The registered callback receives it
    Then: A warning should be logged, the handler should run inside a
          fresh envelope, and the message should be acked
    """
    # Arrange
    seen: list[Envelope | None] = []
    consumer.handle(_order_placed, lambda payload: seen.append(Envelope.current()), config=_config)
    headers = {MESSAGE_ID_HEADER: "junk", CORRELATION_ID_HEADER: "junk"}
    message = _message(b'{"order_id": 7}', headers=headers)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert seen[0] is not None
    assert "unparseable envelope headers" in caplog.text
    message.channel.basic_ack.assert_awaited_once_with(11)


async def test_partial_envelope_headers_log_info(
    consumer: RabbitMQConsumer, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    A lopsided identifying pair should leave evidence before minting.

    Given: A message carrying only a correlation id
    When: The registered callback receives it
    Then: An info record should note the partial headers and the handler
          should continue the delivered correlation chain
    """
    # Arrange
    caplog.set_level(logging.INFO)
    seen: list[Envelope | None] = []
    consumer.handle(_order_placed, lambda payload: seen.append(Envelope.current()), config=_config)
    correlation_id = uuid4()
    message = _message(b'{"order_id": 7}', headers={CORRELATION_ID_HEADER: str(correlation_id)})

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert "partial envelope headers" in caplog.text
    assert seen[0] is not None
    assert seen[0].correlation_id == correlation_id


async def test_async_callable_object_handler_awaited(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A handler that's an object with async __call__ should be awaited with the payload.

    Given: A connected consumer with a handler whose __call__ is async
    When: The registered callback receives a message
    Then: The handler's __call__ runs to completion and the message is acked
    """

    # Arrange
    class Handler:
        def __init__(self) -> None:
            self.received: list[_OrderPlaced] = []

        async def __call__(self, payload: _OrderPlaced) -> None:
            self.received.append(payload)

    handler = Handler()
    consumer.handle(_order_placed, handler, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert handler.received[0].order_id == 7
    message.channel.basic_ack.assert_awaited_once_with(11)


async def test_failed_start_on_enter_closes_connection(
    consumer: RabbitMQConsumer, connection: AsyncMock, channel: AsyncMock
):
    """
    A registration failing to start while entering the context closes the connection.

    Given: A consumer with a registration whose queue declaration fails
    When: The context is entered
    Then: The error propagates and the connection the consumer opened is closed
    """
    # Arrange
    consumer.handle(_order_placed, lambda _: None, config=_config)
    channel.queue_declare.side_effect = RuntimeError("declare failed")

    # Act
    with pytest.raises(RuntimeError, match="declare failed"):
        async with consumer:
            pass

    # Assert
    connection.close.assert_awaited_once()


async def test_exit_cancels_consumers_before_closing(
    consumer: RabbitMQConsumer, channel: AsyncMock, connection: AsyncMock
):
    """
    Exiting the context should cancel each live AMQP consumer before closing the connection.

    Given: A consumer with a started registration
    When: The context exits
    Then: The registration's consumer is cancelled, then the connection is closed
    """
    # Arrange
    calls: list[str] = []

    def record_cancel(tag: str) -> None:
        calls.append("cancel")

    def record_close() -> None:
        calls.append("close")

    channel.basic_cancel.side_effect = record_cancel
    connection.close.side_effect = record_close
    consumer.handle(_order_placed, lambda payload: None, config=_config)

    # Act
    async with consumer:
        pass

    # Assert
    channel.basic_cancel.assert_awaited_once_with(channel.basic_consume.return_value.consumer_tag)
    assert calls == ["cancel", "close"]


async def test_exit_waits_for_running_handler_to_ack(
    consumer: RabbitMQConsumer, channel: AsyncMock, connection: AsyncMock
):
    """
    Exiting the context should wait for a running handler to finish and ack before closing.

    Given: A connected consumer whose handler is mid-delivery
    When: The context exits
    Then: The connection stays open until the handler returns and its message is acked
    """
    # Arrange
    release = asyncio.Event()

    async def handler(payload: _OrderPlaced) -> None:
        await release.wait()

    consumer.handle(_order_placed, handler, config=_config)
    await consumer.__aenter__()
    message = _message(b'{"order_id": 1}')
    delivery = asyncio.create_task(_delivery_callback(channel)(message))
    await asyncio.sleep(0)

    # Act
    exiting = asyncio.create_task(consumer.__aexit__(None, None, None))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    closed_while_running = connection.close.await_count
    release.set()
    await exiting

    # Assert
    assert closed_while_running == 0
    assert delivery.done()
    message.channel.basic_ack.assert_awaited_once_with(11)
    connection.close.assert_awaited_once()


async def test_exit_stops_waiting_at_drain_timeout(
    connect_mock: AsyncMock,
    channel: AsyncMock,
    connection: AsyncMock,
    caplog: pytest.LogCaptureFixture,
):
    """
    Exiting the context should close the connection once the drain timeout passes.

    Given: A consumer with a short drain timeout and a handler that never returns
    When: The context exits
    Then: The connection closes without the ack, and the stuck handler is logged
    """
    # Arrange
    consumer = RabbitMQConsumer(_URL, drain_timeout=0.01)

    async def handler(payload: _OrderPlaced) -> None:
        await asyncio.Event().wait()

    consumer.handle(_order_placed, handler, config=_config)
    await consumer.__aenter__()
    message = _message(b'{"order_id": 1}')
    delivery = asyncio.create_task(_delivery_callback(channel)(message))
    await asyncio.sleep(0)

    # Act
    await consumer.__aexit__(None, None, None)

    # Assert
    connection.close.assert_awaited_once()
    message.channel.basic_ack.assert_not_awaited()
    assert "1 handler(s) still running after the drain timeout" in caplog.text
    delivery.cancel()


async def test_failed_cancel_on_exit_logs_and_closes(
    consumer: RabbitMQConsumer,
    channel: AsyncMock,
    connection: AsyncMock,
    caplog: pytest.LogCaptureFixture,
):
    """
    A consumer cancel that raises on exit should be logged without blocking the close.

    Given: A started registration whose basic_cancel raises
    When: The context exits
    Then: The failure is logged and the connection is still closed
    """
    # Arrange
    channel.basic_cancel.side_effect = ConnectionError("broker gone")
    consumer.handle(_order_placed, lambda payload: None, config=_config)

    # Act
    async with consumer:
        pass

    # Assert
    assert "failed to cancel AMQP consumers on exit" in caplog.text
    connection.close.assert_awaited_once()


async def test_handler_exiting_its_own_consumer_does_not_wait_on_itself(
    consumer: RabbitMQConsumer, channel: AsyncMock, connection: AsyncMock
):
    """
    A handler that exits its own consumer should close it without waiting on itself.

    Given: A connected consumer whose handler exits the consumer
    When: A message is delivered
    Then: The connection closes without waiting for the drain timeout
    """

    # Arrange
    async def handler(payload: _OrderPlaced) -> None:
        await consumer.__aexit__(None, None, None)

    consumer.handle(_order_placed, handler, config=_config)
    await consumer.__aenter__()

    # Act
    await asyncio.wait_for(_delivery_callback(channel)(_message(b'{"order_id": 1}')), timeout=1)

    # Assert
    connection.close.assert_awaited_once()


async def test_handle_hooks_run_before_ack(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    Hooks passed to handle should run around the handler, before the message is acked.

    Given: A connected consumer with a sync handler registered with hooks
    When: The registered callback receives a message
    Then: before, the handler, success and complete run in order, then the ack
    """
    # Arrange
    log: list[str] = []

    def before(event: Any, payload: _OrderPlaced) -> None:
        log.append(f"before {payload.order_id}")

    def success(event: Any, payload: _OrderPlaced, reply: object) -> None:
        log.append(f"success {reply!r}")

    def complete(event: Any, payload: _OrderPlaced, error: BaseException | None) -> None:
        log.append(f"complete {error!r}")

    def on_order(order: _OrderPlaced) -> str:
        log.append("handler")
        return "done"

    hooks = Hooks(before=(before,), success=(success,), complete=(complete,))
    consumer.handle(_order_placed, on_order, config=_config, hooks=hooks)
    message = _message(b'{"order_id": 7}')

    def record_ack(*_: object) -> None:
        log.append("ack")

    message.channel.basic_ack.side_effect = record_ack

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert log == ["before 7", "handler", "success 'done'", "complete None", "ack"]


async def test_raising_handle_hook_nacks(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    A before hook that raises should nack the message, as a failing handler does.

    Given: A connected consumer with a handler registered with a before hook that raises
    When: The registered callback receives a message
    Then: The handler doesn't run, and the message is nacked without requeue
    """
    # Arrange
    received: list[_OrderPlaced] = []

    def refuse(event: Any, payload: _OrderPlaced) -> None:
        raise PermissionError("not today")

    consumer.handle(_order_placed, received.append, config=_config, hooks=Hooks(before=(refuse,)))
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert received == []
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()


async def test_signal_handle_hooks_receive_no_payload(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A schema-less event's hooks should receive a NoPayload instance.

    Given: A connected consumer with a schema-less event's handler registered with an async
        before hook
    When: The registered callback receives a message with an empty body
    Then: The hook sees a NoPayload payload, and the handler runs with no arguments
    """
    # Arrange
    payloads: list[object] = []
    calls: list[str] = []

    async def record(event: Any, payload: NoPayload) -> None:
        payloads.append(payload)

    consumer.handle(
        _heartbeat, lambda: calls.append("beat"), config=_config, hooks=Hooks(before=(record,))
    )

    # Act
    async with consumer:
        await _delivery_callback(channel)(_message(b""))

    # Assert
    assert calls == ["beat"]
    assert len(payloads) == 1
    assert isinstance(payloads[0], NoPayload)


async def test_raising_success_hook_still_acks(
    consumer: RabbitMQConsumer, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    A success hook that raises shouldn't nack a message its handler already handled.

    Given: A connected consumer with a handler registered with a success hook that raises
    When: The registered callback receives a message
    Then: The handler runs, the message is acked, and the hook's error is logged
    """
    # Arrange
    received: list[_OrderPlaced] = []

    def audit(event: Any, payload: _OrderPlaced, reply: object) -> None:
        raise RuntimeError("audit down")

    consumer.handle(_order_placed, received.append, config=_config, hooks=Hooks(success=(audit,)))
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert [order.order_id for order in received] == [7]
    message.channel.basic_ack.assert_awaited_once_with(11)
    message.channel.basic_nack.assert_not_awaited()
    assert "audit down" in caplog.text


async def test_handle_middleware_runs_before_ack(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Handle middleware should wrap each handler and its hooks, before the message is acked.

    Given: A consumer with async handle middleware, and a handler registered with a hook
    When: The registered callback receives a message
    Then: The middleware wraps the hook and handler, then the message is acked
    """
    # Arrange
    log: list[str] = []

    @asynccontextmanager
    async def record(call: HandleCall) -> AsyncGenerator[None, None]:
        log.append(f"mw in {call.payload.order_id}")
        yield
        log.append("mw out")

    def before(event: Any, payload: _OrderPlaced) -> None:
        log.append("hook")

    def record_ack(*_: object) -> None:
        log.append("ack")

    consumer = RabbitMQConsumer(_URL, middleware=Middleware(handle=(record,)))
    consumer.handle(
        _order_placed,
        lambda order: log.append("handler"),
        config=_config,
        hooks=Hooks(before=(before,)),
    )
    message = _message(b'{"order_id": 7}')
    message.channel.basic_ack.side_effect = record_ack

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert log == ["mw in 7", "hook", "handler", "mw out", "ack"]


async def test_handle_middleware_refusing_nacks(
    connect_mock: AsyncMock, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    Handle middleware raising on entry should nack the message without running the handler.

    Given: A consumer whose handle middleware raises on entry
    When: The registered callback receives a message
    Then: The handler doesn't run, and the message is nacked without requeue, the error logged
    """
    # Arrange
    received: list[_OrderPlaced] = []

    @asynccontextmanager
    async def refuse(call: HandleCall) -> AsyncGenerator[None, None]:
        raise PermissionError("rate limited")
        yield

    consumer = RabbitMQConsumer(_URL, middleware=Middleware(handle=(refuse,)))
    consumer.handle(_order_placed, received.append, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert received == []
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()
    assert "rate limited" in caplog.text
    assert "for event '_OrderPlaced'" in caplog.text


async def test_handle_middleware_sees_event(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Handle middleware should see the event as call.event, and the consume config as call.config.

    Given: A consumer with handle middleware recording call.event.name and call.config
    When: The registered callback receives a message
    Then: The middleware sees the event's name and the registration's config, and the
        message is acked
    """
    # Arrange
    names: list[str] = []
    configs: list[object] = []

    @asynccontextmanager
    async def record(call: HandleCall) -> AsyncGenerator[None, None]:
        names.append(call.event.name)
        configs.append(call.config)
        yield

    consumer = RabbitMQConsumer(_URL, middleware=Middleware(handle=(record,)))
    consumer.handle(_order_placed, lambda order: None, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert names == ["_OrderPlaced"]
    assert configs == [_config]
    message.channel.basic_ack.assert_awaited_once_with(11)


@asynccontextmanager
async def _trace(call: HandleCall) -> AsyncGenerator[None, None]:
    yield


class _NotAManager:
    """A middleware class whose instances aren't context managers."""

    def __init__(self, call: Any) -> None: ...


async def test_swallowed_cancellation_is_not_nacked(connect_mock: AsyncMock, channel: AsyncMock):
    """
    A delivery cancelled while handle middleware tries to swallow it should stay cancelled.

    Given: A consumer whose handle middleware's exit returns True, around a handler that waits
    When: The delivery is cancelled while the handler waits
    Then: The cancellation propagates, and the message is neither nacked nor acked
    """
    # Arrange
    started = asyncio.Event()

    class Suppressing:
        def __init__(self, call: HandleCall) -> None: ...

        async def __aenter__(self) -> None: ...

        async def __aexit__(self, *exc: object) -> bool:
            return True

    async def wait(order: _OrderPlaced) -> None:
        started.set()
        await asyncio.Event().wait()

    consumer = RabbitMQConsumer(_URL, middleware=Middleware(handle=(Suppressing,)))
    consumer.handle(_order_placed, wait, config=_config)
    message = _message(b'{"order_id": 7}')

    async with consumer:
        delivery = asyncio.ensure_future(_delivery_callback(channel)(message))
        await started.wait()

        # Act
        delivery.cancel()
        with pytest.raises(asyncio.CancelledError):
            await delivery

    # Assert
    assert delivery.cancelled()
    message.channel.basic_nack.assert_not_awaited()
    message.channel.basic_ack.assert_not_awaited()


def test_consumer_accepts_shared_middleware():
    """
    The consumer should accept a Middleware shared with a publisher, checking only its half.

    Given: Middleware with a valid handle middleware, and a publish middleware class that
        isn't a context manager
    When: A RabbitMQConsumer is built with it
    Then: It's accepted, since the consumer never runs the publish half
    """
    # Arrange
    middleware = Middleware(publish=(cast(Any, _NotAManager),), handle=(_trace,))

    # Act
    consumer = RabbitMQConsumer(_URL, middleware=middleware)

    # Assert
    assert isinstance(consumer, RabbitMQConsumer)


def test_consumer_refuses_non_manager_middleware():
    """
    The consumer should refuse a middleware class that isn't a context manager when built.

    Given: Handle middleware that's a plain class
    When: A RabbitMQConsumer is built with it
    Then: A TypeError says the consumer can't enter it
    """
    # Act / Assert
    with pytest.raises(TypeError, match="RabbitMQConsumer can't enter"):
        RabbitMQConsumer(_URL, middleware=Middleware(handle=(cast(Any, _NotAManager),)))


async def test_timeout_middleware_nacks(
    connect_mock: AsyncMock, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    Handle middleware timing out a slow handler should nack the message.

    Given: A consumer whose handle middleware is asyncio.timeout around a slow handler
    When: The registered callback receives a message
    Then: The message is nacked without requeue, and the TimeoutError is logged
    """

    # Arrange
    async def slow(order: _OrderPlaced) -> None:
        await asyncio.Event().wait()

    consumer = RabbitMQConsumer(
        _URL, middleware=Middleware(handle=(lambda call: asyncio.timeout(0.01),))
    )
    consumer.handle(_order_placed, slow, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()
    assert "TimeoutError" in caplog.text


async def test_swallowed_timeout_nacks(
    connect_mock: AsyncMock, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    Handle middleware that swallows its own timeout should nack, not strand the message.

    Given: A consumer whose handle middleware times out a slow handler and swallows it
    When: The registered callback receives a message
    Then: The message is nacked without requeue, and a MiddlewareError is logged
    """

    # Arrange
    async def slow(order: _OrderPlaced) -> None:
        await asyncio.Event().wait()

    @asynccontextmanager
    async def time_out(call: HandleCall) -> AsyncGenerator[None, None]:
        try:
            async with asyncio.timeout(0.01):
                yield
        except TimeoutError:
            pass  # swallowed on purpose: the message must still be settled

    consumer = RabbitMQConsumer(_URL, middleware=Middleware(handle=(time_out,)))
    consumer.handle(_order_placed, slow, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()
    assert "MiddlewareError" in caplog.text
