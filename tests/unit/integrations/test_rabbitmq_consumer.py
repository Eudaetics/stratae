"""
Unit tests for the RabbitMQ consume adapter.

This test suite verifies the following behaviors:

RabbitMQConsumeConfig:
- The queue is stored on initialization.
- Exchange, binding key, and exchange type are stored on initialization.
- Omitting both the queue and the exchange raises ValueError.
- It's an immutable value: assignment is refused, and configs with the same fields, binding
  keys given as one string or a list of it, are equal.
- Configs built separately with equal arguments and equal failure policies are equal and hash
  alike, with or without either; a different policy or different arguments makes them unequal.
- A config keeps a read-only copy of its arguments: changing the dict it was given leaves the
  config and its repr unchanged, and the copy refuses assignment.

RabbitMQConsumer:
- Constructing a consumer without a deserializer is a type error and raises TypeError.
- Entering the context opens the connection and starts consuming for
  registrations made beforehand.
- Registrations made while connected start consuming immediately.
- Exiting the context closes the connection.
- Exiting without a connection closes nothing.
- Entering the context while already connected raises AlreadyConnectedError.
- A failed close on exit still leaves the consumer disconnected.
- handle registers directly or as a decorator, returning the Handler.
- handle raises TypeError for an event that isn't PubSub.
- Delivered messages are decoded with the consumer's deserializer and the
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
- Handle middleware reads the message's text headers, envelope headers included; headers that
  aren't text, such as the attempts count, are left out, and a message without headers has none.
- Handle middleware that times out a handler nacks the message, and one that swallows the
  timeout does too, logging a MiddlewareError.
- Handle middleware raising on entry nacks the message without running the handler.
- A cancellation handle middleware tries to swallow still cancels the delivery, without a nack.
- A handle middleware class that isn't a context manager is refused when the consumer is
  built; publish middleware isn't checked, so one Middleware can be shared with a publisher.
- A schema-less event's hooks receive a NoPayload instance.
- With a scope, a scope-cached dependency is made once per message and not shared between
  messages.
- With a scope, the message is acked only after the scope has closed.
- A failing handler closes the scope with its exception, then the message is nacked.
- A failure while closing the scope nacks the message, though its handler succeeded.
- A delivery cancelled at the drain timeout closes its scope with the cancellation and stays
  unacked.
- Handle middleware and hooks run inside the message's scope.
- A message that fails to decode never activates the scope.

RabbitMQFailurePolicy:
- It's an immutable value: assignment is refused, and policies with the same fields are equal
  and hash alike.
- A non-positive retry delay, or a backoff that never runs out, raises ValueError.
- A dead-letter queue name with dead_letter=False raises ValueError.
- A failure policy on a config without a named queue raises ValueError.
- retry_queue names one queue per delay and returns None past the last retry;
  dead_letter_target defaults to <queue>.dead-letter, takes a custom name, and is None
  when dropping.
- An exponential backoff names its retry queues from its capped delays.
- The consumer declares one durable retry queue per distinct delay, each expiring back into
  the consumed queue, and a durable dead-letter queue; a dropping policy declares none.
- A failed message with retries left is published to its next retry queue with its body,
  envelope ids, attempt count and failure, then acked.
- A failed message out of retries is dead-lettered with its attempt count, then acked.
- A message that fails to decode skips its retries and is dead-lettered.
- With dead_letter=False, a failed message out of retries is acked and dropped.
- A custom dead-letter queue receives the dead-lettered message.
- An unreadable attempt count is treated as a first failure.
- A failed message without a delivery tag is neither published nor settled.
- A delivery cancelled under a failure policy is neither published nor settled.
- The consumer opens its channel with publisher confirms, so retries and dead letters are
  confirmed before the original is acked.

Connection health and reconnecting:
- The consumer reports closed before entering and after exiting, and connected inside.
- When the channel closes unexpectedly, the consumer reports reconnecting, then reconnects,
  sets QoS again and resumes every registration on the new channel, including one added
  while it was reconnecting, and not one removed meanwhile.
- With reconnecting turned off, a lost channel leaves the consumer closed.
- Exiting while reconnecting stops trying and leaves the consumer closed.
"""

import asyncio
import dataclasses
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from itertools import count
from typing import Any, cast
from unittest.mock import AsyncMock, call
from uuid import UUID, uuid4

import pytest
from pamqp.commands import Basic
from pytest_mock import MockerFixture

from stratae.backoff import Delays, Exponential
from stratae.events import (
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    Envelope,
    EnvelopeConfig,
    HandleCall,
    Handler,
    Hooks,
    Middleware,
    NoPayload,
    PubSub,
    Request,
)
from stratae.events.exceptions import AlreadyConnectedError
from stratae.integrations.rabbitmq import (
    ATTEMPTS_HEADER,
    FAILURE_HEADER,
    ConnectionState,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQFailurePolicy,
)
from stratae.lifecycle import AsyncScope

_URL = "amqp://guest:guest@localhost/"


class _AuditDown(RuntimeError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("audit down")


class _CommitFailed(RuntimeError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("commit failed")


class _NotToday(PermissionError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("not today")


class _RateLimited(PermissionError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("rate limited")


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


def _decode(data: bytes, /, *, type: type[Any]) -> Any:
    """Build the payload type from a JSON object's fields."""
    return type(**json.loads(data))


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
def connection(channel: AsyncMock) -> AsyncMock:
    """Return a mock AMQP connection that allocates the mock channel."""
    connection = AsyncMock()
    connection.channel.return_value = channel
    return connection


@pytest.fixture
def connect_mock(mocker: MockerFixture, connection: AsyncMock) -> AsyncMock:
    """Patch aiormq's connect in the adapter module and return the mock."""
    mock = AsyncMock(return_value=connection)
    mocker.patch("stratae.integrations.rabbitmq._link.connect", mock)
    return mock


@pytest.fixture
def consumer(connect_mock: AsyncMock) -> RabbitMQConsumer:
    """Return a RabbitMQConsumer wired to the mocked connection."""
    return RabbitMQConsumer(_URL, deserializer=_decode)


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


def test_consume_config_is_an_immutable_value():
    """
    RabbitMQConsumeConfig should be an immutable value.

    Given: Two configs with the same fields, one binding key given as a string and the other
        as a list, and a config for another queue
    When: They're compared, and a field is assigned
    Then: The first two are equal, the third isn't, and assignment raises
    """
    config = RabbitMQConsumeConfig("audit", exchange="events", binding_key="order.*")

    assert config == RabbitMQConsumeConfig("audit", exchange="events", binding_key=["order.*"])
    assert config != RabbitMQConsumeConfig("ledger", exchange="events", binding_key="order.*")
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.queue = "ledger"  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize("with_arguments", [False, True])
@pytest.mark.parametrize("with_policy", [False, True])
def test_consume_configs_built_separately_compare_as_values(
    with_arguments: bool, with_policy: bool
):
    """
    Consume configs built separately from equal fields should be equal and hash alike.

    Given: Two configs for one queue, each built with its own arguments dict and its own
        failure policy where the case has them
    When: They're compared and hashed
    Then: They're equal and share a hash
    """

    def build() -> RabbitMQConsumeConfig:
        return RabbitMQConsumeConfig(
            "orders",
            arguments={"x-message-ttl": 1000} if with_arguments else None,
            on_failure=RabbitMQFailurePolicy(Delays(1, 10)) if with_policy else None,
        )

    first, second = build(), build()

    assert first == second
    assert hash(first) == hash(second)


def test_consume_configs_differ_by_policy_and_arguments():
    """
    Consume configs should differ when their failure policies or arguments do.

    Given: A config with arguments and a failure policy
    When: It's compared with configs that change the policy's backoff, its dead-letter queue,
        or an argument
    Then: None of them is equal to it
    """
    config = RabbitMQConsumeConfig(
        "orders", arguments={"x-message-ttl": 1000}, on_failure=RabbitMQFailurePolicy(Delays(1))
    )

    assert config != RabbitMQConsumeConfig(
        "orders", arguments={"x-message-ttl": 1000}, on_failure=RabbitMQFailurePolicy(Delays(2))
    )
    assert config != RabbitMQConsumeConfig(
        "orders",
        arguments={"x-message-ttl": 1000},
        on_failure=RabbitMQFailurePolicy(Delays(1), dead_letter_queue="dlq"),
    )
    assert config != RabbitMQConsumeConfig(
        "orders", arguments={"x-message-ttl": 2000}, on_failure=RabbitMQFailurePolicy(Delays(1))
    )


def test_consume_config_keeps_a_read_only_copy_of_arguments():
    """
    A consume config should keep a read-only copy of its arguments.

    Given: A config built from an arguments dict
    When: The dict is changed afterwards, and the config's arguments are assigned to
    Then: The config's arguments, their length, its hash and its repr are unchanged, and
        assignment raises TypeError
    """
    arguments: dict[str, Any] = {"x-message-ttl": 1000}
    config = RabbitMQConsumeConfig("orders", arguments=arguments)
    before = hash(config)

    arguments["x-message-ttl"] = 2000
    arguments["x-max-length"] = 10

    kept = config.arguments
    assert kept is not None
    assert kept == {"x-message-ttl": 1000}
    assert len(kept) == 1
    assert hash(config) == before
    assert "arguments={'x-message-ttl': 1000}" in repr(config)
    with pytest.raises(TypeError):
        kept["x-message-ttl"] = 2000  # pyright: ignore[reportIndexIssue]


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
    A delivered message should be decoded with the consumer's deserializer and acked.

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
        await asyncio.sleep(0)
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
    consumer = RabbitMQConsumer(_URL, deserializer=_decode, prefetch_count=1)

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
    consumer = RabbitMQConsumer(_URL, deserializer=_decode, drain_timeout=0.01)

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

    def on_order(order: _OrderPlaced) -> None:
        log.append("handler")

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
    assert log == ["before 7", "handler", "success None", "complete None", "ack"]


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
        raise _NotToday()

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
        await asyncio.sleep(0)
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
        raise _AuditDown()

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

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, middleware=Middleware(handle=(record,)))
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
        raise _RateLimited()
        yield

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, middleware=Middleware(handle=(refuse,)))
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

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, middleware=Middleware(handle=(record,)))
    consumer.handle(_order_placed, lambda order: None, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert names == ["_OrderPlaced"]
    assert configs == [_config]
    message.channel.basic_ack.assert_awaited_once_with(11)


async def test_handle_middleware_reads_text_headers(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Handle middleware should read the message's text headers, and only those.

    Given: A consumer with handle middleware recording the headers it reads
    When: A message arrives carrying a tenant, an attempts count and a correlation id, then one
        without headers
    Then: The middleware read the tenant and the correlation id, then no headers
    """
    # Arrange
    seen: list[dict[str, str]] = []

    @asynccontextmanager
    async def record(call: HandleCall) -> AsyncGenerator[None, None]:
        seen.append(dict(call.headers))
        yield

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, middleware=Middleware(handle=(record,)))
    consumer.handle(_order_placed, lambda order: None, config=_config)
    correlation_id = str(uuid4())
    headers = {"x-tenant": "acme", ATTEMPTS_HEADER: 2, CORRELATION_ID_HEADER: correlation_id}

    # Act
    async with consumer:
        await _delivery_callback(channel)(_message(b'{"order_id": 7}', headers))
        await _delivery_callback(channel)(_message(b'{"order_id": 8}'))

    # Assert
    assert seen == [{"x-tenant": "acme", CORRELATION_ID_HEADER: correlation_id}, {}]


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

    consumer = RabbitMQConsumer(
        _URL, deserializer=_decode, middleware=Middleware(handle=(Suppressing,))
    )
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
    consumer = RabbitMQConsumer(_URL, deserializer=_decode, middleware=middleware)

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
        RabbitMQConsumer(
            _URL, deserializer=_decode, middleware=Middleware(handle=(cast(Any, _NotAManager),))
        )


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
        _URL,
        deserializer=_decode,
        middleware=Middleware(handle=(lambda call: asyncio.timeout(0.01),)),
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

    consumer = RabbitMQConsumer(
        _URL, deserializer=_decode, middleware=Middleware(handle=(time_out,))
    )
    consumer.handle(_order_placed, slow, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()
    assert "MiddlewareError" in caplog.text


def _scoped_resource(events: list[str], *, fail_close: bool = False) -> tuple[AsyncScope, Any]:
    """Return a message scope and a resource cached in it, recording each open and close."""
    scope = AsyncScope("message")
    opened = count(1)

    @scope.cache()
    async def get_resource() -> AsyncGenerator[int, None]:
        await asyncio.sleep(0)
        n = next(opened)
        events.append(f"open {n}")
        try:
            yield n
        except BaseException as exc:
            events.append(f"close {n} with {type(exc).__name__}")
            raise
        if fail_close:
            raise _CommitFailed()
        events.append(f"close {n}")

    return scope, get_resource


async def test_scope_caches_once_per_message(connect_mock: AsyncMock, channel: AsyncMock):
    """
    A scope-cached dependency should be made once per message and not shared between messages.

    Given: A consumer with a message scope, and a handler resolving a scope-cached resource twice
    When: Two messages are delivered
    Then: Each message gets its own resource, made once, and each is closed
    """
    # Arrange
    events: list[str] = []
    scope, get_resource = _scoped_resource(events)
    seen: list[tuple[int, int]] = []

    async def handler(order: _OrderPlaced) -> None:
        seen.append((await get_resource(), await get_resource()))

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, scope=scope)
    consumer.handle(_order_placed, handler, config=_config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(_message(b'{"order_id": 1}'))
        await _delivery_callback(channel)(_message(b'{"order_id": 2}'))

    # Assert
    assert seen == [(1, 1), (2, 2)]
    assert events == ["open 1", "close 1", "open 2", "close 2"]


async def test_scope_closes_before_ack(connect_mock: AsyncMock, channel: AsyncMock):
    """
    A message should be acked only after its scope has closed.

    Given: A consumer with a message scope whose resource records its cleanup
    When: A message is handled successfully
    Then: The resource's cleanup runs before the ack
    """
    # Arrange
    events: list[str] = []
    scope, get_resource = _scoped_resource(events)

    async def handler(order: _OrderPlaced) -> None:
        await get_resource()

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, scope=scope)
    consumer.handle(_order_placed, handler, config=_config)
    message = _message(b'{"order_id": 7}')

    def record_ack(*_: object) -> None:
        events.append("ack")

    message.channel.basic_ack.side_effect = record_ack

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert events == ["open 1", "close 1", "ack"]
    message.channel.basic_nack.assert_not_awaited()


async def test_failing_handler_closes_scope_with_error_then_nacks(
    connect_mock: AsyncMock, channel: AsyncMock
):
    """
    A failing handler should close the scope with its exception before the message is nacked.

    Given: A consumer with a message scope, and a handler that raises after resolving a resource
    When: A message is delivered
    Then: The resource sees the handler's exception, then the message is nacked without requeue
    """
    # Arrange
    events: list[str] = []
    scope, get_resource = _scoped_resource(events)

    async def handler(order: _OrderPlaced) -> None:
        await get_resource()
        raise ValueError("boom")

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, scope=scope)
    consumer.handle(_order_placed, handler, config=_config)
    message = _message(b'{"order_id": 7}')

    def record_nack(*_: object, **__: object) -> None:
        events.append("nack")

    message.channel.basic_nack.side_effect = record_nack

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert events == ["open 1", "close 1 with ValueError", "nack"]
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()


async def test_failed_scope_close_nacks(
    connect_mock: AsyncMock, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    A failure while closing the scope should nack the message, though its handler succeeded.

    Given: A consumer with a message scope whose resource raises in its cleanup
    When: A message is handled successfully
    Then: The message is nacked without requeue, never acked, and the failure is logged
    """
    # Arrange
    events: list[str] = []
    scope, get_resource = _scoped_resource(events, fail_close=True)

    async def handler(order: _OrderPlaced) -> None:
        await get_resource()

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, scope=scope)
    consumer.handle(_order_placed, handler, config=_config)
    message = _message(b'{"order_id": 7}')

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)
    message.channel.basic_ack.assert_not_awaited()
    assert "commit failed" in caplog.text


async def test_cancelled_delivery_closes_scope_unacked(connect_mock: AsyncMock, channel: AsyncMock):
    """
    A delivery cancelled at shutdown should close its scope and leave the message unacked.

    Given: A consumer with a short drain timeout and a message scope, around a handler that waits
    When: The context exits and the drain timeout cancels the delivery
    Then: The scope closes with the cancellation, and the message is neither acked nor nacked
    """
    # Arrange
    events: list[str] = []
    scope, get_resource = _scoped_resource(events)
    started = asyncio.Event()

    async def wait(order: _OrderPlaced) -> None:
        await get_resource()
        started.set()
        await asyncio.Event().wait()

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, drain_timeout=0.01, scope=scope)
    consumer.handle(_order_placed, wait, config=_config)
    await consumer.__aenter__()
    message = _message(b'{"order_id": 7}')
    delivery = asyncio.ensure_future(_delivery_callback(channel)(message))
    await started.wait()

    # Act
    await consumer.__aexit__(None, None, None)
    delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivery

    # Assert
    assert events == ["open 1", "close 1 with CancelledError"]
    message.channel.basic_ack.assert_not_awaited()
    message.channel.basic_nack.assert_not_awaited()


async def test_middleware_and_hooks_run_in_scope(connect_mock: AsyncMock, channel: AsyncMock):
    """
    Handle middleware and hooks should run inside the message's scope.

    Given: A consumer with a message scope, handle middleware and a before hook that each
           resolve a scope-cached resource
    When: A message is delivered
    Then: The middleware, hook and handler all get the message's one resource
    """
    # Arrange
    events: list[str] = []
    scope, get_resource = _scoped_resource(events)
    seen: list[tuple[str, int]] = []

    @asynccontextmanager
    async def record(call: HandleCall) -> AsyncGenerator[None, None]:
        seen.append(("middleware", await get_resource()))
        yield

    async def before(event: Any, order: _OrderPlaced) -> None:
        seen.append(("hook", await get_resource()))

    async def handler(order: _OrderPlaced) -> None:
        seen.append(("handler", await get_resource()))

    consumer = RabbitMQConsumer(
        _URL, deserializer=_decode, scope=scope, middleware=Middleware(handle=(record,))
    )
    consumer.handle(_order_placed, handler, config=_config, hooks=Hooks(before=(before,)))

    # Act
    async with consumer:
        await _delivery_callback(channel)(_message(b'{"order_id": 7}'))

    # Assert
    assert seen == [("middleware", 1), ("hook", 1), ("handler", 1)]
    assert events == ["open 1", "close 1"]


async def test_undecodable_message_never_activates_scope(
    connect_mock: AsyncMock, channel: AsyncMock
):
    """
    A message that fails to decode should be nacked without activating the scope.

    Given: A consumer with a message scope
    When: A message with an undecodable body is delivered
    Then: The scope is never activated, and the message is nacked without requeue
    """
    # Arrange
    activations: list[str] = []

    class RecordingScope(AsyncScope):
        __slots__ = ()

        async def __aenter__(self) -> None:
            activations.append(self.name)
            await super().__aenter__()

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, scope=RecordingScope("message"))
    consumer.handle(_order_placed, lambda order: None, config=_config)
    message = _message(b"not json")

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert activations == []
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)


def test_failure_policy_is_an_immutable_value():
    """
    RabbitMQFailurePolicy should be an immutable value.

    Given: Two policies built from the same fields, and one dropping failures instead
    When: They're compared and hashed, and a field is assigned
    Then: The first two are equal and share a hash, the third isn't equal, and assignment
        raises
    """
    policy = RabbitMQFailurePolicy(Delays(1, 10), dead_letter_queue="dlq")
    same = RabbitMQFailurePolicy(Delays(1, 10), dead_letter_queue="dlq")

    assert policy == same
    assert hash(policy) == hash(same)
    assert policy != RabbitMQFailurePolicy(Delays(1, 10), dead_letter=False)
    with pytest.raises(dataclasses.FrozenInstanceError):
        policy.dead_letter = False  # pyright: ignore[reportAttributeAccessIssue]


def test_failure_policy_refuses_non_positive_delay():
    """
    RabbitMQFailurePolicy should refuse a retry delay that isn't positive.

    Given: A retry delay of zero
    When: A RabbitMQFailurePolicy is created
    Then: A ValueError should be raised
    """
    with pytest.raises(ValueError, match="positive"):
        RabbitMQFailurePolicy(Delays(1, 0))


def test_failure_policy_refuses_endless_backoff():
    """
    RabbitMQFailurePolicy should refuse a backoff that never runs out.

    Given: An exponential backoff without a retry limit
    When: A RabbitMQFailurePolicy is created
    Then: A ValueError should be raised, since each delay names a retry queue
    """
    with pytest.raises(ValueError, match="must run out"):
        RabbitMQFailurePolicy(Exponential(1))


def test_failure_policy_refuses_queue_when_dropping():
    """
    RabbitMQFailurePolicy should refuse a dead-letter queue name when it drops failures.

    Given: A dead-letter queue name and dead_letter=False
    When: A RabbitMQFailurePolicy is created
    Then: A ValueError should be raised
    """
    with pytest.raises(ValueError, match="drops"):
        RabbitMQFailurePolicy(dead_letter=False, dead_letter_queue="orders.dead")


def test_failure_policy_needs_named_queue():
    """
    A failure policy should need a named queue to route retries and dead letters back to.

    Given: An exchange config without a queue
    When: It's created with a failure policy
    Then: A ValueError should be raised
    """
    with pytest.raises(ValueError, match="named queue"):
        RabbitMQConsumeConfig(exchange="events", on_failure=RabbitMQFailurePolicy())


def test_failure_policy_names_its_queues():
    """
    A failure policy should name a retry queue per delay and its dead-letter queue.

    Given: Policies retrying twice, with a custom dead-letter queue, and dropping
    When: Their queues are named for the "orders" queue
    Then: Each attempt maps to its delay's retry queue, then None; the dead-letter queue is
          the default, the custom name, or None when dropping
    """
    policy = RabbitMQFailurePolicy(Delays(0.5, 2))

    assert [policy.retry_queue("orders", n) for n in (1, 2, 3)] == [
        "orders.retry.500",
        "orders.retry.2000",
        None,
    ]
    assert policy.dead_letter_target("orders") == "orders.dead-letter"
    assert RabbitMQFailurePolicy(dead_letter_queue="dlq").dead_letter_target("orders") == "dlq"
    assert RabbitMQFailurePolicy(dead_letter=False).dead_letter_target("orders") is None


def test_failure_policy_takes_exponential_backoff():
    """
    A failure policy should name its retry queues from an exponential backoff's delays.

    Given: A policy retrying four times after 1 second, doubling, capped at 4
    When: Its retry queues are named for the "orders" queue
    Then: The attempts map to the 1, 2, 4 and 4 second queues, then None
    """
    policy = RabbitMQFailurePolicy(Exponential(1, cap=4, retries=4))

    assert [policy.retry_queue("orders", n) for n in range(1, 6)] == [
        "orders.retry.1000",
        "orders.retry.2000",
        "orders.retry.4000",
        "orders.retry.4000",
        None,
    ]


async def test_failure_policy_declares_retry_and_dead_letter_queues(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    The consumer should declare a policy's retry and dead-letter queues when it starts.

    Given: A registration whose policy retries after 1, 1 and 5 seconds
    When: The async context is entered
    Then: One durable retry queue per distinct delay is declared, expiring back into the
          consumed queue, then a durable dead-letter queue
    """
    # Arrange
    policy = RabbitMQFailurePolicy(Delays(1, 1, 5))
    config = RabbitMQConsumeConfig("orders", on_failure=policy)
    consumer.handle(_order_placed, lambda payload: None, config=config)

    def back_to_orders(ttl: int) -> dict[str, Any]:
        return {
            "x-message-ttl": ttl,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": "orders",
        }

    # Act
    async with consumer:
        # Assert
        assert channel.queue_declare.await_args_list[1:] == [
            call("orders.retry.1000", durable=True, arguments=back_to_orders(1000)),
            call("orders.retry.5000", durable=True, arguments=back_to_orders(5000)),
            call("orders.dead-letter", durable=True),
        ]


async def test_dropping_policy_declares_no_dead_letter_queue(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A policy that drops failures should declare no dead-letter queue.

    Given: A registration whose policy drops failed messages without retrying
    When: The async context is entered
    Then: Only the consumed queue is declared
    """
    # Arrange
    config = RabbitMQConsumeConfig("orders", on_failure=RabbitMQFailurePolicy(dead_letter=False))
    consumer.handle(_order_placed, lambda payload: None, config=config)

    # Act
    async with consumer:
        # Assert
        assert [c.args[0] for c in channel.queue_declare.await_args_list] == ["orders"]


def _sent(message: AsyncMock) -> tuple[str, bytes, Basic.Properties, dict[str, Any]]:
    """Return the routing key, body, properties and headers a failed message was sent with."""
    published = message.channel.basic_publish.await_args
    properties: Basic.Properties = published.kwargs["properties"]
    headers = properties.headers or {}
    return published.kwargs["routing_key"], published.args[0], properties, headers


def _failing(
    policy: RabbitMQFailurePolicy, headers: dict[str, Any] | None = None
) -> tuple[RabbitMQConsumeConfig, AsyncMock]:
    """Return a config consuming "orders" under `policy`, and a message with real properties."""
    message = _message(b'{"order_id": 7}')
    message.header.properties = Basic.Properties(
        headers=headers, message_id="m-1", correlation_id="c-1"
    )
    return RabbitMQConsumeConfig("orders", on_failure=policy), message


def _boom(order: _OrderPlaced) -> None:
    raise ValueError("boom")


async def test_failed_message_retried(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    A failed message with retries left should go to its next retry queue, then be acked.

    Given: A registration retrying after 1 and 10 seconds, and a handler that raises
    When: A first delivery fails
    Then: The message is published to the 1-second retry queue with its body, envelope ids,
          one attempt and the failure, then acked, never nacked
    """
    # Arrange
    config, message = _failing(RabbitMQFailurePolicy(Delays(1, 10)))
    consumer.handle(_order_placed, _boom, config=config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    routing_key, body, properties, headers = _sent(message)
    assert (routing_key, body) == ("orders.retry.1000", b'{"order_id": 7}')
    assert (properties.message_id, properties.correlation_id) == ("m-1", "c-1")
    assert headers[ATTEMPTS_HEADER] == 1
    assert headers[FAILURE_HEADER] == "ValueError: boom"
    assert message.channel.basic_publish.await_args.kwargs["exchange"] == ""
    message.channel.basic_ack.assert_awaited_once_with(11)
    message.channel.basic_nack.assert_not_awaited()


async def test_failed_message_dead_lettered_after_retries(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A failed message out of retries should be dead-lettered with its attempt count.

    Given: A registration retrying twice, and a message that has already failed twice
    When: It fails again
    Then: It's published to the dead-letter queue with three attempts, then acked
    """
    # Arrange
    config, message = _failing(RabbitMQFailurePolicy(Delays(1, 10)), headers={ATTEMPTS_HEADER: 2})
    consumer.handle(_order_placed, _boom, config=config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    routing_key, _, _, headers = _sent(message)
    assert routing_key == "orders.dead-letter"
    assert headers[ATTEMPTS_HEADER] == 3
    message.channel.basic_ack.assert_awaited_once_with(11)


async def test_undecodable_message_skips_retries(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    A message that fails to decode should be dead-lettered without being retried.

    Given: A registration with retries left, and a body that isn't valid JSON
    When: The message is delivered
    Then: It's published to the dead-letter queue on its first attempt, then acked
    """
    # Arrange
    received: list[_OrderPlaced] = []
    config, message = _failing(RabbitMQFailurePolicy(Delays(1, 10)))
    message.body = b"not json"
    consumer.handle(_order_placed, received.append, config=config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    routing_key, body, _, headers = _sent(message)
    assert (routing_key, body) == ("orders.dead-letter", b"not json")
    assert headers[ATTEMPTS_HEADER] == 1
    assert received == []
    message.channel.basic_ack.assert_awaited_once_with(11)


async def test_dropping_policy_acks_without_publishing(
    consumer: RabbitMQConsumer, channel: AsyncMock, caplog: pytest.LogCaptureFixture
):
    """
    A policy that drops failures should ack a failed message without publishing it.

    Given: A registration that drops failed messages without retrying
    When: Its handler raises
    Then: The message is acked, nothing is published, and the drop is logged
    """
    # Arrange
    config, message = _failing(RabbitMQFailurePolicy(dead_letter=False))
    consumer.handle(_order_placed, _boom, config=config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_publish.assert_not_awaited()
    message.channel.basic_ack.assert_awaited_once_with(11)
    message.channel.basic_nack.assert_not_awaited()
    assert "dropped" in caplog.text


async def test_custom_dead_letter_queue_receives_message(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A custom dead-letter queue should receive the dead-lettered message.

    Given: A registration dead-lettering to "orders.parked" without retrying
    When: Its handler raises
    Then: The message is published to "orders.parked"
    """
    # Arrange
    config, message = _failing(RabbitMQFailurePolicy(dead_letter_queue="orders.parked"))
    consumer.handle(_order_placed, _boom, config=config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    assert _sent(message)[0] == "orders.parked"


async def test_unreadable_attempt_count_is_first_failure(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    An attempt count that isn't a positive integer should count as no earlier failures.

    Given: A registration retrying once, and a message whose attempt header is a string
    When: Its handler raises
    Then: It's retried as a first failure, with one attempt
    """
    # Arrange
    config, message = _failing(RabbitMQFailurePolicy(Delays(1)), headers={ATTEMPTS_HEADER: "many"})
    consumer.handle(_order_placed, _boom, config=config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    routing_key, _, _, headers = _sent(message)
    assert routing_key == "orders.retry.1000"
    assert headers[ATTEMPTS_HEADER] == 1


async def test_failed_message_without_delivery_tag_not_rerouted(
    consumer: RabbitMQConsumer, channel: AsyncMock
):
    """
    A failed message without a delivery tag should be neither published nor settled.

    Given: A registration with retries, and a message without a delivery tag
    When: Its handler raises
    Then: Nothing is published, acked or nacked
    """
    # Arrange
    config, message = _failing(RabbitMQFailurePolicy(Delays(1)))
    message.delivery_tag = None
    consumer.handle(_order_placed, _boom, config=config)

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    message.channel.basic_publish.assert_not_awaited()
    message.channel.basic_ack.assert_not_awaited()
    message.channel.basic_nack.assert_not_awaited()


async def test_cancelled_delivery_not_retried(consumer: RabbitMQConsumer, channel: AsyncMock):
    """
    A delivery cancelled under a failure policy should be neither retried nor settled.

    Given: A registration with retries, around a handler that waits
    When: The delivery is cancelled while the handler waits
    Then: The cancellation propagates, and nothing is published, acked or nacked
    """
    # Arrange
    started = asyncio.Event()

    async def wait(order: _OrderPlaced) -> None:
        started.set()
        await asyncio.Event().wait()

    config, message = _failing(RabbitMQFailurePolicy(Delays(1)))
    consumer.handle(_order_placed, wait, config=config)

    async with consumer:
        delivery = asyncio.ensure_future(_delivery_callback(channel)(message))
        await started.wait()

        # Act
        delivery.cancel()
        with pytest.raises(asyncio.CancelledError):
            await delivery

    # Assert
    message.channel.basic_publish.assert_not_awaited()
    message.channel.basic_ack.assert_not_awaited()
    message.channel.basic_nack.assert_not_awaited()


async def test_channel_opened_with_confirms(consumer: RabbitMQConsumer, connection: AsyncMock):
    """
    The consumer should open its channel with publisher confirms, raising on returns.

    Given: A consumer
    When: The context is entered
    Then: The channel is opened with publisher_confirms and on_return_raises set, so a retry
          or dead letter is confirmed before the original message is acked
    """
    # Act
    async with consumer:
        pass

    # Assert
    connection.channel.assert_awaited_once_with(publisher_confirms=True, on_return_raises=True)


_FAST = Exponential(0.001, factor=1)


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


async def test_consumer_reports_state(consumer: RabbitMQConsumer):
    """
    The consumer should report its connection state.

    Given: A consumer
    When: It's created, entered and exited
    Then: It's closed, connected, then closed
    """
    # Act / Assert
    assert consumer.state is ConnectionState.CLOSED
    async with consumer:
        assert consumer.state is ConnectionState.CONNECTED
    assert consumer.state is ConnectionState.CLOSED


async def test_consumer_resumes_registrations_after_channel_loss(
    connect_mock: AsyncMock, connection: AsyncMock
):
    """
    A consumer whose channel closes unexpectedly should reconnect and resume consuming.

    Given: A connected consumer with a prefetch count and two registrations
    When: Its channel closes, one registration is removed and another added while it's
          reconnecting
    Then: It reports reconnecting, then reconnects, sets QoS again, and consumes the kept
          and added registrations' queues on the new channel, but not the removed one's
    """
    # Arrange
    channels = _new_channels(connection)
    consumer = RabbitMQConsumer(
        _URL, deserializer=_decode, prefetch_count=5, reconnect=Exponential(0.05, factor=1)
    )
    consumer.handle(_order_placed, lambda order: None, config=RabbitMQConsumeConfig("kept"))
    removed = consumer.handle(
        _order_placed, lambda order: None, config=RabbitMQConsumeConfig("removed")
    )

    async with consumer:
        # Act
        channels[0].closing.set_exception(ConnectionError("broker gone"))
        await asyncio.sleep(0)
        reconnecting = consumer.state
        await consumer.remove(removed)
        consumer.handle(_order_placed, lambda order: None, config=RabbitMQConsumeConfig("added"))
        async with asyncio.timeout(1):
            await consumer.wait_for_state(ConnectionState.CONNECTED)

    # Assert
    assert reconnecting is ConnectionState.RECONNECTING
    channels[1].basic_qos.assert_awaited_once_with(prefetch_count=5)
    resumed = [c.args[0] for c in channels[1].basic_consume.await_args_list]
    assert resumed == ["kept", "added"]


async def test_lost_channel_without_reconnect_closes_consumer(
    connect_mock: AsyncMock, channel: AsyncMock, connection: AsyncMock
):
    """
    With reconnecting turned off, a lost channel should leave the consumer closed.

    Given: A connected consumer with reconnect=None
    When: Its channel closes unexpectedly
    Then: It's closed, its old connection is closed, and it never reconnects
    """
    # Arrange
    consumer = RabbitMQConsumer(_URL, deserializer=_decode, reconnect=None)

    async with consumer:
        # Act
        closed = asyncio.Event()
        connection.close.side_effect = lambda: closed.set()
        channel.closing.set_exception(ConnectionError("gone"))
        async with asyncio.timeout(1):
            await closed.wait()

        # Assert
        assert consumer.state is ConnectionState.CLOSED

    connect_mock.assert_awaited_once()


async def test_consumer_exit_while_reconnecting_stops(
    connect_mock: AsyncMock, connection: AsyncMock
):
    """
    Exiting while reconnecting should stop trying and leave the consumer closed.

    Given: A connected consumer, and a broker that never answers a reconnect
    When: Its channel closes, then its context exits
    Then: The exit returns promptly, and the consumer is closed
    """
    # Arrange
    channels = _new_channels(connection)
    consumer = RabbitMQConsumer(_URL, deserializer=_decode, reconnect=_FAST)

    attempted = asyncio.Event()

    async def never(*_: Any) -> None:
        attempted.set()
        await asyncio.Event().wait()

    await consumer.__aenter__()
    connect_mock.side_effect = never
    channels[0].closing.set_exception(ConnectionError("gone"))
    async with asyncio.timeout(1):
        await attempted.wait()

    # Act
    async with asyncio.timeout(1):
        await consumer.__aexit__(None, None, None)

    # Assert
    assert consumer.state is ConnectionState.CLOSED


def test_consumer_requires_a_deserializer():
    """
    A consumer built without a deserializer should be refused.

    Given: no deserializer
    When: a consumer is constructed
    Then: it's a type error, and raises TypeError
    """
    # Act / Assert
    with pytest.raises(TypeError, match="deserializer"):
        RabbitMQConsumer(_URL)  # pyright: ignore[reportCallIssue]


class _Ids:
    """An id source handing out UUID(int=1), UUID(int=2), and so on, in order."""

    def __init__(self) -> None:
        self.made: list[UUID] = []

    def __call__(self) -> UUID:
        made = UUID(int=len(self.made) + 1)
        self.made.append(made)
        return made


@pytest.mark.parametrize("headers", ["none", "correlation only", "unreadable"])
async def test_missing_envelope_ids_come_from_consumer_source(
    connect_mock: AsyncMock, channel: AsyncMock, headers: str
):
    """
    A consumer should draw the ids a delivered message lacks from its id source.

    Given: A consumer with a counting id source
    When: A message arrives with no envelope ids, with only a correlation id, or with
        unreadable ones
    Then: The handler's envelope takes its missing ids from the source, and keeps the
        correlation id the message carried
    """
    # Arrange
    consumer = RabbitMQConsumer(_URL, deserializer=_decode, envelope=EnvelopeConfig(new_id=_Ids()))
    seen: list[Envelope | None] = []
    consumer.handle(_order_placed, lambda payload: seen.append(Envelope.current()), config=_config)
    correlation = uuid4()
    message = _message(
        b'{"order_id": 7}',
        {
            "none": None,
            "correlation only": {"x-correlation-id": str(correlation)},
            "unreadable": {"x-message-id": "not a uuid"},
        }[headers],
    )

    # Act
    async with consumer:
        await _delivery_callback(channel)(message)

    # Assert
    [envelope] = seen
    assert envelope is not None
    assert envelope.message_id == UUID(int=1)
    expected = correlation if headers == "correlation only" else UUID(int=2)
    assert envelope.correlation_id == expected


def test_handle_types_pubsub_handlers_as_returning_none(consumer: RabbitMQConsumer):
    """
    A consumer's handler should be typed as returning None, as a pub/sub handler is.

    Given: Handlers returning None, sync and async, and handlers returning a value
    When: Each is registered, directly and as a decorator
    Then: Those returning None type-check, and those returning a value are type errors
    """

    # Arrange
    def returns_none(order: _OrderPlaced) -> None: ...

    async def resolves_to_none(order: _OrderPlaced) -> None: ...

    def returns_value(order: _OrderPlaced) -> str:
        return "done"

    # Act
    consumer.handle(_order_placed, returns_none, config=_config)
    consumer.handle(_order_placed, resolves_to_none, config=_config)
    consumer.handle(_order_placed, returns_value, config=_config)  # pyright: ignore[reportCallIssue, reportArgumentType]
    decorate = consumer.handle(_order_placed, config=_config)
    decorate(returns_value)  # pyright: ignore[reportArgumentType]

    # Assert
    assert len(consumer._registrations) == 4  # pyright: ignore[reportPrivateUsage]
