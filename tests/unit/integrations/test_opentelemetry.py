"""
Unit tests for the OpenTelemetry bus middleware.

This test suite verifies the following behaviors:

Direct buses:
- An emit and each handler call are INTERNAL spans, the handler's a child of the emit's,
  carrying the event's name and the envelope's ids, and no messaging attributes.
- An emit inside another span is its child; a handler's own emit is a child of the handler's.
- On an async bus, concurrent emits' handlers are each the child of their own emit.
- A handler that raises records the error on its span: ERROR status, error.type and an
  exception event; the error still propagates.

RabbitMQ:
- A publish is a PRODUCER span named after its exchange and routing key, with the messaging
  attributes and envelope ids, and its context is sent in the message's headers.
- The default exchange with no routing key is named amq.default; a routing key alone names it.
- A publish whose headers already carry a context is a CLIENT span linking to it, and the
  headers keep that context.
- A handled message is a CONSUMER span, the child of the context in its headers, named after
  the exchange and queue; a private queue is marked anonymous.
- A failing handler's span records the error, and the message is nacked.

Outbox:
- A write is a PRODUCER create span, whose context is stored in the record's headers.

Without an SDK or a known adapter:
- With a no-op tracer provider, messages carry no trace headers, and handling still works.
- A config that doesn't describe its destination, or whose messaging_destination isn't a
  Destination, is a messaging span named after the event, with no system or destination.
- A config from any adapter that describes its destination gets its system, name and
  attributes, and spans named after it. A destination with no name gives its system, and
  spans named after the event.
- A config whose messaging_operation isn't an Operation is traced as a publish.
- A config that declares a create operation is traced as an outbox write is: a PRODUCER
  create span named after its destination, whose context is sent in the headers.
"""

import asyncio
import json
from itertools import pairwise
from typing import Any
from unittest.mock import AsyncMock

import pytest
from opentelemetry import propagate, trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NoOpTracerProvider, SpanContext, SpanKind, StatusCode
from pytest_mock import MockerFixture

from stratae.events import (
    AsyncDirectBus,
    Destination,
    DirectBus,
    Envelope,
    EnvelopeConfig,
    Operation,
    PubSub,
)
from stratae.events.middleware import HandleCall, MessageHeaders, PublishCall, run_middleware
from stratae.events.outbox import InMemoryOutboxStore, OutboxConfig, OutboxProducer
from stratae.integrations.opentelemetry import tracing
from stratae.integrations.rabbitmq import (
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQPublisher,
)

_URL = "amqp://guest:guest@localhost/"


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


_order_placed = PubSub(_OrderPlaced)


class _OutOfStock(LookupError):
    """An error from outside builtins, recorded under its module's name."""

    def __init__(self) -> None:
        super().__init__("out of stock")


def _encode(payload: object) -> bytes:
    """Serialize a test payload's attributes as JSON."""
    return json.dumps(vars(payload)).encode()


def _decode(data: bytes, /, *, type: type[Any]) -> Any:
    """Build the payload type from a JSON object's fields."""
    return type(**json.loads(data))


def _config() -> RabbitMQConfig:
    """Return a RabbitMQ config publishing to the events exchange."""
    return RabbitMQConfig("events", "order.placed")


@pytest.fixture
def exporter() -> InMemorySpanExporter:
    """Return an exporter collecting finished spans."""
    return InMemorySpanExporter()


@pytest.fixture
def provider(exporter: InMemorySpanExporter) -> TracerProvider:
    """Return a tracer provider exporting to the in-memory exporter."""
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


def _spans(exporter: InMemorySpanExporter) -> dict[str, ReadableSpan]:
    """Return the finished spans by name."""
    return {span.name: span for span in exporter.get_finished_spans()}


def _parent_of(span: ReadableSpan) -> int | None:
    """Return the span id of a span's parent, or None for a root."""
    return span.parent.span_id if span.parent is not None else None


def _context_of(span: ReadableSpan) -> SpanContext:
    """Return a finished span's own context."""
    assert span.context is not None
    return span.context


def _id_of(span: ReadableSpan) -> int:
    """Return a span's own span id."""
    return _context_of(span).span_id


def _trace_of(span: ReadableSpan) -> int:
    """Return a span's trace id."""
    return _context_of(span).trace_id


# region: Direct buses


def test_direct_bus_spans(provider: TracerProvider, exporter: InMemorySpanExporter):
    """
    An emit and its handler call should be INTERNAL spans, the handler's the emit's child.

    Given: A direct bus with envelopes, traced, and a handler
    When: An event is emitted
    Then: The emit and handle spans are INTERNAL, the handle span a child of the emit span,
        each with the event's name and the envelope's ids, and no messaging attributes
    """
    # Arrange
    seen: list[Envelope | None] = []
    bus = DirectBus(envelope=EnvelopeConfig(), middleware=tracing(provider))
    bus.handle(_order_placed, lambda order: seen.append(Envelope.current()))

    # Act
    bus.emit(_order_placed, _OrderPlaced(1))

    # Assert
    spans = _spans(exporter)
    emit, handle = spans["emit _OrderPlaced"], spans["handle _OrderPlaced"]
    (envelope,) = seen
    assert envelope is not None
    assert (emit.kind, handle.kind) == (SpanKind.INTERNAL, SpanKind.INTERNAL)
    assert _parent_of(handle) == _id_of(emit)
    assert _parent_of(emit) is None
    for span in (emit, handle):
        assert dict(span.attributes or {}) == {
            "stratae.event": "_OrderPlaced",
            "stratae.envelope.message_id": str(envelope.message_id),
            "stratae.envelope.correlation_id": str(envelope.correlation_id),
        }


def test_direct_bus_spans_nest(provider: TracerProvider, exporter: InMemorySpanExporter):
    """
    An emit should be the child of the current span, and a handler's emit of its handler's.

    Given: A traced direct bus with envelopes, whose handler for one event emits another
    When: The first event is emitted inside a request span
    Then: The spans form one chain: request, emit, handle, then the inner emit and handle;
        the inner spans carry the outer message's id as their causation id
    """
    # Arrange
    shipped = PubSub(name="shipped")
    bus = DirectBus(envelope=EnvelopeConfig(), middleware=tracing(provider))
    ship = bus.bind(shipped)
    bus.handle(shipped, lambda: None)
    bus.handle(_order_placed, lambda order: ship())
    tracer = provider.get_tracer("test")

    # Act
    with tracer.start_as_current_span("request"):
        bus.emit(_order_placed, _OrderPlaced(1))

    # Assert
    spans = _spans(exporter)
    chain = [
        "request",
        "emit _OrderPlaced",
        "handle _OrderPlaced",
        "emit shipped",
        "handle shipped",
    ]
    for parent, child in pairwise(chain):
        assert _parent_of(spans[child]) == _id_of(spans[parent])
    assert len({_trace_of(span) for span in spans.values()}) == 1
    outer = (spans["emit _OrderPlaced"].attributes or {})["stratae.envelope.message_id"]
    assert (spans["handle shipped"].attributes or {})["stratae.envelope.causation_id"] == outer


async def test_async_bus_concurrent_emits(provider: TracerProvider, exporter: InMemorySpanExporter):
    """
    Concurrent emits' handler spans should each be the child of their own emit.

    Given: A traced async bus whose handler yields to the loop
    When: Two emits run concurrently, each inside its own request span
    Then: Each handle span is the child of the emit span in its own trace
    """

    # Arrange
    async def handler(order: _OrderPlaced) -> None:
        await asyncio.sleep(0)

    bus = AsyncDirectBus(middleware=tracing(provider))
    bus.handle(_order_placed, handler)
    tracer = provider.get_tracer("test")

    async def request(order_id: int) -> None:
        with tracer.start_as_current_span(f"request {order_id}"):
            await bus.emit(_order_placed, _OrderPlaced(order_id))

    # Act
    await asyncio.gather(request(1), request(2))

    # Assert
    spans = exporter.get_finished_spans()
    emits = {_trace_of(span): span for span in spans if span.name == "emit _OrderPlaced"}
    handles = [span for span in spans if span.name == "handle _OrderPlaced"]
    assert len(emits) == len(handles) == 2
    for handle in handles:
        assert _parent_of(handle) == _id_of(emits[_trace_of(handle)])


def test_failing_handler_recorded(provider: TracerProvider, exporter: InMemorySpanExporter):
    """
    A handler that raises should have the error recorded on its span.

    Given: A traced direct bus whose handler raises an error from outside builtins
    When: An event is emitted
    Then: The error propagates; the handle and emit spans have ERROR status, error.type, and
        an exception event
    """

    # Arrange
    def fail(order: _OrderPlaced) -> None:
        raise _OutOfStock()

    bus = DirectBus(middleware=tracing(provider))
    bus.handle(_order_placed, fail)

    # Act
    with pytest.raises(ExceptionGroup):
        bus.emit(_order_placed, _OrderPlaced(1))

    # Assert
    handle = _spans(exporter)["handle _OrderPlaced"]
    emit = _spans(exporter)["emit _OrderPlaced"]
    assert handle.status.status_code is StatusCode.ERROR
    assert handle.status.description == "out of stock"
    assert (handle.attributes or {})["error.type"] == f"{__name__}._OutOfStock"
    assert [event.name for event in handle.events] == ["exception"]
    assert (emit.attributes or {})["error.type"] == "stratae.events.exceptions.HandlerErrors"


def test_builtin_error_named_bare(provider: TracerProvider, exporter: InMemorySpanExporter):
    """
    A builtin error should be recorded by its bare name, as error.type takes it.

    Given: A traced direct bus whose handler raises KeyError
    When: An event is emitted
    Then: The handle span's error.type is "KeyError"
    """

    # Arrange
    def fail(order: _OrderPlaced) -> None:
        raise KeyError(order.order_id)

    bus = DirectBus(middleware=tracing(provider))
    bus.handle(_order_placed, fail)

    # Act
    with pytest.raises(ExceptionGroup):
        bus.emit(_order_placed, _OrderPlaced(1))

    # Assert
    handle = _spans(exporter)["handle _OrderPlaced"]
    assert (handle.attributes or {})["error.type"] == "KeyError"


# endregion

# region: RabbitMQ


@pytest.fixture
def connect_mock(mocker: MockerFixture, channel: AsyncMock) -> AsyncMock:
    """Patch aiormq's connect in the adapter module to allocate the mock channel."""
    connection = AsyncMock()
    connection.channel.return_value = channel
    mock = AsyncMock(return_value=connection)
    mocker.patch("stratae.integrations.rabbitmq._link.connect", mock)
    return mock


async def test_publish_span(
    connect_mock: AsyncMock,
    channel: AsyncMock,
    provider: TracerProvider,
    exporter: InMemorySpanExporter,
):
    """
    A publish should be a PRODUCER span, whose context is sent with the message.

    Given: A traced publisher
    When: An event is published to an exchange with a routing key
    Then: The span is a PRODUCER named after both, with the messaging attributes and the
        message's envelope ids, and the message's headers carry the span's context
    """
    # Arrange
    publisher = RabbitMQPublisher(_URL, serializer=_encode, middleware=tracing(provider))

    # Act
    publisher.route(_order_placed, RabbitMQConfig("events", "order.placed"))
    async with publisher:
        await publisher.emit(_order_placed, _OrderPlaced(1))

    # Assert
    span = _spans(exporter)["publish events:order.placed"]
    headers = channel.basic_publish.await_args.kwargs["properties"].headers
    envelope = Envelope.from_headers(headers)
    assert span.kind is SpanKind.PRODUCER
    assert dict(span.attributes or {}) == {
        "stratae.event": "_OrderPlaced",
        "messaging.system": "rabbitmq",
        "messaging.operation.name": "publish",
        "messaging.operation.type": "send",
        "messaging.destination.name": "events:order.placed",
        "messaging.rabbitmq.destination.routing_key": "order.placed",
        "messaging.message.id": str(envelope.message_id),
        "messaging.message.conversation_id": str(envelope.correlation_id),
    }
    sent = trace.get_current_span(propagate.extract(headers)).get_span_context()
    assert (sent.trace_id, sent.span_id) == (_trace_of(span), _id_of(span))


@pytest.mark.parametrize(
    ("exchange", "routing_key", "name"),
    [("", "", "publish amq.default"), ("", "orders", "publish orders")],
)
async def test_publish_span_default_exchange(
    connect_mock: AsyncMock,
    provider: TracerProvider,
    exporter: InMemorySpanExporter,
    exchange: str,
    routing_key: str,
    name: str,
):
    """
    A publish to the default exchange should be named after its routing key, or amq.default.

    Given: A traced publisher
    When: An event is published to the default exchange, with or without a routing key
    Then: The span is named after the routing key, or amq.default without one
    """
    # Arrange
    publisher = RabbitMQPublisher(_URL, serializer=_encode, middleware=tracing(provider))

    # Act
    publisher.route(_order_placed, RabbitMQConfig(exchange, routing_key))
    async with publisher:
        await publisher.emit(_order_placed, _OrderPlaced(1))

    # Assert
    assert [span.name for span in exporter.get_finished_spans()] == [name]


async def test_republish_links_to_creation(
    connect_mock: AsyncMock,
    channel: AsyncMock,
    provider: TracerProvider,
    exporter: InMemorySpanExporter,
):
    """
    A publish whose headers already carry a context should link to it, and keep it.

    Given: A traced publisher, and headers carrying the context of an earlier write span
    When: An event is published with those headers, as a relay re-publishes a record
    Then: The span is a CLIENT span in its own trace, linking to the write's context, and the
        message's headers still carry the write's context
    """
    # Arrange
    publisher = RabbitMQPublisher(_URL, serializer=_encode, middleware=tracing(provider))
    created: dict[str, str] = {}
    with provider.get_tracer("test").start_as_current_span("write") as write:
        propagate.inject(created)

    # Act
    publisher.route(_order_placed, _config())
    async with publisher:
        await publisher.emit(_order_placed, _OrderPlaced(1), headers=created)

    # Assert
    span = _spans(exporter)["publish events:order.placed"]
    headers = channel.basic_publish.await_args.kwargs["properties"].headers
    sent = trace.get_current_span(propagate.extract(headers)).get_span_context()
    assert span.kind is SpanKind.CLIENT
    assert [link.context.span_id for link in span.links] == [write.get_span_context().span_id]
    assert _trace_of(span) != write.get_span_context().trace_id
    assert sent.span_id == write.get_span_context().span_id


def _message(headers: dict[str, Any]) -> AsyncMock:
    """Return a mock delivered message for an order, carrying the headers."""
    message = AsyncMock()
    message.body = b'{"order_id": 7}'
    message.delivery_tag = 11
    message.header.properties.headers = headers
    message.header.properties.message_id = None
    message.header.properties.correlation_id = None
    message.header.properties.timestamp = None
    return message


@pytest.mark.parametrize(
    ("config", "name", "anonymous"),
    [
        (RabbitMQConsumeConfig("orders"), "process orders", False),
        (RabbitMQConsumeConfig("orders", exchange="events"), "process events:orders", False),
        (RabbitMQConsumeConfig(exchange="events"), "process events", True),
    ],
    ids=["queue", "exchange and queue", "private queue"],
)
async def test_process_span(
    connect_mock: AsyncMock,
    channel: AsyncMock,
    provider: TracerProvider,
    exporter: InMemorySpanExporter,
    config: RabbitMQConsumeConfig,
    name: str,
    anonymous: bool,
):
    """
    A handled message should be a CONSUMER span, the child of the context in its headers.

    Given: A traced consumer, and a message whose headers carry a publish span's context and
        an envelope
    When: The message is delivered
    Then: The span is a CONSUMER named after the exchange and queue, the publish span's
        child, with the messaging attributes and the message's envelope ids
    """
    # Arrange
    consumer = RabbitMQConsumer(_URL, deserializer=_decode, middleware=tracing(provider))
    consumer.handle(_order_placed, lambda order: None, config=config)
    envelope = Envelope()
    headers: dict[str, Any] = envelope.to_headers()
    with provider.get_tracer("test").start_as_current_span("publish") as publish:
        propagate.inject(headers)

    # Act
    async with consumer:
        await channel.basic_consume.await_args.args[1](_message(headers))

    # Assert
    span = _spans(exporter)[name]
    attributes = dict(span.attributes or {})
    assert span.kind is SpanKind.CONSUMER
    assert _parent_of(span) == publish.get_span_context().span_id
    assert _trace_of(span) == publish.get_span_context().trace_id
    assert attributes["messaging.system"] == "rabbitmq"
    assert attributes["messaging.operation.type"] == "process"
    assert attributes["messaging.destination.name"] == name.removeprefix("process ")
    assert attributes["messaging.message.id"] == str(envelope.message_id)
    assert attributes.get("messaging.destination.anonymous", False) is anonymous


async def test_failing_consumer_handler_recorded(
    connect_mock: AsyncMock,
    channel: AsyncMock,
    provider: TracerProvider,
    exporter: InMemorySpanExporter,
):
    """
    A failing handler's span should record the error, and the message should be nacked.

    Given: A traced consumer whose handler raises
    When: A message is delivered
    Then: The process span has ERROR status and error.type, and the message is nacked
    """

    # Arrange
    def fail(order: _OrderPlaced) -> None:
        raise _OutOfStock()

    consumer = RabbitMQConsumer(_URL, deserializer=_decode, middleware=tracing(provider))
    consumer.handle(_order_placed, fail, config=RabbitMQConsumeConfig("orders"))
    message = _message({})

    # Act
    async with consumer:
        await channel.basic_consume.await_args.args[1](message)

    # Assert
    span = _spans(exporter)["process orders"]
    assert span.status.status_code is StatusCode.ERROR
    assert (span.attributes or {})["error.type"] == f"{__name__}._OutOfStock"
    message.channel.basic_nack.assert_awaited_once_with(11, requeue=False)


# endregion

# region: Outbox


async def test_outbox_write_is_create_span(
    provider: TracerProvider, exporter: InMemorySpanExporter
):
    """
    An outbox write should be a PRODUCER create span, stored as the record's context.

    Given: A traced outbox producer bound to a RabbitMQ config
    When: An event is written
    Then: The span is a PRODUCER create span named after the destination, and the record's
        headers carry its context
    """
    # Arrange
    store = InMemoryOutboxStore()
    outbox = OutboxProducer(store, lambda order: b"{}", middleware=tracing(provider))

    # Act
    outbox.route(_order_placed, OutboxConfig(_config()))
    await outbox.emit(_order_placed, _OrderPlaced(1))

    # Assert
    span = _spans(exporter)["create events:order.placed"]
    (record,) = store.pending()
    stored = trace.get_current_span(propagate.extract(record.headers)).get_span_context()
    assert span.kind is SpanKind.PRODUCER
    assert (span.attributes or {})["messaging.operation.type"] == "create"
    assert (span.attributes or {})["messaging.message.id"] == str(record.id)
    assert stored.span_id == _id_of(span)


# endregion

# region: Without an SDK or a known adapter


async def test_no_sdk_changes_nothing(connect_mock: AsyncMock, channel: AsyncMock):
    """
    With a no-op tracer provider, messages should carry no trace headers.

    Given: A publisher and a direct bus traced with a no-op tracer provider
    When: An event is published, and another emitted to a handler
    Then: The message carries only its envelope's headers, and the handler ran
    """
    # Arrange
    ran: list[int] = []
    publisher = RabbitMQPublisher(
        _URL, serializer=_encode, middleware=tracing(NoOpTracerProvider())
    )
    bus = DirectBus(middleware=tracing(NoOpTracerProvider()))
    bus.handle(_order_placed, lambda order: ran.append(order.order_id))

    # Act
    publisher.route(_order_placed, _config())
    async with publisher:
        await publisher.emit(_order_placed, _OrderPlaced(1))
    bus.emit(_order_placed, _OrderPlaced(2))

    # Assert
    headers = channel.basic_publish.await_args.kwargs["properties"].headers
    assert headers == Envelope.from_headers(headers).to_headers()
    assert ran == [2]


class _Undescribed:
    """A routing config from an adapter that doesn't describe its destination."""


class _NotADestination:
    """A routing config with an unrelated attribute that happens to share the name."""

    messaging_destination = "audit"
    messaging_operation = "create"


@pytest.mark.parametrize(
    "config", ["custom", _Undescribed(), _NotADestination()], ids=["str", "plain", "unrelated"]
)
def test_undescribed_config(
    provider: TracerProvider, exporter: InMemorySpanExporter, config: object
):
    """
    A config that doesn't describe its destination should give spans named after the event.

    Given: A config with no messaging_destination or messaging_operation, or ones that
        aren't a Destination and an Operation
    When: The publish and handle middleware run around a call with it
    Then: The spans are named after the event, with no system or destination, and the emit is
        traced as a publish
    """
    # Arrange
    middleware = tracing(provider)
    publish = PublishCall(_order_placed, config, _OrderPlaced(1), MessageHeaders())

    # Act
    run_middleware(middleware.publish, publish, lambda: None)
    run_middleware(middleware.handle, _handle_call(config, publish.headers), lambda: None)

    # Assert
    spans = _spans(exporter)
    assert set(spans) == {"publish _OrderPlaced", "process _OrderPlaced"}
    for span in spans.values():
        assert "messaging.system" not in (span.attributes or {})
        assert "messaging.destination.name" not in (span.attributes or {})
    assert (spans["publish _OrderPlaced"].attributes or {})["messaging.operation.type"] == "send"


class _QueueConfig:
    """A routing config from an adapter of the test's own, describing its destination."""

    def __init__(self, destination: Destination) -> None:
        self.messaging_destination = destination


def test_described_config_gets_messaging_attributes(
    provider: TracerProvider, exporter: InMemorySpanExporter
):
    """
    A config from any adapter that describes its destination should get messaging attributes.

    Given: A test adapter's config whose Destination has a system, a name, and an attribute
        of its own
    When: The publish and handle middleware run around a call with it
    Then: The spans are named after the destination and carry its system, name and attribute
    """
    # Arrange
    destination = Destination("testmq", "orders", attributes={"messaging.testmq.lane": 3})
    config = _QueueConfig(destination)
    middleware = tracing(provider)
    publish = PublishCall(_order_placed, config, _OrderPlaced(1), MessageHeaders())

    # Act
    run_middleware(middleware.publish, publish, lambda: None)
    run_middleware(middleware.handle, _handle_call(config, publish.headers), lambda: None)

    # Assert
    spans = _spans(exporter)
    assert set(spans) == {"publish orders", "process orders"}
    for span in spans.values():
        attributes = span.attributes or {}
        assert attributes["messaging.system"] == "testmq"
        assert attributes["messaging.destination.name"] == "orders"
        assert attributes["messaging.testmq.lane"] == 3
        assert "messaging.destination.anonymous" not in attributes


def _handle_call(config: object, headers: MessageHeaders) -> HandleCall:
    """Return a handler call for an order, under the config and with the headers."""
    return HandleCall(_order_placed, config, _OrderPlaced(1), dict(headers))


# endregion


def test_described_config_without_name(provider: TracerProvider, exporter: InMemorySpanExporter):
    """
    A destination with no name should give its system, and spans named after the event.

    Given: A test adapter's config whose Destination has a system but no name
    When: The publish and handle middleware run around a call with it
    Then: The spans are named after the event, and carry the system but no destination name
    """
    # Arrange
    config = _QueueConfig(Destination("testmq"))
    middleware = tracing(provider)
    publish = PublishCall(_order_placed, config, _OrderPlaced(1), MessageHeaders())

    # Act
    run_middleware(middleware.publish, publish, lambda: None)
    run_middleware(middleware.handle, _handle_call(config, publish.headers), lambda: None)

    # Assert
    spans = _spans(exporter)
    assert set(spans) == {"publish _OrderPlaced", "process _OrderPlaced"}
    for span in spans.values():
        attributes = span.attributes or {}
        assert attributes["messaging.system"] == "testmq"
        assert "messaging.destination.name" not in attributes


class _CreatingConfig:
    """A routing config from an adapter of the test's own, creating each message to send later."""

    messaging_destination = Destination("testmq", "orders")
    messaging_operation = Operation("create", "create")


def test_creating_config_traced_as_a_write(
    provider: TracerProvider, exporter: InMemorySpanExporter
):
    """
    A config that declares a create operation should be traced as an outbox write is.

    Given: A test adapter's config declaring a create operation and a destination
    When: The publish middleware runs around an emit with it
    Then: The span is a PRODUCER create span named after the destination, and its context is
        written into the headers
    """
    # Arrange
    middleware = tracing(provider)
    publish = PublishCall(_order_placed, _CreatingConfig(), _OrderPlaced(1), MessageHeaders())

    # Act
    run_middleware(middleware.publish, publish, lambda: None)

    # Assert
    span = _spans(exporter)["create orders"]
    attributes = span.attributes or {}
    sent = trace.get_current_span(propagate.extract(publish.headers)).get_span_context()
    assert span.kind is SpanKind.PRODUCER
    assert attributes["messaging.operation.name"] == "create"
    assert attributes["messaging.operation.type"] == "create"
    assert attributes["messaging.system"] == "testmq"
    assert sent.span_id == _id_of(span)
