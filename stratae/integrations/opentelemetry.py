"""
OpenTelemetry tracing for buses, as bus middleware.

{py:func}`tracing` returns {py:class}`Middleware <stratae.events.middleware.Middleware>` that
traces every emit and handler call. Give it to a publisher, a consumer, an outbox producer
or a direct bus. Its publish half opens a span around each emit and writes the trace context
into the message's headers, with OpenTelemetry's configured propagators. Its handle half
reads that context from the message being handled, and opens a span around the handler as
its child. So one trace follows an event from the request that emitted it, through the
broker, into its handler. It depends only on `opentelemetry-api`; the application chooses the
SDK and exporter. With no SDK configured, it does nothing.

Spans over a broker follow OpenTelemetry's messaging semantic conventions:

- **Publishing** to RabbitMQ is a `send` span of kind `PRODUCER`. It's the message's
  creation context, which the handler's span is a child of.
- **Writing to an outbox** is a `create` span of kind `PRODUCER`, the creation context. The
  relay publishes the record later, with the context it was written with, so its `send` span
  is of kind `CLIENT`, and links to that context rather than replacing it. The handler's
  span is still the child of the write, in the trace that wrote it.
- **Handling** a message is a `process` span of kind `CONSUMER`. It's the child of the
  message's creation context, not only linked to it. The conventions allow that for a span
  covering a single message, as each handler call does, and it keeps the handling in the
  publisher's trace.

On a direct bus, an emit and each of its handler calls are `INTERNAL` spans, without
messaging attributes. Every span records the event's name, and the ids of the
{py:class}`Envelope <stratae.events.envelope.Envelope>` it runs in, when there's one. Over
a broker, the message id is `messaging.message.id`, and the correlation id
`messaging.message.conversation_id`. A call that raises records the error on its span.

HTTP, database and other spans come from OpenTelemetry's own instrumentation. Stratae's spans
join them through the current context, so a request's handler span is the parent of the
emits it makes.

````{example} Tracing an emit and its handler on a direct bus
```{code-block} python
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from stratae.events import DirectBus, PubSub
from stratae.integrations.opentelemetry import tracing

class OrderPlaced:
    def __init__(self, item: str) -> None:
        self.item = item

exporter = InMemorySpanExporter()
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(exporter))

order_placed = PubSub(OrderPlaced)
bus = DirectBus(middleware=tracing(provider))
place_order = bus.bind(order_placed, factory=OrderPlaced)
bus.handle(order_placed, lambda order: None)

place_order(item="lamp")

spans = {span.context.span_id: span for span in exporter.get_finished_spans()}
for span in spans.values():
    parent = spans[span.parent.span_id].name if span.parent else "none"
    print(f"{span.name}: {span.kind.name}, parent {parent}")
```
```{output}
handle OrderPlaced: INTERNAL, parent emit OrderPlaced
emit OrderPlaced: INTERNAL, parent none
```
````
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from opentelemetry import context, propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import Link, Span, SpanKind, Status, StatusCode, Tracer, TracerProvider

from stratae.events.envelope import Envelope
from stratae.events.middleware import HandleCall, MessageHeaders, Middleware, PublishCall
from stratae.events.outbox import OutboxConfig

if TYPE_CHECKING:
    from stratae.integrations.rabbitmq import RabbitMQConfig, RabbitMQConsumeConfig

_RABBITMQ = "stratae.integrations.rabbitmq"
"""The RabbitMQ adapter's module, read from `sys.modules` so `aiormq` is never imported."""


def _rabbitmq() -> tuple[type[RabbitMQConfig], type[RabbitMQConsumeConfig]] | None:
    """
    Return the RabbitMQ adapter's config types, or `None` if it isn't imported.

    A config of these types can only exist once the adapter is imported, so a module that
    isn't imported yet has no configs to recognize.
    """
    module = sys.modules.get(_RABBITMQ)
    if module is None:
        return None
    return module.RabbitMQConfig, module.RabbitMQConsumeConfig


class _Target(NamedTuple):
    """The messaging system and destination a call's config names, if it names them."""

    system: str | None
    destination: str | None
    attributes: dict[str, Any]


_UNKNOWN = _Target(None, None, {})


def _send_target(config: object) -> _Target:
    """Return where a publish to `config` goes."""
    rabbitmq = _rabbitmq()
    if rabbitmq is not None and isinstance(config, rabbitmq[0]):
        exchange, key = config.exchange, config.routing_key
        destination = ":".join(part for part in (exchange, key) if part) or "amq.default"
        attributes = {"messaging.rabbitmq.destination.routing_key": key} if key else {}
        return _Target("rabbitmq", destination, attributes)
    return _UNKNOWN


def _process_target(config: object) -> _Target:
    """Return where a message handled under `config` comes from."""
    rabbitmq = _rabbitmq()
    if rabbitmq is not None and isinstance(config, rabbitmq[1]):
        queue = config.queue
        destination = ":".join(part for part in (config.exchange, queue) if part)
        attributes = {"messaging.destination.anonymous": True} if queue is None else {}
        return _Target("rabbitmq", destination, attributes)
    return _UNKNOWN


def _envelope_attributes(messaging: bool) -> dict[str, Any]:
    """Return the current envelope's ids as span attributes, if there's an envelope."""
    envelope = Envelope.current()
    if envelope is None:
        return {}
    if messaging:
        attributes = {
            "messaging.message.id": str(envelope.message_id),
            "messaging.message.conversation_id": str(envelope.correlation_id),
        }
    else:
        attributes = {
            "stratae.envelope.message_id": str(envelope.message_id),
            "stratae.envelope.correlation_id": str(envelope.correlation_id),
        }
    if envelope.causation_id is not None:
        attributes["stratae.envelope.causation_id"] = str(envelope.causation_id)
    return attributes


def _internal_attributes(event: str) -> dict[str, Any]:
    """Return an in-process span's attributes."""
    return {"stratae.event": event} | _envelope_attributes(messaging=False)


def _messaging_attributes(
    event: str, target: _Target, operation: str, operation_type: str
) -> dict[str, Any]:
    """Return a messaging span's attributes, per the messaging semantic conventions."""
    attributes: dict[str, Any] = {
        "stratae.event": event,
        "messaging.operation.name": operation,
        "messaging.operation.type": operation_type,
    }
    if target.system is not None:
        attributes["messaging.system"] = target.system
    if target.destination:
        attributes["messaging.destination.name"] = target.destination
    return attributes | target.attributes | _envelope_attributes(messaging=True)


def _type_name(error: BaseException) -> str:
    """Return an error's fully qualified type name, as `error.type` takes it."""
    kind = type(error)
    if kind.__module__ == "builtins":
        return kind.__qualname__
    return f"{kind.__module__}.{kind.__qualname__}"


class _Traced:
    """A span around one call, current while the call runs."""

    __slots__ = (
        "_inject",
        "_kind",
        "_links",
        "_name",
        "_parent",
        "_attributes",
        "_span",
        "_token",
        "_tracer",
    )

    def __init__(
        self,
        tracer: Tracer,
        name: str,
        kind: SpanKind,
        attributes: dict[str, Any],
        *,
        parent: Context | None = None,
        links: tuple[Link, ...] = (),
        inject: MessageHeaders | None = None,
    ) -> None:
        """
        Describe the span to open.

        :param parent: The context to open it in; the current one if `None`.
        :param links: Links to other spans' contexts.
        :param inject: Headers to write the span's context into once it's open.
        """
        self._tracer = tracer
        self._name = name
        self._kind = kind
        self._attributes = attributes
        self._parent = parent
        self._links = links
        self._inject = inject

    def __enter__(self) -> Span:
        """Open the span, make it current, and write its context into the headers."""
        self._span = self._tracer.start_span(
            self._name,
            context=self._parent,
            kind=self._kind,
            attributes=self._attributes,
            links=self._links,
        )
        self._token = context.attach(trace.set_span_in_context(self._span, self._parent))
        if self._inject is not None:
            propagate.inject(self._inject)
        return self._span

    def __exit__(self, kind: object, error: BaseException | None, traceback: object) -> None:
        """Record the call's error, if any, end the span, and restore the context."""
        try:
            if error is not None:
                self._span.set_attribute("error.type", _type_name(error))
                self._span.record_exception(error)
                self._span.set_status(Status(StatusCode.ERROR, str(error)))
            self._span.end()
        finally:
            context.detach(self._token)


class _Publish:
    """Publish middleware opening a span around each emit."""

    __slots__ = ("_tracer",)

    def __init__(self, tracer: Tracer) -> None:
        self._tracer = tracer

    def __call__(self, call: PublishCall) -> _Traced:
        """Return the span for an emit."""
        event = call.event.name
        if call.config is None:
            attributes = _internal_attributes(event)
            return _Traced(
                self._tracer, f"emit {event}", SpanKind.INTERNAL, attributes, inject=call.headers
            )
        config: object = call.config
        if isinstance(config, OutboxConfig):
            outbox = cast("OutboxConfig[object]", config)
            target, operation, operation_type = _send_target(outbox.config), "create", "create"
        else:
            target, operation, operation_type = _send_target(config), "publish", "send"
        attributes = _messaging_attributes(event, target, operation, operation_type)
        name = f"{operation} {target.destination or event}"
        created = trace.get_current_span(propagate.extract(call.headers)).get_span_context()
        if created.is_valid:
            # Created earlier, as a relay's record was: link to its creation, and keep it.
            return _Traced(self._tracer, name, SpanKind.CLIENT, attributes, links=(Link(created),))
        return _Traced(self._tracer, name, SpanKind.PRODUCER, attributes, inject=call.headers)


class _Handle:
    """Handle middleware opening a span around each handler call."""

    __slots__ = ("_tracer",)

    def __init__(self, tracer: Tracer) -> None:
        self._tracer = tracer

    def __call__(self, call: HandleCall) -> _Traced:
        """Return the span for a handler call, a child of the message's creation context."""
        event = call.event.name
        # The message's creation context, over the current one: the current span if none.
        parent = propagate.extract(call.headers, context=context.get_current())
        if call.config is None:
            attributes = _internal_attributes(event)
            return _Traced(
                self._tracer, f"handle {event}", SpanKind.INTERNAL, attributes, parent=parent
            )
        target = _process_target(call.config)
        attributes = _messaging_attributes(event, target, "process", "process")
        name = f"process {target.destination or event}"
        return _Traced(self._tracer, name, SpanKind.CONSUMER, attributes, parent=parent)


def tracing(tracer_provider: TracerProvider | None = None) -> Middleware:
    """
    Return middleware tracing every emit and handler call on a bus.

    :param tracer_provider: The provider to get the tracer from; the global one if `None`.
    :returns: {py:class}`Middleware <stratae.events.middleware.Middleware>` with a `publish`
        and a `handle` half, so it can be given to a publisher, a consumer, or both.
    """
    tracer = trace.get_tracer(__name__, tracer_provider=tracer_provider)
    return Middleware(publish=(_Publish(tracer),), handle=(_Handle(tracer),))
