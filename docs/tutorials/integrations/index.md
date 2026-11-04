# Integrations

`stratae.integrations` bridges the core modules to specific third-party tools. Each integration is a small, focused module — importing it is what activates the bridge; nothing here changes how the core modules behave on their own.

- **[ASGI](../../apidocs/stratae.integrations/stratae.integrations.asgi)** — `EnvelopeMiddleware` runs each HTTP request and WebSocket connection inside its own `Envelope`. Events emitted while handling it share its correlation id, and a caller's correlation id header is adopted and echoed back.
- **[msgspec](../../apidocs/stratae.integrations/stratae.integrations.msgspec)** — registers a `msgspec.Struct`-specific fast path onto `stratae.serde.serialize`.
- **[OpenTelemetry](../../apidocs/stratae.integrations/stratae.integrations.opentelemetry)** — `tracing` is bus middleware that opens a span around each emit and handler call. It carries the trace context in message headers, so one trace follows an event from the request that emitted it, through RabbitMQ or the outbox, into its handler.
- **[RabbitMQ](../../apidocs/stratae.integrations/stratae.integrations.rabbitmq)** — `RabbitMQPublisher` and `RabbitMQConsumer` implement `stratae.events`' bus protocols over AMQP, including automatic `Envelope` correlation across the wire.

Databases need no integration: [A database session per scope](../database.md) shows the recipe, and how it lines up with `scoped_route` and `RabbitMQConsumer`.

Full reference: {doc}`stratae.integrations API reference <../../apidocs/stratae.integrations/stratae.integrations>`.
