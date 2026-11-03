# Integrations

`stratae.integrations` bridges the core modules to specific third-party tools. Each integration is a small, focused module — importing it is what activates the bridge; nothing here changes how the core modules behave on their own.

- **[msgspec](../../apidocs/stratae.integrations/stratae.integrations.msgspec)** — registers a `msgspec.Struct`-specific fast path onto `stratae.serde.serialize`.
- **[RabbitMQ](../../apidocs/stratae.integrations/stratae.integrations.rabbitmq)** — `RabbitMQPublisher` and `RabbitMQConsumer` implement `stratae.events`' bus protocols over AMQP, including automatic `Envelope` correlation across the wire.

Databases need no integration: [A database session per scope](../database.md) shows the recipe, and how it lines up with `scoped_route` and `RabbitMQConsumer`.

Full reference: {doc}`stratae.integrations API reference <../../apidocs/stratae.integrations/stratae.integrations>`.
