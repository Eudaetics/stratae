# Integrations

`stratae.integrations` bridges the core modules to specific third-party tools. Each integration is a small, focused module — importing it is what activates the bridge; nothing here changes how the core modules behave on their own.

```{toctree}
:maxdepth: 1

msgspec
rabbitmq
```

- **[msgspec](msgspec)** — registers a `msgspec.Struct`-specific fast path onto `stratae.serde.pack`.
- **[RabbitMQ](rabbitmq)** — `RabbitMQPublisher` and `RabbitMQConsumer` implement `stratae.events`' bus protocols over AMQP, including automatic `Envelope` correlation across the wire.

Full reference: {doc}`stratae.integrations API reference <../../apidocs/stratae.integrations/stratae.integrations>`.
