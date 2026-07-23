# msgspec

`stratae.integrations.msgspec` is a single-function module whose entire job is a side-effecting registration: it registers a `msgspec.Struct`-specific fast path onto `stratae.serde.pack`, using `msgspec.json.encode` in place of the default `json.dumps`-based path, and wires `stratae.serde.encode` in as msgspec's `enc_hook` so fields msgspec doesn't natively know how to serialize (`UUID`, `datetime`, `Decimal`, or anything with `to_dict`/`model_dump`) still get handled.

```python
import stratae.integrations.msgspec  # noqa: F401 -- registers the pack fast path
from stratae.serde import pack

result = pack(some_msgspec_struct)  # now goes through msgspec.json.encode
```

The import is not optional. Because `pack` is a `functools.singledispatch` function, the registration only takes effect once this module has actually been imported somewhere in the process — having `msgspec` installed is not enough. Forget the import and `pack(some_msgspec_struct)` still "works": it silently falls back to the generic JSON path, with no error and no fast path. That's the one gotcha worth remembering here — the fallback doesn't announce itself.

Pairs naturally with a per-binding `serializer` override on `stratae.integrations.rabbitmq.RabbitMQPublisher`, to use the fast path for specific event bindings without changing the adapter's default:

```python
from stratae.serde import pack

place_order = publisher.bind(order_placed, config=config, serializer=pack)
```

Full signatures: {doc}`stratae.integrations.msgspec API reference <../../apidocs/stratae.integrations/stratae.integrations.msgspec>`.
