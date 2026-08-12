"""
Register a faster serialize path for msgspec.Struct payloads.

Registers a `msgspec.Struct`-specific implementation of {py:func}`serialize
<stratae.serde.serialize>` that uses `msgspec.json.encode` in place of
`json.dumps`. {py:func}`encode <stratae.serde.encode>` is wired in as
`msgspec`'s `enc_hook`. Whatever `msgspec` doesn't already know how to
encode natively falls through to the same handling the default `serialize`
uses. That includes anything registered via `@encode.register`.

The registration only takes effect once this module has actually been
imported, since {py:func}`serialize <stratae.serde.serialize>` is a
`functools.singledispatch` function and only dispatches to implementations
from modules the interpreter has run. Import `stratae.integrations.msgspec`
once, for its side effect, before serializing any `msgspec.Struct`.
Installing `msgspec` itself is not enough. Without that import, `serialize`
still runs on a `msgspec.Struct` by falling back to its generic
`json.dumps`-based path, so the fallback can go unnoticed rather than
failing loudly.

The decode side has the same symmetry: `deserialize.construct` has the
same `(type, obj) -> Any` shape as msgspec's `dec_hook`, so passing it
directly as `dec_hook` covers `msgspec.Struct` fields the same way
`encode` already covers them as `enc_hook`.

````{example} Serializing a msgspec.Struct with a field msgspec can't natively encode
```{code-block} python
import msgspec
import stratae.integrations.msgspec  # noqa: F401
from stratae.serde import deserialize, encode, serialize

class Amount:
    def __init__(self, value: float, currency: str) -> None:
        self.value = value
        self.currency = currency

@encode.register
def encode_amount(obj: Amount) -> dict[str, object]:
    return {"value": obj.value, "currency": obj.currency}

@deserialize.register(Amount)
def decode_amount(value: dict[str, object]) -> Amount:
    return Amount(**value)

class OrderPlaced(msgspec.Struct):
    order_id: int
    total: Amount

order = OrderPlaced(order_id=42, total=Amount(value=19.99, currency="usd"))
data = serialize(order)
print(data.decode())

restored = msgspec.json.decode(data, type=OrderPlaced, dec_hook=deserialize.construct)
print(restored.total.value, restored.total.currency)
```
```{output}
{"order_id":42,"total":{"value":19.99,"currency":"usd"}}
19.99 usd
```
````

See {py:func}`serialize <stratae.serde.serialize>` and {py:func}`encode
<stratae.serde.encode>` for the rest of the module's API.

"""

import msgspec

from stratae.serde import encode, serialize


@serialize.register
def _(obj: msgspec.Struct) -> bytes:
    """Serialize a msgspec.Struct to JSON bytes, using stratae's encode for non-native types."""
    return msgspec.json.encode(obj, enc_hook=encode)
