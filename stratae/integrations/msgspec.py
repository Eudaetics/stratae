"""
Register a faster serialize path for msgspec.Struct payloads.

Importing this module registers a {py:func}`serialize <stratae.serde.serialize>` path that
uses `msgspec.json.encode`, with {py:func}`encode <stratae.serde.encode>` as its `enc_hook`.
Without the import, `serialize` silently falls back to `json.dumps`. For decoding, pass
`deserialize.construct` as msgspec's `dec_hook`.

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
"""

import msgspec

from stratae.serde import encode, serialize


@serialize.register
def _(obj: msgspec.Struct) -> bytes:
    """Serialize a msgspec.Struct to JSON bytes, using stratae's encode for non-native types."""
    return msgspec.json.encode(obj, enc_hook=encode)
