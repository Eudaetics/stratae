"""
Register a faster pack path for msgspec.Struct payloads.

Registers a `msgspec.Struct`-specific implementation of {py:func}`pack
<stratae.serde.pack>` that uses `msgspec.json.encode` in place of
`json.dumps`. {py:func}`encode <stratae.serde.encode>` is wired in as
`msgspec`'s `enc_hook`. Struct fields that aren't natively serializable by
`msgspec` (UUIDs, datetimes, Decimals, or objects exposing
`to_dict`/`model_dump`) are handled the same way as the default `pack`.

The registration only takes effect once this module has actually been
imported, since {py:func}`pack <stratae.serde.pack>` is a
`functools.singledispatch` function and only dispatches to implementations
from modules the interpreter has run. Import `stratae.integrations.msgspec`
once, for its side effect, before packing any `msgspec.Struct`. Installing
`msgspec` itself is not enough. Without that import, `pack` still runs on a
`msgspec.Struct` by falling back to its generic `json.dumps`-based path,
so the fallback can go unnoticed rather than failing loudly.

````{example} Packing a msgspec.Struct with a field msgspec can't natively encode
```{code-block} python
import msgspec
import stratae.integrations.msgspec  # noqa: F401 (registers the pack fast path)
from stratae.serde import pack

class Amount:
    def __init__(self, value: float, currency: str) -> None:
        self.value = value
        self.currency = currency

    def to_dict(self) -> dict[str, object]:
        return {"value": self.value, "currency": self.currency}

class OrderPlaced(msgspec.Struct):
    order_id: int
    total: Amount

def dec_hook(type, obj):
    if type is Amount:
        return Amount(value=obj["value"], currency=obj["currency"])
    raise TypeError(f"unsupported type: {type}")

order = OrderPlaced(order_id=42, total=Amount(value=19.99, currency="usd"))
data = pack(order)
print(data.decode())

restored = msgspec.json.decode(data, type=OrderPlaced, dec_hook=dec_hook)
print(restored.total.value, restored.total.currency)
```
```{output}
{"order_id":42,"total":{"value":19.99,"currency":"usd"}}
19.99 usd
```
````

See {py:func}`pack <stratae.serde.pack>` and {py:func}`encode
<stratae.serde.encode>` for the rest of the module's API.

"""

import msgspec

from stratae.serde import encode, pack


@pack.register
def _(obj: msgspec.Struct) -> bytes:
    """Serialize a msgspec.Struct to JSON bytes, using stratae's encode for non-native types."""
    return msgspec.json.encode(obj, enc_hook=encode)
