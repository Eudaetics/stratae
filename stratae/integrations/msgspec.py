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
once, for its side effect, before packing any `msgspec.Struct` — installing
`msgspec` itself is not enough. Without that import, `pack` still runs on a
`msgspec.Struct` by falling back to its generic `json.dumps`-based path,
so the fallback can go unnoticed rather than failing loudly.

```{rubric} Example:
```
```{code-block} python
:caption: Packing a msgspec.Struct uses the faster msgspec-based encoder

import msgspec
import stratae.integrations.msgspec  # noqa: F401 (registers the pack fast path)
from stratae.serde import pack

class Point(msgspec.Struct):
    x: int
    y: int

point = Point(x=1, y=2)
result = pack(point)
assert isinstance(result, bytes)
assert msgspec.json.decode(result, type=Point) == point
```

See {py:func}`pack <stratae.serde.pack>` and {py:func}`encode
<stratae.serde.encode>` for additional examples.

"""

import msgspec

from stratae.serde import encode, pack


@pack.register
def _(obj: msgspec.Struct) -> bytes:
    """Serialize a msgspec.Struct to JSON bytes, using stratae's encode for non-native types."""
    return msgspec.json.encode(obj, enc_hook=encode)
