# Serde

`stratae.serde` is a small, dependency-free set of primitives for turning arbitrary Python objects into bytes and back — `pack`, `encode`, and `unpack_json`. It's deliberately minimal: enough to round-trip DTOs across a serialization boundary (an HTTP response, a queue message, a cache) without hand-writing a (de)serializer for every type.

## Packing objects

`pack(obj)` is the entry point — by default, `json.dumps(obj, default=encode).encode()`. `encode` is the field-level hook used for anything `json.dumps` doesn't already know how to handle: it has built-in support for `UUID`, `datetime`, and `Decimal`, and falls back to calling `obj.to_dict()` or `obj.model_dump()` if either exists — which means most dataclasses and Pydantic-style models serialize with no registration at all.

```python
from dataclasses import dataclass, asdict
from uuid import UUID, uuid4
from stratae.serde import pack, unpack_json

@dataclass
class Widget:
    id: UUID
    name: str

    def to_dict(self) -> dict:
        return asdict(self)

    def __post_init__(self) -> None:
        if isinstance(self.id, str):
            self.id = UUID(self.id)

widget = Widget(id=uuid4(), name="sprocket")
data = pack(widget)  # b'{"id": "...", "name": "sprocket"}'
restored = unpack_json(data, type=Widget)
assert restored == widget
```

`unpack_json` is the structural inverse for the easy direction: `type(**json.loads(data))`. It's intentionally naive — no nested-type coercion happens automatically, which is why `Widget.__post_init__` above turns the `id` field back into a `UUID` itself. `pack`/`encode` handle the hard direction (arbitrary object → JSON); getting values back into the right types on the way in is the receiving type's job, via its constructor.

## Extending encode and pack

Both `encode` and `pack` are `functools.singledispatch` functions — register a type-specific implementation the same way you would for any singledispatch function:

```python
from stratae.serde import encode

@encode.register
def _(obj: Money) -> str:
    return f"{obj.amount:.2f}"
```

`stratae.integrations.msgspec` uses exactly this mechanism to register a `msgspec.Struct`-specific fast path onto `pack` — see the [msgspec integration guide](integrations/msgspec) for that pattern, and the gotcha that comes with it: the registration only takes effect once that module has actually been imported somewhere.

## `Unpacker` as a protocol

`unpack_json` is just the default implementation of a small structural protocol, `Unpacker` — `__call__(data: bytes, *, type: type[S]) -> S`. It's shaped to match `msgspec.json.decode`'s signature exactly, so a schema-aware decoder like msgspec's can be dropped in as a drop-in replacement anywhere `stratae.serde` expects an `Unpacker`, with no adapter code needed.

Full signatures: {doc}`stratae.serde API reference <../apidocs/stratae.serde/stratae.serde>`.
