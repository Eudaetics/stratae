"""
Serialization and deserialization tools for encoding/decoding data.

{py:func}`pack` turns a payload into bytes, using {py:func}`encode` to
convert individual fields that aren't natively JSON-serializable (UUIDs,
datetimes, Decimals, or objects exposing `to_dict`/`model_dump`).
{py:class}`Unpacker` is the structural counterpart: a type-directed
deserializer shaped like `msgspec.json.decode(data, type=T)`, with
{py:func}`unpack_json` as the default, dependency-free implementation.
Register additional types with `@encode.register` or `@pack.register` as
needed; see {py:mod}`stratae.integrations.msgspec` for a faster `pack`
registered for `msgspec.Struct` payloads.

```{rubric} Example:
```
```{code-block} python
:caption: Round-tripping a dataclass through the default pack/unpack pair

from dataclasses import asdict, dataclass
from uuid import UUID, uuid4
from stratae.serde import pack, unpack_json

@dataclass
class Widget:
    id: UUID
    name: str

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

    def to_dict(self):
        return asdict(self)

widget = Widget(id=uuid4(), name="sprocket")
data = pack(widget)
assert data == f'{{"id": "{widget.id}", "name": "sprocket"}}'.encode()

restored = unpack_json(data, type=Widget)
assert restored == widget
```

See {py:func}`encode`, {py:func}`pack`, {py:class}`Unpacker`, and
{py:func}`unpack_json` for additional examples.

"""

import json
from datetime import datetime
from decimal import Decimal
from functools import singledispatch
from typing import Any, Protocol
from uuid import UUID

__all__ = ["encode", "pack", "Unpacker", "unpack_json"]


@singledispatch
def encode(obj: object) -> Any:
    """
    Encode a field value for serialization.

    Falls back to `to_dict()` or `model_dump()` if present on `obj`, covering
    plain dataclass-like and Pydantic-style objects without a registered
    encoder. Uses `functools.singledispatch`; register additional types via
    `@encode.register`. Pre-registered for `UUID` ({py:func}`encode_uuid`),
    `datetime` ({py:func}`encode_datetime`), and `Decimal`
    ({py:func}`encode_decimal`). Used as the `default` hook by
    {py:func}`pack`.

    :param obj: The value to encode.
    :returns: A JSON-serializable representation of `obj`.
    :raises TypeError: If `obj` has no registered encoder and no `to_dict` or
        `model_dump` method.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Falling back to a to_dict() method for an unregistered type

    from stratae.serde import encode

    class Point:
        def to_dict(self):
            return {"x": 1, "y": 2}

    assert encode(Point()) == {"x": 1, "y": 2}
    ```

    """
    if to_dict := getattr(obj, "to_dict", None):
        return to_dict()
    if model_dump := getattr(obj, "model_dump", None):
        return model_dump()
    raise TypeError(f"Object of type {type(obj)} is not encodable")


@encode.register
def encode_uuid(obj: UUID) -> str:
    """
    Encode a `UUID` as its string representation.

    Pre-registered on {py:func}`encode` as a common default. Overwrite it
    with `@encode.register` if this doesn't match your requirements.

    :param obj: The UUID to encode.
    :returns: The string form of `obj`.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Encoding a UUID field as a str

    from uuid import UUID
    from stratae.serde import encode

    value = UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6")
    assert encode(value) == "3fa85f64-5717-4562-b3fc-2c963f66afa6"
    ```

    """
    return str(obj)


@encode.register
def encode_datetime(obj: datetime) -> str:
    """
    Encode a `datetime` as an ISO 8601 string.

    Pre-registered on {py:func}`encode` as a common default. Overwrite it
    with `@encode.register` if this doesn't match your requirements.

    :param obj: The datetime to encode.
    :returns: The ISO 8601 string form of `obj`.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Encoding a datetime field as a string using ISO date format

    from datetime import datetime, timezone
    from stratae.serde import encode

    value = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert encode(value) == "2024-01-01T00:00:00+00:00"
    ```

    """
    return obj.isoformat()


@encode.register
def encode_decimal(obj: Decimal) -> str:
    """
    Encode a `Decimal` as its string form.

    Pre-registered on {py:func}`encode` as a common default. Overwrite it
    with `@encode.register` if this doesn't match your requirements.

    :param obj: The decimal to encode.
    :returns: The string form of `obj`.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Encoding a Decimal field, e.g. a currency amount

    from decimal import Decimal
    from stratae.serde import encode

    assert encode(Decimal("9.99")) == "9.99"
    ```

    """
    return str(obj)


@singledispatch
def pack(obj: object) -> bytes:
    """
    Serialize a payload to bytes.

    Falls back to `json.dumps`, using {py:func}`encode` as the field-level
    hook for types not natively serializable by `json`. Register faster or
    format-specific paths for particular payload types via `@pack.register`;
    see {py:mod}`stratae.integrations.msgspec` for a registered path
    that uses `msgspec.json.encode` for `msgspec.Struct` payloads.

    :param obj: The payload to serialize.
    :returns: The serialized payload as bytes.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Serializing a plain JSON-compatible payload

    from stratae.serde import pack

    assert pack({"id": 1, "name": "widget"}) == b'{"id": 1, "name": "widget"}'
    ```

    """
    return json.dumps(obj, default=encode).encode()


class Unpacker(Protocol):
    """
    Structural protocol for a type-directed deserializer.

    Shaped after the call signature of `msgspec.json.decode(data, type=T)`,
    so adapters for other tools can be written as lambdas or thin wrappers
    around that tool's own decode function. {py:func}`unpack_json` is the
    default, dependency-free implementation.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Adapting msgspec's decoder to the Unpacker protocol

    import msgspec

    from stratae.serde import Unpacker

    unpacker: Unpacker = msgspec.json.decode
    assert unpacker(b'{"x": 1}', type=dict) == {"x": 1}
    ```

    """

    def __call__[S](self, data: bytes, /, *, type: type[S]) -> S:
        """
        Deserialize `data` into an instance of `type`.

        :param data: The raw bytes to decode.
        :param type: The type to reconstruct.
        :returns: The reconstructed `type` instance.
        """
        ...


def unpack_json[S](data: bytes, /, *, type: type[S]) -> S:
    """
    Deserialize JSON bytes by constructing `type` from keyword arguments.

    The default {py:class}`Unpacker` — the inverse of {py:func}`pack`'s
    default — covering plain keyword-constructible classes and dataclasses.

    :param data: The raw JSON bytes to decode.
    :param type: The type to reconstruct.
    :returns: The reconstructed `type` instance.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Reconstructing a dataclass from JSON bytes

    from dataclasses import dataclass

    from stratae.serde import unpack_json


    @dataclass
    class Point:
        x: int
        y: int


    assert unpack_json(b'{"x": 1, "y": 2}', type=Point) == Point(x=1, y=2)
    ```

    """
    fields: dict[str, Any] = json.loads(data)
    return type(**fields)
