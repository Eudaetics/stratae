"""
Serialization and deserialization tools for encoding/decoding data.

`pack` turns a payload into bytes, using `encode` to convert individual
fields that aren't natively JSON-serializable (UUIDs, datetimes, Decimals,
or objects exposing `to_dict`/`model_dump`). `Unpacker` is the structural
counterpart: a type-directed deserializer shaped like
``msgspec.json.decode(data, type=T)``, with `unpack_json` as the default,
dependency-free implementation. Register additional types with
`@encode.register` or `@pack.register` as needed.

Examples:
    Round-tripping a dataclass through the default pack/unpack pair:

    .. code-block:: python

        from dataclasses import dataclass
        from uuid import UUID, uuid4

        from stratae.serde import pack, unpack_json

        @dataclass
        class Widget:
            id: UUID
            name: str

        widget = Widget(id=uuid4(), name="sprocket")
        data = pack(widget)  # b'{"id": "...", "name": "sprocket"}'
        unpack_json(data, type=dict)  # {'id': '...', 'name': 'sprocket'}

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

    Checks for common serialization methods before raising. `encode` uses
    `singledispatch`. Register additional types via ``@encode.register``.

    Args:
        obj: The value to encode.

    Returns:
        A JSON-serializable representation of `obj`.

    Raises:
        TypeError: If `obj` has no registered encoder and no `to_dict` or
            `model_dump` method.

    Examples:
        .. code-block:: python

            from uuid import uuid4

            from stratae.serde import encode

            encode(uuid4())  # '3fa85f64-5717-4562-b3fc-2c963f66afa6'

            class Point:
                def to_dict(self):
                    return {"x": 1, "y": 2}

            encode(Point())  # {'x': 1, 'y': 2}

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

    Pre-registered as a common helper. If this doesn't match your
    requirements, overwrite it with ``@encode.register``.
    """
    return str(obj)


@encode.register
def encode_datetime(obj: datetime) -> str:
    """
    Encode a `datetime` as an ISO 8601 string.

    Pre-registered as a common helper. If this doesn't match your
    requirements, overwrite it with ``@encode.register``.
    """
    return obj.isoformat()


@encode.register
def encode_decimal(obj: Decimal) -> str:
    """
    Encode a `Decimal` as its string form.

    Pre-registered as a common helper. If this doesn't match your
    requirements, overwrite it with ``@encode.register``.
    """
    return str(obj)


@singledispatch
def pack(obj: object) -> bytes:
    """
    Serialize a payload to bytes.

    Falls back to ``json.dumps`` with ``encode`` as the field-level hook for
    types not natively serializable by ``json``. Register faster or
    format-specific paths for particular payload types via ``@pack.register``.

    Args:
        obj: The payload to serialize.

    Returns:
        The serialized payload as bytes.

    Examples:
        .. code-block:: python

            from stratae.serde import pack

            pack({"id": 1, "name": "widget"})  # b'{"id": 1, "name": "widget"}'

    """
    return json.dumps(obj, default=encode).encode()


class Unpacker(Protocol):
    """
    Structural protocol for a type-directed deserializer.

    Based on the call shape of ``msgspec.json.decode(data, type=T)``,
    adapters for other common tools can be implemented through lambdas
    or wrappers.
    """

    def __call__[S](self, data: bytes, /, *, type: type[S]) -> S:
        """
        Deserialize ``data`` into an instance of ``type``.

        Args:
            data: The raw bytes to decode.
            type: The type to reconstruct.

        Returns:
            The reconstructed ``type`` instance.

        Examples:
            .. code-block:: python

                import msgspec

                from stratae.serde import Unpacker

                unpacker: Unpacker = msgspec.json.decode
                unpacker(b'{"x": 1}', type=dict)  # {'x': 1}

        """
        ...


def unpack_json[S](data: bytes, /, *, type: type[S]) -> S:
    """
    Deserialize JSON bytes by constructing ``type`` from keyword arguments.

    The default ``Unpacker`` — the inverse of ``pack``'s default — covering
    plain keyword-constructible classes and dataclasses.

    Args:
        data: The raw JSON bytes to decode.
        type: The type to reconstruct.

    Returns:
        The reconstructed ``type`` instance.

    Examples:
        .. code-block:: python

            from dataclasses import dataclass

            from stratae.serde import unpack_json

            @dataclass
            class Point:
                x: int
                y: int

            unpack_json(b'{"x": 1, "y": 2}', type=Point)  # Point(x=1, y=2)

    """
    fields: dict[str, Any] = json.loads(data)
    return type(**fields)
