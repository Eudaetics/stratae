"""Serialization and deserialization tools for encoding/decoding data."""

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

    Checks for common serialization methods before raising. Register
    additional types via ``@encode.register``.
    """
    if to_dict := getattr(obj, "to_dict", None):
        return to_dict()
    if model_dump := getattr(obj, "model_dump", None):
        return model_dump()
    raise TypeError(f"Object of type {type(obj)} is not encodable")


@encode.register
def encode_uuid(obj: UUID) -> str:
    return str(obj)


@encode.register
def encode_datetime(obj: datetime) -> str:
    return obj.isoformat()


@encode.register
def encode_decimal(obj: Decimal) -> str:
    return str(obj)


@singledispatch
def pack(obj: object) -> bytes:
    """
    Serialize a payload to bytes.

    Falls back to ``json.dumps`` with ``encode`` as the field-level hook for
    types not natively serializable by ``json``. Register faster or
    format-specific paths for particular payload types via ``@pack.register``.
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

    """
    fields: dict[str, Any] = json.loads(data)
    return type(**fields)
