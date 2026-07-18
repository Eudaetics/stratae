"""Byte-decoding entrypoint for deserializing whole payloads."""

import json
from typing import Any, Protocol


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

    The default ``Unpacker`` (the inverse of ``pack``'s default) covering
    plain keyword-constructible classes and dataclasses.

    Args:
        data: The raw JSON bytes to decode.
        type: The type to reconstruct.

    Returns:
        The reconstructed ``type`` instance.

    """
    fields: dict[str, Any] = json.loads(data)
    return type(**fields)
