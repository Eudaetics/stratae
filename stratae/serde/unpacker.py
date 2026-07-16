"""Byte-decoding entrypoint for deserializing whole payloads."""

from typing import Protocol


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
