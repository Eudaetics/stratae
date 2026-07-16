"""Byte-decoding entrypoint for deserializing whole payloads."""

import json
from typing import Any, Callable, Protocol, runtime_checkable


@runtime_checkable
class Unpacker(Protocol):
    """
    Structural protocol for a type-directed deserializer.

    Captures the call shape of ``unpack`` — a target type and raw bytes in,
    an instance of that type out — so adapters can accept any compatible
    deserializer where ``unpack`` is the default.
    """

    def __call__[S: Any](self, payload_type: type[S], data: bytes) -> S:
        """
        Deserialize ``data`` into an instance of ``payload_type``.

        Args:
            payload_type: The type to reconstruct.
            data:         The raw bytes to decode.

        Returns:
            The reconstructed ``payload_type`` instance.

        """
        ...


class _Unpack:
    """
    Type-directed deserializer dispatching on the target class itself.

    Mirrors the shape of ``msgspec.json.decode(data, type=T)``, generalized
    across model libraries: decoders are registered against a base class,
    and lookup walks the target type's MRO to find the most specific
    registered base — so a decoder registered for ``msgspec.Struct`` handles
    every struct subclass.  Unregistered types fall back to ``json.loads``
    and calling the type with the decoded mapping as keyword arguments,
    which covers plain keyword-constructible classes and dataclasses.

    ``functools.singledispatch`` is deliberately not used here: it dispatches
    on ``type(first_arg)``, and when the first argument is itself a class
    that means dispatching on its metaclass — the wrong axis, since most
    types share ``type`` as their metaclass and could never be distinguished.
    """

    __slots__ = ("_cache", "_decoders")

    def __init__(self) -> None:
        """Initialise the decoder registry and MRO-lookup cache."""
        self._decoders: dict[type[Any], Callable[[type[Any], bytes], Any]] = {}
        self._cache: dict[type[Any], Callable[[type[Any], bytes], Any] | None] = {}

    def register[S: Any](self, base: type[S], decoder: Callable[[type[S], bytes], S]) -> None:
        """
        Register a decoder for a base class and all of its subclasses.

        Args:
            base:    The base class the decoder handles; lookup selects the
                     decoder whose base appears earliest in the target
                     type's MRO.
            decoder: Decodes bytes into an instance of the given target
                     type, which is always ``base`` or a subclass of it.

        """
        self._decoders[base] = decoder
        self._cache.clear()

    def __call__[S: Any](self, payload_type: type[S], data: bytes) -> S:
        """
        Deserialize bytes into an instance of ``payload_type``.

        Args:
            payload_type: The type to reconstruct.  Must be a plain class;
                          subscripted generic aliases are not supported.
            data:         The raw bytes to decode.

        Returns:
            The reconstructed ``payload_type`` instance.

        """
        decoder = self._lookup(payload_type)
        if decoder is not None:
            return decoder(payload_type, data)
        fields: dict[str, Any] = json.loads(data)
        return payload_type(**fields)

    def _lookup(self, payload_type: type[Any]) -> Callable[[type[Any], bytes], Any] | None:
        if payload_type in self._cache:
            return self._cache[payload_type]
        decoder: Callable[[type[Any], bytes], Any] | None = None
        for base in payload_type.__mro__:
            found = self._decoders.get(base)
            if found is not None:
                decoder = found
                break
        self._cache[payload_type] = decoder
        return decoder


unpack = _Unpack()
"""
Deserialize bytes into an instance of a target type.

The default ``Unpacker``: integrations register decode paths for their base
classes via ``unpack.register`` (e.g. ``stratae.integrations.serde.msgspec``
registers ``msgspec.Struct``), and unregistered types fall back to JSON
keyword construction — the inverse of ``pack``'s default.
"""
