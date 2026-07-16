"""Simple protocol tests for unpack."""

from typing import Any

import pytest

from stratae.serde import Unpacker


def unpack_with(unpacker: Unpacker, data: bytes, payload_type: type[Any]) -> Any:
    """Call through the protocol exactly the way an adapter would."""
    return unpacker(data, type=payload_type)


def test_satisfies_unpacker_protocol():
    """A callable with the protocol's shape binds and runs through it."""

    def compatible(data: bytes, /, type: type[Any]) -> Any:
        return type(data)

    assert unpack_with(compatible, b"payload", bytes) == b"payload"


def test_fails_protocol():
    """A callable without the protocol's shape fails to bind at call time."""

    def incompatible() -> str:
        return "test"

    unpacker: Any = incompatible
    with pytest.raises(TypeError):
        unpack_with(unpacker, b"payload", bytes)
