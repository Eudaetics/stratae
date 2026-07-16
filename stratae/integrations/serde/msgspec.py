"""Msgspec integration for serialization and deserialization."""

import msgspec

from stratae.serde import encode, pack, unpack


@pack.register
def _(obj: msgspec.Struct) -> bytes:
    return msgspec.json.encode(obj, enc_hook=encode)


def _unpack_struct[S: msgspec.Struct](payload_type: type[S], data: bytes) -> S:
    return msgspec.json.decode(data, type=payload_type)


unpack.register(msgspec.Struct, _unpack_struct)
