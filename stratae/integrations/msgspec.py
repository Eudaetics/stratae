"""Msgspec integration for serialization and deserialization."""

import msgspec

from stratae.serde import encode, pack


@pack.register
def _(obj: msgspec.Struct) -> bytes:
    return msgspec.json.encode(obj, enc_hook=encode)
