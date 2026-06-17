"""Msgspec integration to let Stratae use pack and encode automatically."""

import msgspec

from stratae.serial import encode, pack


@pack.register
def _(obj: msgspec.Struct) -> bytes:
    return msgspec.json.encode(obj, enc_hook=encode)
