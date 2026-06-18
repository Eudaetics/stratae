"""Byte-encoding entrypoint for serializing whole payloads."""

import json
from functools import singledispatch
from typing import Any

from stratae.serde import encode


@singledispatch
def pack(obj: Any) -> bytes:
    """
    Serialize a payload to bytes.

    Falls back to ``json.dumps`` with ``encode`` as the field-level hook for
    types not natively serializable by ``json``. Register faster or
    format-specific paths for particular payload types via ``@pack.register``.
    """
    return json.dumps(obj, default=encode).encode()
