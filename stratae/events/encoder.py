"""Field-level encoding for non-natively-serializable types."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import singledispatch
from typing import Any
from uuid import UUID


@singledispatch
def encode(obj: Any) -> Any:
    """
    Encode a field value for serialization.

    Checks for common serialization methods before raising. Register
    additional types via ``@encode.register``.
    """
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    raise TypeError(f"Object of type {type(obj)} is not encodable")


@encode.register
def _(obj: UUID) -> str:
    return str(obj)


@encode.register
def _(obj: datetime) -> str:
    return obj.isoformat()


@encode.register
def _(obj: Decimal) -> str:
    return str(obj)


@encode.register
def _(obj: Enum) -> Any:
    return obj.value
