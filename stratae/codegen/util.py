"""Miscellaneous helpers for codegen'd wrappers."""

from typing import Any


def wrapper_filename(obj: Any) -> str:
    """Create a filename for the wrapper based on the underlying function."""
    qualname = getattr(obj, "__qualname__", None) or getattr(obj, "__name__", None) or repr(obj)
    module = getattr(obj, "__module__", "?")
    return f"<stratae: {module}.{qualname}@{id(obj):#x}>"
