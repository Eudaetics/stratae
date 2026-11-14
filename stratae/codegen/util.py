"""Miscellaneous helpers for codegen'd wrappers."""

import linecache
import weakref
from collections.abc import Iterable
from itertools import count
from typing import Any

from .lines import Lines, block

_serial = count(1)
"""Numbers each wrapper filename, so wrappers of the same object never share one."""


def wrapper_filename(obj: Any) -> str:
    """
    Create a filename for a wrapper of `obj`, naming `obj` for tracebacks.

    Each call returns a new filename, so every wrapper's source is kept apart, even when the
    same object is wrapped more than once.
    """
    qualname = getattr(obj, "__qualname__", None) or getattr(obj, "__name__", None) or repr(obj)
    module = getattr(obj, "__module__", "?")
    return f"<stratae: {module}.{qualname}#{next(_serial)}>"


def compile_wrapper(lines: Lines, target: Any, namespace: dict[str, Any]) -> Any:
    """
    Compile `lines`, which define a function named `wrapper`, and return that function.

    The source is registered with `linecache` under a filename naming `target`. Tracebacks and
    debuggers show its lines, and `inspect.getsource(wrapper.__code__)` returns it. It's
    dropped once the wrapper is garbage collected.

    :param lines: The wrapper's source.
    :param target: The object being wrapped, named in the filename.
    :param namespace: The globals the source runs in. The wrapper is defined into it.
    :returns: The compiled wrapper.
    """
    source = "\n".join(lines)
    filename = wrapper_filename(target)
    exec(compile(source, filename, "exec"), namespace)  # noqa: S102
    wrapper = namespace["wrapper"]
    linecache.cache[filename] = (len(source), None, source.splitlines(keepends=True), filename)
    weakref.finalize(wrapper, linecache.cache.pop, filename, None)
    return wrapper


def def_wrapper(parameters: str, body: Iterable[str]) -> Lines:
    """Define the function {py:func}`compile_wrapper` compiles, taking `parameters`."""
    return block(f"def wrapper({parameters}):", body)


def async_def_wrapper(parameters: str, body: Iterable[str]) -> Lines:
    """Define {py:func}`compile_wrapper`'s function as a coroutine function."""
    return block(f"async def wrapper({parameters}):", body)
