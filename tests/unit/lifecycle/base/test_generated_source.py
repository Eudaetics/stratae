"""
Snapshot tests for the code that cache wrappers generate.

Each case caches one kind of callable in one kind of scope, and checks the wrapper's generated
source against `snapshots/<case>.txt`. Together the cases run every branch of the generator,
so any change to the generated code changes a snapshot. Record changed snapshots with
`STRATAE_UPDATE_SNAPSHOTS=1`, and review them like code.

This test suite verifies the following behaviors:

Generated source:
- Each case's wrapper source matches its snapshot. The cases cover sync and async functions,
  generator, async generator and decorated generator functions; slot and keyed wrappers,
  including single-parameter, `**kwargs`-only and `cache_key` keys; shared, context and
  `threads=True` scopes; dense and sparse storage; and owned, reused slots.

Tracebacks:
- A traceback through a cache wrapper shows the wrapper's source line.
"""

import asyncio
import functools
import gc
import inspect
import traceback
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Any

import pytest

from stratae.lifecycle import AsyncScope, Scope


def _plain() -> int:
    return 1


def _keyed(a: int, b: int = 2, *args: int, c: int, d: int = 4, **kw: int) -> int:
    return a


def _one(a: int) -> int:
    return a


def _kwargs_only(**kw: int) -> dict[str, int]:
    return kw


async def _async_plain() -> int:
    await asyncio.sleep(0)
    return 1


async def _async_keyed(a: int, b: int = 2) -> int:
    await asyncio.sleep(0)
    return a


def _generator() -> Generator[int]:
    yield 1


async def _async_generator() -> AsyncGenerator[int]:
    await asyncio.sleep(0)
    yield 1


def _hiding(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Hide `fn` behind a decorator that records it, as `functools.wraps` does."""

    @functools.wraps(fn)
    def inner(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    return inner


def _key(*args: Any, **kwargs: Any) -> int:
    return 1


_SCOPES: dict[str, Callable[[], Scope | AsyncScope]] = {
    "sync-context": lambda: Scope("s"),
    "sync-sparse": lambda: Scope("s", storage="sparse"),
    "sync-shared": lambda: Scope("s", isolation="shared"),
    "sync-shared-sparse": lambda: Scope("s", isolation="shared", storage="sparse"),
    "sync-threads": lambda: Scope("s", threads=True),
    "async-context": lambda: AsyncScope("s"),
    "async-shared": lambda: AsyncScope("s", isolation="shared"),
    "async-threads": lambda: AsyncScope("s", threads=True),
}

_CASES: list[tuple[str, str, Callable[..., Any], dict[str, Any], bool]] = [
    # name, scope, callable, cache options, reuse a freed slot
    ("slot-context", "sync-context", _plain, {}, False),
    ("slot-context-owned", "sync-context", _plain, {}, True),
    ("slot-sparse", "sync-sparse", _plain, {}, False),
    ("slot-sparse-owned", "sync-sparse", _plain, {}, True),
    ("slot-shared", "sync-shared", _plain, {}, False),
    ("slot-shared-reused", "sync-shared", _plain, {}, True),
    ("slot-shared-sparse", "sync-shared-sparse", _plain, {}, False),
    ("slot-threads", "sync-threads", _plain, {}, False),
    ("slot-ignore-params", "sync-context", _keyed, {"ignore_params": True}, False),
    ("keyed-context", "sync-context", _keyed, {}, False),
    ("keyed-context-owned", "sync-context", _keyed, {}, True),
    ("keyed-shared", "sync-shared", _keyed, {}, False),
    ("keyed-threads", "sync-threads", _keyed, {}, False),
    ("keyed-one-param", "sync-context", _one, {}, False),
    ("keyed-kwargs-only", "sync-context", _kwargs_only, {}, False),
    ("keyed-cache-key", "sync-context", _keyed, {"cache_key": _key}, False),
    ("async-slot-context", "async-context", _async_plain, {}, False),
    ("async-slot-context-owned", "async-context", _async_plain, {}, True),
    ("async-slot-shared", "async-shared", _async_plain, {}, False),
    ("async-slot-threads", "async-threads", _async_plain, {}, False),
    ("async-keyed-context", "async-context", _async_keyed, {}, False),
    ("async-keyed-context-owned", "async-context", _async_keyed, {}, True),
    ("async-keyed-shared", "async-shared", _async_keyed, {}, False),
    ("async-keyed-threads", "async-threads", _async_keyed, {}, False),
    ("sync-in-async-scope", "async-context", _plain, {}, False),
    ("generator", "sync-context", _generator, {}, False),
    ("generator-in-async-scope", "async-context", _generator, {}, False),
    ("async-generator", "async-context", _async_generator, {}, False),
    ("decorated-generator", "sync-context", _hiding(_generator), {}, False),
    ("decorated-generator-in-async-scope", "async-context", _hiding(_generator), {}, False),
    ("decorated-async-generator", "async-context", _hiding(_async_generator), {}, False),
]


def _cache(scope: Scope | AsyncScope, fn: Callable[..., Any], options: dict[str, Any]) -> Any:
    """Cache `fn` in `scope`, whichever kind of scope it is."""
    return scope.cache(**options)(fn)


@pytest.mark.parametrize(
    ("name", "scope_kind", "fn", "options", "reuse"), _CASES, ids=[case[0] for case in _CASES]
)
def test_generated_source_matches_snapshot(
    snapshot: Callable[[str, str], None],
    name: str,
    scope_kind: str,
    fn: Callable[..., Any],
    options: dict[str, Any],
    reuse: bool,
):
    """
    A cache wrapper's generated source should match its snapshot.

    Given: A new scope of the case's kind, with a freed slot to reuse if the case asks
    When: The case's callable is cached in it
    Then: The wrapper's generated source matches the case's snapshot
    """
    # Arrange
    scope = _SCOPES[scope_kind]()
    if reuse:
        freed = _cache(scope, _plain, {})
        del freed
        gc.collect()

    # Act
    wrapper = _cache(scope, fn, options)

    # Assert
    snapshot(name, inspect.getsource(wrapper.__code__))


def test_traceback_shows_cache_wrapper_source():
    """
    A traceback through a cache wrapper should show the wrapper's source line.

    Given: A cached function that raises, in an active scope
    When: It's called and the traceback formatted
    Then: The traceback names the function and shows the wrapper's call to it
    """
    # Arrange
    scope = Scope("s")

    @scope.cache()
    def failing() -> int:
        raise ValueError("failed")

    # Act
    with scope, pytest.raises(ValueError, match="failed") as raised:
        failing()

    # Assert
    formatted = "".join(traceback.format_exception(raised.value))
    assert "failing#" in formatted
    assert "__value__ = __func__()" in formatted
