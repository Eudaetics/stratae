"""
Tests for how a cache wrapper's parts are chosen and bound, without building a wrapper.

This test suite verifies the following behaviors:

Claims:
- A context scope's sync wrapper claims nothing, and its async wrapper claims by task.
- A `threads=True` or shared scope's wrapper claims behind a lock, sync or async.

Namespace:
- For every combination of parts, every name the source reads is bound by some part.
"""

import builtins
import dis
from collections.abc import Callable, Hashable
from itertools import product
from types import CodeType

import pytest

from stratae.codegen import read_signature
from stratae.lifecycle._slots import SlotStore
from stratae.lifecycle._wrappers import (
    _ASYNC,  # pyright: ignore[reportPrivateUsage]
    _AWAIT,  # pyright: ignore[reportPrivateUsage]
    _CALL,  # pyright: ignore[reportPrivateUsage]
    _CHECK_ASYNC,  # pyright: ignore[reportPrivateUsage]
    _CHECK_SYNC,  # pyright: ignore[reportPrivateUsage]
    _ENTER_ASYNC_GENERATOR,  # pyright: ignore[reportPrivateUsage]
    _ENTER_GENERATOR,  # pyright: ignore[reportPrivateUsage]
    _SYNC,  # pyright: ignore[reportPrivateUsage]
    _access,  # pyright: ignore[reportPrivateUsage]
    _claims,  # pyright: ignore[reportPrivateUsage]
    _key,  # pyright: ignore[reportPrivateUsage]
    _LockedClaims,  # pyright: ignore[reportPrivateUsage]
    _Miss,  # pyright: ignore[reportPrivateUsage]
    _Mode,  # pyright: ignore[reportPrivateUsage]
    _OwnedSlot,  # pyright: ignore[reportPrivateUsage]
    _Parts,  # pyright: ignore[reportPrivateUsage]
    _PlainSlot,  # pyright: ignore[reportPrivateUsage]
    _source,  # pyright: ignore[reportPrivateUsage]
    _TaskClaims,  # pyright: ignore[reportPrivateUsage]
    _Unclaimed,  # pyright: ignore[reportPrivateUsage]
)

_ISOLATIONS = {
    "context": {"shared": False, "threads": False},
    "threads": {"shared": False, "threads": True},
    "shared": {"shared": True, "threads": False},
}


def _store(isolation: str, dense: bool = True) -> SlotStore:
    return SlotStore("s", dense=dense, **_ISOLATIONS[isolation])


@pytest.mark.parametrize(
    ("isolation", "mode", "claims"),
    [
        ("context", _SYNC, _Unclaimed),
        ("context", _ASYNC, _TaskClaims),
        ("threads", _SYNC, _LockedClaims),
        ("threads", _ASYNC, _LockedClaims),
        ("shared", _SYNC, _LockedClaims),
        ("shared", _ASYNC, _LockedClaims),
    ],
)
def test_claims_follow_isolation_and_mode(isolation: str, mode: _Mode, claims: type):
    """
    A wrapper's claims should follow its scope's isolation, and whether it's async.

    Given: Each isolation, for a sync and an async wrapper.
    When: Choosing the claims.
    Then: Only a context scope goes without a lock, claiming nothing when sync and by task
        when async.
    """
    assert type(_claims(_store(isolation), mode)) is claims


def _one(a: int) -> int:
    return a


def _custom(a: int) -> int:
    return a


def _wrapper_code(source: str) -> CodeType:
    module = compile(source, "<test>", "exec")
    return next(c for c in module.co_consts if isinstance(c, CodeType) and c.co_name == "wrapper")


def _globals_read(code: CodeType) -> set[str]:
    names = {
        instruction.argval
        for instruction in dis.get_instructions(code)
        if instruction.opname == "LOAD_GLOBAL"
    }
    return names - set(dir(builtins))


_MISSES = [_CALL, _AWAIT, _ENTER_GENERATOR, _ENTER_ASYNC_GENERATOR, _CHECK_SYNC, _CHECK_ASYNC]
_KEYS: list[tuple[bool, Callable[..., Hashable] | None]] = [
    (True, None),
    (False, None),
    (False, _custom),
]
"""Each way of keeping values: one per activation, keyed by parameters, or by a custom key."""


@pytest.mark.parametrize(
    ("isolation", "dense", "miss", "keying", "owned"),
    list(product(_ISOLATIONS, (True, False), _MISSES, _KEYS, (True, False))),
)
def test_parts_bind_every_name_the_source_reads(
    isolation: str,
    dense: bool,
    miss: _Miss,
    keying: tuple[bool, Callable[..., Hashable] | None],
    owned: bool,
):
    """
    Every name a wrapper's source reads should be bound by one of its parts.

    Given: Each combination of isolation, storage, miss, keying and slot ownership.
    When: Rendering the source from those parts.
    Then: Every global the compiled wrapper reads is in the parts' merged names.
    """
    ignore_params, cache_key = keying
    params = list(read_signature(_one).parameters.values())
    store = _store(isolation, dense)
    slot = _OwnedSlot(4) if owned else _PlainSlot(4)
    parts = _Parts(slot, _access(store), _claims(store, miss.mode), miss)

    source = _source(params, _key(params, cache_key, ignore_params), parts)

    bound = {"__func__", "__cache_key__", *parts.names}
    assert _globals_read(_wrapper_code("\n".join(source))) <= bound
