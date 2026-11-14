"""
Codegen'd wrappers for scope-cached functions and generator functions.

Generated wrappers match the original signature exactly, and a function whose result
doesn't vary by argument reads one slot instead of a keyed dict. A generator function's
generator is driven directly: entered on a miss, and closed when the activation ends. A
function whose decorator hides a generator function checks what each miss returns.

A wrapper is built from parts, each chosen once from what it varies by. The slot is plain
or owned. The access follows the scope's kind and storage. The claims follow the scope's
isolation and the wrapper's mode, sync or async. The miss follows what the function gives.
Each part returns its lines of source and the names those lines read, so nothing downstream
asks which variant it holds.
"""

import threading
import weakref
from asyncio import current_task
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Hashable, Sequence
from dataclasses import dataclass, field
from functools import wraps
from inspect import Parameter
from types import AsyncGeneratorType, CoroutineType, GeneratorType
from typing import (
    Any,
    NoReturn,
    Protocol,
    cast,
)

from stratae.callables import Gives
from stratae.codegen import (
    Lines,
    async_def_wrapper,
    block,
    compile_wrapper,
    def_wrapper,
    defaults,
    kwdefaults,
    read_signature,
    render_arguments,
    render_parameters,
)
from stratae.lifecycle._inflight import (
    AsyncPending,
    Pending,
    WatchedClaim,
    wait_for_claim,
    watch_task_claim,
    watch_thread_claim,
)
from stratae.lifecycle._slots import UNSET, SlotStore
from stratae.lifecycle._stack import enter_async_generator, enter_generator
from stratae.lifecycle.exceptions import (
    ActivationEndedError,
    AsyncInSyncScopeError,
    UnreadableSignatureError,
)


class Cached[**P, T, U](Protocol):
    """
    A function returned by `Scope.cache` or `AsyncScope.cache`.

    Calling it returns the value cached for the current activation. `uncached` is the
    original function, callable without an active scope.
    """

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Return the cached value for the current scope activation."""
        ...

    def uncached(self, *args: P.args, **kwargs: P.kwargs) -> U:
        """Call the original function, bypassing the cache."""
        ...


class _Slot(Protocol):
    """
    Source for one wrapper's dedicated slot: reading it, testing the read, and writing it.

    A reused slot in a context scope may still hold a dead wrapper's value in other
    activations, so an owned slot boxes its values in `__Owned__` and treats anything else
    as a miss. A plain slot tests against `__UNSET__`.
    """

    @property
    def index(self) -> int:
        """Return the slot's index in its scope's storage."""
        ...

    @property
    def names(self) -> dict[str, Any]:
        """Return the names this slot's code reads."""
        ...

    def read(self, target: str) -> str:
        """Read the slot, leaving what a hit needs for `target`."""
        ...

    def on_hit(self, target: str, hit: Lines) -> Lines:
        """Run `hit` if the last read found a value, which is then in `target`."""
        ...

    def on_miss(self, target: str, miss: Lines, hit: Sequence[str] = ()) -> Lines:
        """Run `miss` if the last read found nothing, or else `hit`, with the value in `target`."""
        ...

    def write(self, source: str) -> str:
        """Store `source` into the slot."""
        ...


@dataclass(frozen=True, slots=True)
class _PlainSlot:
    """A slot holding its values as they are, empty when `__UNSET__`."""

    index: int
    names: dict[str, Any] = field(default_factory=lambda: {"__UNSET__": UNSET})

    def read(self, target: str) -> str:
        """Read the slot into `target`."""
        return f"{target} = __slots__[{self.index}]"

    def on_hit(self, target: str, hit: Lines) -> Lines:
        """Run `hit` if `target` holds a value."""
        return block(f"if {target} is not __UNSET__:", hit)

    def on_miss(self, target: str, miss: Lines, hit: Sequence[str] = ()) -> Lines:
        """Run `miss` if `target` is empty, or else `hit`."""
        return [*block(f"if {target} is __UNSET__:", miss), *(block("else:", hit) if hit else [])]

    def write(self, source: str) -> str:
        """Store `source` as it is."""
        return f"__slots__[{self.index}] = {source}"


def _box_class() -> type:
    """Return a new box class, so only this wrapper's boxes pass its owned slot's test."""

    class Owned:
        __slots__ = ("value",)

        def __init__(self, value: Any) -> None:
            self.value = value

    return Owned


@dataclass(frozen=True, slots=True)
class _OwnedSlot:
    """A reused slot in a context scope, holding its values boxed in its own `__Owned__`."""

    index: int
    names: dict[str, Any] = field(default_factory=lambda: {"__Owned__": _box_class()})

    def read(self, target: str) -> str:
        """Read the slot into `target`, which a hit unboxes in place."""
        return f"{target} = __slots__[{self.index}]"

    def on_hit(self, target: str, hit: Lines) -> Lines:
        """Unbox `target` and run `hit` if it's this wrapper's box."""
        return block(f"if {target}.__class__ is __Owned__:", [f"{target} = {target}.value", *hit])

    def on_miss(self, target: str, miss: Lines, hit: Sequence[str] = ()) -> Lines:
        """Run `miss` unless `target` is this wrapper's box, or else unbox it and run `hit`."""
        return [
            *block(f"if {target}.__class__ is not __Owned__:", miss),
            *block("else:", [f"{target} = {target}.value", *hit]),
        ]

    def write(self, source: str) -> str:
        """Store `source` boxed."""
        return f"__slots__[{self.index}] = __Owned__({source})"


def _allocate_slot(store: SlotStore) -> _Slot:
    """Allocate a slot, owned if it's reused in a context scope."""
    index, reused = store.allocate()
    return _OwnedSlot(index) if reused and not store.shared else _PlainSlot(index)


_CACHE_CHECK = block("if __ck__ in __cache__:", ["return __cache__[__ck__]"])
"""The cache-hit fast path."""

_SHARED_CACHE_CHECK = [
    *block("try:", ["return __cache__[__ck__]"]),
    *block("except KeyError:", ["pass"]),
]
"""
A shared scope's lock-free cache-hit fast path, with a single lookup.

A shared cache lives as long as the application, so hits dominate and outweigh the raised
KeyError on a miss.
"""

_CACHE_STORE = ["__cache__[__ck__] = __value__"]

_ENDED_CHECK = block("if not __slots__:", ["__ended__(__store__)"])
"""Raise for a claim waiter whose activation ended while it was waiting."""


def _shared_first_read(read: str) -> Lines:
    """
    Read a shared scope's SharedVar storage directly, then the slot.

    An inactive scope's UNSET raises TypeError on subscript. An ended activation's emptied
    storage raises IndexError or KeyError. Either falls back to the store's `get()`, which
    raises the right error.
    """
    return [
        "__slots__ = __var__.storage",
        *block("try:", [read]),
        *block("except (TypeError, LookupError):", ["__slots__ = __store__.get()", read]),
    ]


def _context_first_read(read: str) -> Lines:
    """
    Read a context scope's storage from its ContextVar, then the slot.

    `get()` raises LookupError when inactive. An ended activation's emptied storage raises
    IndexError or KeyError. Either falls back to the store's `get()`, which raises the right
    error.
    """
    return [
        *block("try:", ["__slots__ = __var__.get()", read]),
        *block("except LookupError:", ["__slots__ = __store__.get()", read]),
    ]


def _shared_claim_key(entry: str | None) -> tuple[Lines, str]:
    """Name a claim by its entry alone, since a shared scope has one activation."""
    return [], "None" if entry is None else entry


def _context_claim_key(entry: str | None) -> tuple[Lines, str]:
    """
    Name a claim by its activation's storage id and its entry, into `__pk__`.

    Activations share the wrapper's claims dict. The id is unique while claimed, since the
    claimant holds the storage.
    """
    key = "__id__(__slots__)" if entry is None else f"(__id__(__slots__), {entry})"
    return [f"__pk__ = {key}"], "__pk__"


def _dense_store(store: str) -> Lines:
    """Run `store`, skipped if the activation ended, since an emptied list raises IndexError."""
    return [*block("try:", [store]), *block("except IndexError:", ["pass"])]


def _sparse_store(store: str) -> Lines:
    """Run `store` unless the activation ended, since an emptied SlotDict wouldn't raise."""
    return block("if __slots__:", [store])


@dataclass(frozen=True, slots=True)
class _Access:
    """How a wrapper reaches its scope's storage, from the scope's kind."""

    first_read: Callable[[str], Lines]
    """Resolves `__slots__`, then runs the given slot read."""
    claim_key: Callable[[str | None], tuple[Lines, str]]
    """Returns the lines naming a claim, and its source, for the slot or a cache key."""
    keyed_hit: Lines
    """The lock-free cache-hit check of a locked keyed wrapper."""
    slot_store: Callable[[str], Lines]
    """Guards a slot store against refilling an activation that ended mid-computation."""
    names: dict[str, Any]


def _access(store: SlotStore) -> _Access:
    """Return how a wrapper reaches `store`'s storage."""
    slot_store = _dense_store if store.dense else _sparse_store
    names: dict[str, Any] = {"__store__": store, "__var__": store.var}
    if store.shared:
        return _Access(
            _shared_first_read, _shared_claim_key, _SHARED_CACHE_CHECK, slot_store, names
        )
    names["__id__"] = id
    return _Access(_context_first_read, _context_claim_key, _CACHE_CHECK, slot_store, names)


def _new_cache(slot: _Slot) -> Lines:
    """Create a keyed function's cache dict, and store it in its slot."""
    return ["__cache__ = {}", slot.write("__cache__")]


def _slot_recheck(slot: _Slot) -> Lines:
    """Re-read the slot for a waiter, returning a value another caller stored."""
    return [slot.read("__value__"), *slot.on_hit("__value__", ["return __value__"])]


def _cache_recheck(slot: _Slot) -> Lines:
    """Re-read the cache dict for a waiter, creating it if absent, and return a stored hit."""
    return [slot.read("__cache__"), *slot.on_miss("__cache__", _new_cache(slot)), *_CACHE_CHECK]


def _locked(body: Lines) -> Lines:
    """Guard `body` by the wrapper's lock, via acquire/try/finally for speed."""
    return ["__acquire__()", *block("try:", body), *block("finally:", ["__release__()"])]


class _Claim(Protocol):
    """How overlapping callers of one wrapper compute each entry once."""

    @property
    def names(self) -> dict[str, Any]:
        """Return the names this claim's code reads."""
        ...

    def slot_miss(self, slot: _Slot, access: _Access, compute: Lines) -> Lines:
        """Return a slot wrapper's miss, which fills `__value__` and stores it."""
        ...

    def keyed(self, slot: _Slot, access: _Access, key: str, compute: Lines) -> Lines:
        """Return a keyed wrapper's body up to its return, with `key` computing `__ck__`."""
        ...


@dataclass(frozen=True, slots=True)
class _Unclaimed:
    """A context scope's sync wrapper: one thread uses an activation at a time."""

    names: dict[str, Any] = field(default_factory=dict[str, Any])

    def slot_miss(self, slot: _Slot, access: _Access, compute: Lines) -> Lines:
        """Compute and store the value."""
        return [*compute, *access.slot_store(slot.write("__value__"))]

    def keyed(self, slot: _Slot, access: _Access, key: str, compute: Lines) -> Lines:
        """Make the dict on the first read, then compute and store each missing key."""
        return [
            *access.first_read(slot.read("__cache__")),
            *slot.on_miss("__cache__", _new_cache(slot)),
            key,
            *_CACHE_CHECK,
            *compute,
            *_CACHE_STORE,
        ]


@dataclass(frozen=True, slots=True)
class _LockedClaims:
    """
    Claims kept in a per-wrapper dict behind a lock, for threads or a shared scope.

    The claim is the owning thread's id or task, replaced by a `Pending` or `AsyncPending`
    only once another caller waits, so an uncontended miss allocates nothing. A keyed
    wrapper's dict is made under the lock, since two threads could otherwise each build one
    and lose inserts.
    """

    wait: str
    """Waits for the claimant, blocking or awaiting."""
    names: dict[str, Any]

    def _compute(
        self, claim_key: tuple[Lines, str], recheck: Lines, compute: Lines, store: Lines
    ) -> Lines:
        key_lines, key = claim_key
        claim = [
            *recheck,
            f"__pending__ = __inflight__.get({key})",
            *block("if __pending__ is None:", [f"__inflight__[{key}] = __claimant__()", "break"]),
            f"__done__ = __watch__(__inflight__, {key}, __pending__)",
        ]
        unclaim = [
            f"__claim__ = __inflight__.pop({key})",
            *block("if __claim__.__class__ is __Pending__:", ["__claim__.release()"]),
        ]
        return [
            *key_lines,
            *block("while True:", [*_locked(claim), self.wait, *_ENDED_CHECK]),
            *block("try:", [*compute, *store]),
            *block("finally:", _locked(unclaim)),
        ]

    def slot_miss(self, slot: _Slot, access: _Access, compute: Lines) -> Lines:
        """Claim the slot, or wait for its claimant and re-read."""
        store = access.slot_store(slot.write("__value__"))
        return self._compute(access.claim_key(None), _slot_recheck(slot), compute, store)

    def keyed(self, slot: _Slot, access: _Access, key: str, compute: Lines) -> Lines:
        """Check for a hit lock-free, then claim the key, or wait for its claimant."""
        return [
            *access.first_read(slot.read("__cache__")),
            key,
            *slot.on_hit("__cache__", access.keyed_hit),
            *self._compute(access.claim_key("__ck__"), _cache_recheck(slot), compute, _CACHE_STORE),
        ]


def _locked_thread_claims() -> _LockedClaims:
    """Return locked claims for a sync wrapper, whose waiters block."""
    lock = threading.Lock()
    return _LockedClaims(
        "__done__.wait()",
        {
            "__acquire__": lock.acquire,
            "__release__": lock.release,
            "__inflight__": {},
            "__Pending__": Pending,
            "__claimant__": threading.get_ident,
            "__watch__": watch_thread_claim,
            "__ended__": _raise_ended,
        },
    )


def _locked_task_claims() -> _LockedClaims:
    """
    Return locked claims for an async wrapper, whose waiters await.

    The lock is a thread lock, since it never guards an await.
    """
    lock = threading.Lock()
    return _LockedClaims(
        "await __done__",
        {
            "__acquire__": lock.acquire,
            "__release__": lock.release,
            "__inflight__": {},
            "__Pending__": AsyncPending,
            "__claimant__": current_task,
            "__watch__": watch_task_claim,
            "__ended__": _raise_ended,
        },
    )


@dataclass(frozen=True, slots=True)
class _TaskClaims:
    """
    A context scope's async wrapper: the owning task is the claim, kept lock-free.

    The claim is replaced by a WatchedClaim only once another task waits, so an uncontended
    miss allocates nothing.
    """

    names: dict[str, Any] = field(
        default_factory=lambda: {
            "__claims__": {},
            "__task__": current_task,
            "__wait_claim__": wait_for_claim,
            "__Watched__": WatchedClaim,
            "__ended__": _raise_ended,
        }
    )

    @staticmethod
    def _compute(
        claim_key: tuple[Lines, str], recheck: Lines, compute: Lines, store: Lines
    ) -> Lines:
        key_lines, key = claim_key
        loop = [
            f"__claim__ = __claims__.get({key})",
            *block("if __claim__ is None:", ["break"]),
            f"await __wait_claim__(__claims__, {key}, __claim__)",
            *_ENDED_CHECK,
            *recheck,
        ]
        unclaim = [
            f"__claim__ = __claims__.pop({key})",
            *block("if __claim__.__class__ is __Watched__:", ["__claim__.done.set()"]),
        ]
        return [
            *key_lines,
            *block("while True:", loop),
            f"__claims__[{key}] = __task__()",
            *block("try:", [*compute, *store]),
            *block("finally:", unclaim),
        ]

    def slot_miss(self, slot: _Slot, access: _Access, compute: Lines) -> Lines:
        """Claim the slot, or wait for its claimant and re-read."""
        store = access.slot_store(slot.write("__value__"))
        return self._compute(access.claim_key(None), _slot_recheck(slot), compute, store)

    def keyed(self, slot: _Slot, access: _Access, key: str, compute: Lines) -> Lines:
        """Make the dict on the first read, then claim each missing key, or wait for it."""
        return [
            *access.first_read(slot.read("__cache__")),
            key,
            *slot.on_miss("__cache__", _new_cache(slot), hit=_CACHE_CHECK),
            *self._compute(access.claim_key("__ck__"), _CACHE_CHECK, compute, _CACHE_STORE),
        ]


@dataclass(frozen=True, slots=True)
class _Mode:
    """Whether a wrapper is sync or async, and everything that follows from it."""

    define: Callable[[str, Lines], Lines]
    """Defines the wrapper from its parameters and body."""
    context_claims: Callable[[], _Claim]
    """Makes the claims of a context scope's wrapper."""
    locked_claims: Callable[[], _Claim]
    """Makes the claims of a threaded or shared scope's wrapper."""


_SYNC = _Mode(def_wrapper, _Unclaimed, _locked_thread_claims)
_ASYNC = _Mode(async_def_wrapper, _TaskClaims, _locked_task_claims)


def _claims(store: SlotStore, mode: _Mode) -> _Claim:
    """Return how a wrapper in `store`'s scope claims its misses."""
    return mode.locked_claims() if store.shared or store.threads else mode.context_claims()


@dataclass(frozen=True, slots=True)
class _Miss:
    """How a miss fills `__value__` from the call to the cached function."""

    mode: _Mode
    compute: Callable[[str], Lines]
    """Returns the code filling `__value__`, given the source of the call."""
    names: dict[str, Any] = field(default_factory=dict[str, Any])


def _check_sync(call: str) -> Lines:
    """Enter a returned generator, refuse a coroutine or async generator, keep anything else."""
    return [
        f"__value__ = {call}",
        "__kind__ = __value__.__class__",
        *block(
            "if __kind__ is __Generator__:",
            ["__value__ = __enter_generator__(__slots__, __value__)"],
        ),
        *block(
            "elif __kind__ is __Coroutine__ or __kind__ is __AsyncGenerator__:",
            ["__refuse_async__(__func__, __store__, __value__)"],
        ),
    ]


def _check_async(call: str) -> Lines:
    """Await a returned coroutine, enter either kind of generator, and keep anything else."""
    return [
        f"__value__ = {call}",
        "__kind__ = __value__.__class__",
        *block(
            "if __kind__ is __AsyncGenerator__:",
            ["__value__ = await __enter_async_generator__(__slots__, __value__)"],
        ),
        *block(
            "elif __kind__ is __Generator__:",
            ["__value__ = __enter_generator__(__slots__, __value__)"],
        ),
        *block("elif __kind__ is __Coroutine__:", ["__value__ = await __value__"]),
    ]


def _raise_ended(store: SlotStore) -> NoReturn:
    """Raise for a claim waiter whose activation ended while it was waiting."""
    raise ActivationEndedError(store.name)


def _refuse_async(func: Callable[..., Any], store: SlotStore, value: object) -> NoReturn:
    """Refuse a coroutine or async generator returned to a sync checked wrapper."""
    if isinstance(value, CoroutineType):
        value.close()
        raise AsyncInSyncScopeError(func, store.name, Gives.COROUTINE)
    raise AsyncInSyncScopeError(func, store.name, Gives.ASYNC_GENERATOR)


_ENTERS = {"__enter_generator__": enter_generator}
_ENTERS_ASYNC = {"__enter_async_generator__": enter_async_generator}
_KINDS = {
    "__Generator__": GeneratorType,
    "__AsyncGenerator__": AsyncGeneratorType,
    "__Coroutine__": CoroutineType,
}

_CALL = _Miss(_SYNC, lambda call: [f"__value__ = {call}"])
_AWAIT = _Miss(_ASYNC, lambda call: [f"__value__ = await {call}"])
_ENTER_GENERATOR = _Miss(
    _SYNC, lambda call: [f"__value__ = __enter_generator__(__slots__, {call})"], _ENTERS
)
_ENTER_ASYNC_GENERATOR = _Miss(
    _ASYNC,
    lambda call: [f"__value__ = await __enter_async_generator__(__slots__, {call})"],
    _ENTERS_ASYNC,
)
_CHECK_SYNC = _Miss(_SYNC, _check_sync, {**_ENTERS, **_KINDS, "__refuse_async__": _refuse_async})
_CHECK_ASYNC = _Miss(_ASYNC, _check_async, {**_ENTERS, **_ENTERS_ASYNC, **_KINDS})


def _params_key(params: Sequence[Parameter]) -> str:
    """
    Render a keyed function's key from its parameters.

    Each keyed function has its own dict, so the key only distinguishes its calls. It's a
    flat tuple of parameter locals, or the bare local for a single parameter. Only a true
    `**kwargs` needs `frozenset(items())`.
    """

    def part(param: Parameter) -> str:
        if param.kind is Parameter.VAR_POSITIONAL:
            return f"*{param.name}"
        if param.kind is Parameter.VAR_KEYWORD:
            return f"frozenset({param.name}.items())"
        return param.name

    if len(params) == 1:
        (param,) = params
        return param.name if param.kind is Parameter.VAR_POSITIONAL else part(param)
    return "(" + "".join(f"{part(param)}, " for param in params) + ")"


def _key(
    params: Sequence[Parameter], cache_key: Callable[..., Hashable] | None, ignore_params: bool
) -> str | None:
    """
    Return the line computing a call's key into `__ck__`, or None to keep one value.

    A function whose result doesn't vary by argument keeps its value in its slot. Any other
    keeps a dict there, keyed by `cache_key`'s result or by the parameters.
    """
    if ignore_params or (cache_key is None and not params):
        return None
    if cache_key is None:
        return f"__ck__ = {_params_key(params)}"
    return f"__ck__ = __cache_key__({render_arguments(params)})"


def _source(
    params: Sequence[Parameter],
    key: str | None,
    slot: _Slot,
    access: _Access,
    claims: _Claim,
    miss: _Miss,
) -> Lines:
    """Return a cache wrapper's source, from what each of its parts was chosen to be."""
    compute = miss.compute(f"__func__({render_arguments(params)})")
    if key is None:
        body = [
            *access.first_read(slot.read("__value__")),
            *slot.on_miss("__value__", claims.slot_miss(slot, access, compute)),
        ]
    else:
        body = claims.keyed(slot, access, key, compute)
    return miss.mode.define(render_parameters(params), [*body, "return __value__"])


def _namespace(
    func: Callable[..., Any],
    cache_key: Callable[..., Hashable] | None,
    slot: _Slot,
    access: _Access,
    claims: _Claim,
    miss: _Miss,
) -> dict[str, Any]:
    """Return the names a wrapper's source reads: the cached function's, and each part's."""
    return {
        "__func__": func,
        "__cache_key__": cache_key,
        **slot.names,
        **access.names,
        **claims.names,
        **miss.names,
    }


def _read_params(func: Callable[..., Any], store: SlotStore) -> list[Parameter]:
    """
    Return `func`'s parameters, which its wrapper mirrors.

    :raises UnreadableSignatureError: If `func`'s signature can't be read.
    """
    try:
        return list(read_signature(func).parameters.values())
    except ValueError:
        raise UnreadableSignatureError(func, store.name) from None


def _create_wrapper(
    func: Callable[..., Any],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
    miss: _Miss,
) -> Callable[..., Any]:
    """
    Build a cache wrapper for `func`, freeing its slot once the wrapper is gone.

    Every kind of wrapper is built here. They differ only in how a miss computes its value.
    Each part is chosen once, and brings the names its code reads.
    """
    params = _read_params(func, store)
    key = _key(params, cache_key, ignore_params)
    slot = _allocate_slot(store)
    access = _access(store)
    claims = _claims(store, miss.mode)
    namespace = _namespace(func, cache_key, slot, access, claims, miss)
    wrapper = compile_wrapper(_source(params, key, slot, access, claims, miss), func, namespace)
    wraps(func)(wrapper)
    wrapper.__defaults__ = defaults(params)
    wrapper.__kwdefaults__ = kwdefaults(params)
    wrapper.uncached = func
    weakref.finalize(wrapper, store.release, slot.index)
    return wrapper


def create_sync_wrapper[**P, T](
    func: Callable[P, T],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, T, T]:
    """
    Create a sync wrapper that caches `func`'s result per scope activation.

    :param func: The function to cache.
    :param store: The owning scope's slot storage.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    return cast(Cached[P, T, T], _create_wrapper(func, store, cache_key, ignore_params, _CALL))


def create_async_wrapper[**P, T](
    func: Callable[P, Awaitable[T]],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, Awaitable[T], Awaitable[T]]:
    """
    Create an async wrapper that caches `func`'s result per scope activation.

    :param func: The async function to cache.
    :param store: The owning {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`'s
        slot storage.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    return cast(
        Cached[P, Awaitable[T], Awaitable[T]],
        _create_wrapper(func, store, cache_key, ignore_params, _AWAIT),
    )


def create_generator_wrapper[**P, T](
    func: Callable[P, Generator[T, Any, Any]],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, T, Generator[T, Any, Any]]:
    """
    Create a wrapper that enters a generator function's generator and caches its yielded value.

    The generator is closed when the scope's activation ends.

    :param func: The generator function.
    :param store: The owning scope's slot storage.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    return cast(
        Cached[P, T, Generator[T, Any, Any]],
        _create_wrapper(func, store, cache_key, ignore_params, _ENTER_GENERATOR),
    )


def create_async_generator_wrapper[**P, T](
    func: Callable[P, AsyncGenerator[T, Any]],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, Awaitable[T], AsyncGenerator[T, Any]]:
    """
    Create an async wrapper that enters an async generator and caches its yielded value.

    The generator is closed when the scope's activation ends.

    :param func: The async generator function.
    :param store: The owning {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`'s
        slot storage.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: An async wrapper matching `func`'s signature, with `func` as its `uncached`
        attribute.
    """
    return cast(
        Cached[P, Awaitable[T], AsyncGenerator[T, Any]],
        _create_wrapper(func, store, cache_key, ignore_params, _ENTER_ASYNC_GENERATOR),
    )


def create_checked_sync_wrapper(
    func: Callable[..., Any],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[..., Any, Any]:
    """
    Create a sync wrapper for a function whose decorator hides one giving something else.

    The decorator may have changed what the function gives. So each miss checks: a generator
    is entered and its yielded value cached, and anything else is cached as it is. A coroutine
    or an async generator is refused with
    {py:exc}`AsyncInSyncScopeError <stratae.lifecycle.exceptions.AsyncInSyncScopeError>`.

    :param func: The decorated function.
    :param store: The owning scope's slot storage.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    return cast(
        Cached[..., Any, Any],
        _create_wrapper(func, store, cache_key, ignore_params, _CHECK_SYNC),
    )


def create_checked_async_wrapper(
    func: Callable[..., Any],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[..., Any, Any]:
    """
    Create an async wrapper for a function whose decorator hides one giving something else.

    The decorator may have changed what the function gives. So each miss checks: a coroutine
    is awaited, a generator of either kind is entered and its yielded value cached, and
    anything else is cached as it is.

    :param func: The decorated function.
    :param store: The owning {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`'s
        slot storage.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: An async wrapper matching `func`'s signature, with `func` as its `uncached`
        attribute.
    """
    return cast(
        Cached[..., Any, Any],
        _create_wrapper(func, store, cache_key, ignore_params, _CHECK_ASYNC),
    )
