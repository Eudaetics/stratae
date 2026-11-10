"""
Codegen'd wrappers for scope-cached functions and generator functions.

Generated wrappers match the original signature exactly, and a function whose result
doesn't vary by argument reads one slot instead of a keyed dict. A generator function's
generator is driven directly: entered on a miss, and closed when the activation ends. A
function whose decorator hides a generator function checks what each miss returns.
"""

import threading
import weakref
from asyncio import current_task
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Hashable
from functools import wraps
from inspect import Parameter
from types import AsyncGeneratorType, GeneratorType
from typing import (
    Any,
    NoReturn,
    Protocol,
    cast,
)

from stratae.codegen import (
    Writer,
    compile_wrapper,
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
    AsyncGeneratorInSyncScopeError,
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


class _SlotCode:
    """
    Source text for reading and writing one wrapper's dedicated slot.

    A reused slot in a context scope may still hold a dead wrapper's value in other
    activations, so an owned slot boxes its values in `__Owned__` and treats anything else
    as a miss.
    """

    __slots__ = ("index", "owned")

    def __init__(self, index: int, owned: bool) -> None:
        """Describe slot `index`, boxing its values when `owned`."""
        self.index = index
        self.owned = owned

    def namespace(self) -> dict[str, Any]:
        """Return the namespace bindings this slot's code needs."""
        if not self.owned:
            return {}

        class Owned:
            __slots__ = ("value",)

            def __init__(self, value: Any) -> None:
                self.value = value

        return {"__Owned__": Owned}

    def write_read(self, writer: Writer, target: str, store: SlotStore | None = None) -> None:
        """Write a read of this slot, resolving __slots__ first when `store` is given."""
        read = f"{'__entry__' if self.owned else target} = __slots__[{self.index}]"
        if store is None:
            writer.write(read)
        else:
            _write_resolve_first_read(writer, store, read)

    def write_branch(
        self,
        writer: Writer,
        target: str,
        write_hit: Callable[[], None] | None = None,
        write_miss: Callable[[], None] | None = None,
    ) -> None:
        """Write the hit/miss branch on the last read, leaving a hit's value in `target`."""
        if self.owned:
            hit, miss = "__entry__.__class__ is __Owned__", "__entry__.__class__ is not __Owned__"
        else:
            hit, miss = f"{target} is not __UNSET__", f"{target} is __UNSET__"

        def write_hit_body() -> None:
            if self.owned:
                writer.write(f"{target} = __entry__.value")
            if write_hit is not None:
                write_hit()

        if write_miss is None:
            writer.write(f"if {hit}:")
            with writer.block():
                write_hit_body()
            return
        writer.write(f"if {miss}:")
        with writer.block():
            write_miss()
        if self.owned or write_hit is not None:
            writer.write("else:")
            with writer.block():
                write_hit_body()

    def write(self, source: str) -> str:
        """Render the store of `source` into this slot."""
        value = f"__Owned__({source})" if self.owned else source
        return f"__slots__[{self.index}] = {value}"


def _read_params(func: Callable[..., Any], store: SlotStore) -> list[Parameter]:
    """
    Return `func`'s parameters, which its wrapper mirrors.

    :raises UnreadableSignatureError: If `func`'s signature can't be read.
    """
    try:
        return list(read_signature(func).parameters.values())
    except ValueError:
        raise UnreadableSignatureError(func, store.name) from None


def _allocate_slot(store: SlotStore) -> _SlotCode:
    """Allocate a slot, owned if it's reused in a context scope."""
    index, reused = store.allocate()
    return _SlotCode(index, reused and not store.shared)


def _is_slot_eligible(
    params: list[Parameter], cache_key: Callable[..., Hashable] | None, ignore_params: bool
) -> bool:
    """Whether a function caches one value per activation, stored directly in its slot."""
    return ignore_params or (cache_key is None and not params)


def _render_key_single_param(param: Parameter) -> str:
    """Render the cache-key source for a function with exactly one parameter."""
    if param.kind is Parameter.VAR_KEYWORD:
        return f"frozenset({param.name}.items())"
    return param.name


def _aggregate_key_param_strs(params: list[Parameter]) -> list[str]:
    """Render each parameter's cache-key source, one entry per parameter."""
    parts: list[str] = []
    for param in params:
        if param.kind is Parameter.VAR_POSITIONAL:
            parts.append(f"*{param.name}")
        elif param.kind is Parameter.VAR_KEYWORD:
            parts.append(f"frozenset({param.name}.items())")
        else:
            parts.append(param.name)
    return parts


def _render_key_expression(
    cache_key: Callable[..., Hashable] | None, params: list[Parameter]
) -> str:
    """
    Render the cache-key expression for a keyed function.

    Each keyed function has its own dict, so the key only distinguishes its calls. It's a
    flat tuple of parameter locals, or the bare local for a single parameter. Only a true
    `**kwargs` needs `frozenset(items())`.
    """
    if cache_key is not None:
        return f"__cache_key__({render_arguments(params)})"
    if len(params) == 1:
        return _render_key_single_param(params[0])
    parts = _aggregate_key_param_strs(params)
    return "(" + "".join(f"{part}, " for part in parts) + ")"


def _write_key(
    writer: Writer, cache_key: Callable[..., Hashable] | None, params: list[Parameter]
) -> None:
    """Write the __ck__ assignment shared by the hit check and the miss store."""
    writer.write(f"__ck__ = {_render_key_expression(cache_key, params)}")


def _write_cache_check(writer: Writer) -> None:
    """Write the cache-hit fast path."""
    writer.write("if __ck__ in __cache__:")
    with writer.block():
        writer.write("return __cache__[__ck__]")


def _write_shared_cache_check(writer: Writer) -> None:
    """
    Write a shared scope's lock-free cache-hit fast path, with a single lookup.

    A shared cache lives as long as the application, so hits dominate and outweigh the
    raised KeyError on a miss.
    """
    writer.write("try:")
    with writer.block():
        writer.write("return __cache__[__ck__]")
    writer.write("except KeyError:")
    with writer.block():
        writer.write("pass")


def _write_resolve_first_read(writer: Writer, store: SlotStore, read: str) -> None:
    """
    Write the resolution of __slots__ and the first slot read.

    No explicit active check is paid. A shared scope reads its SharedVar's storage directly;
    an inactive scope's UNSET raises TypeError on subscript. A context scope's `get()` raises
    LookupError when inactive. An ended activation's emptied storage raises IndexError or
    KeyError. Every miss falls back to the store's `get()`, which raises the right error.
    """
    if store.shared:
        writer.write("__slots__ = __var__.storage")
        writer.write("try:")
        with writer.block():
            writer.write(read)
        writer.write("except (TypeError, LookupError):")
        with writer.block():
            writer.write("__slots__ = __store__.get()")
            writer.write(read)
        return
    writer.write("try:")
    with writer.block():
        writer.write("__slots__ = __var__.get()")
        writer.write(read)
    writer.write("except LookupError:")
    with writer.block():
        writer.write("__slots__ = __store__.get()")
        writer.write(read)


def _raise_ended(store: SlotStore) -> NoReturn:
    """Raise for a claim waiter whose activation ended while it was waiting."""
    raise ActivationEndedError(store.name)


def _build_namespace(func: Callable[..., Any], store: SlotStore) -> dict[str, Any]:
    """Build the namespace bindings every codegen'd wrapper compiles against."""
    return {"__func__": func, "__store__": store, "__UNSET__": UNSET, "__var__": store.var}


def _write_new_cache(writer: Writer, slot: _SlotCode) -> None:
    """Write the lazy creation of a keyed function's cache dict into its slot."""
    writer.write("__cache__ = {}")
    writer.write(slot.write("__cache__"))


def _write_slot_store(writer: Writer, slot: _SlotCode, store: SlotStore) -> None:
    """
    Write the store of __value__ into a slot-eligible function's slot.

    An activation that ended mid-computation must not be refilled. An emptied list raises
    IndexError for free; an emptied SlotDict wouldn't, so sparse stores check first.
    """
    if store.dense:
        writer.write("try:")
        with writer.block():
            writer.write(slot.write("__value__"))
        writer.write("except IndexError:")
        with writer.block():
            writer.write("pass")
        return
    writer.write("if __slots__:")
    with writer.block():
        writer.write(slot.write("__value__"))


def _write_locked(writer: Writer, write_body: Callable[[], None]) -> None:
    """Write `write_body` guarded by the wrapper's lock, via acquire/try/finally for speed."""
    writer.write("__acquire__()")
    writer.write("try:")
    with writer.block():
        write_body()
    writer.write("finally:")
    with writer.block():
        writer.write("__release__()")


def _write_claimed_compute(
    writer: Writer,
    key: str,
    is_async: bool,
    write_recheck: Callable[[], None],
    write_compute: Callable[[], None],
    write_store: Callable[[], None],
) -> None:
    """
    Write a lock-guarded claim-then-compute miss path, for a `"locked"` or `"thread"` style.

    The claim is the owning thread's id or task, replaced by a `Pending` or `AsyncPending`
    only once another caller waits, so an uncontended miss allocates nothing.
    """

    def write_claim() -> None:
        write_recheck()
        writer.write(f"__pending__ = __inflight__.get({key})")
        writer.write("if __pending__ is None:")
        with writer.block():
            writer.write(f"__inflight__[{key}] = __claimant__()")
            writer.write("break")
        writer.write(f"__done__ = __watch__(__inflight__, {key}, __pending__)")

    def write_unclaim() -> None:
        writer.write(f"__claim__ = __inflight__.pop({key})")
        writer.write("if __claim__.__class__ is __Pending__:")
        with writer.block():
            writer.write("__claim__.release()")

    writer.write("while True:")
    with writer.block():
        _write_locked(writer, write_claim)
        writer.write("await __done__" if is_async else "__done__.wait()")
        writer.write("if not __slots__:")
        with writer.block():
            writer.write("__ended__(__store__)")
    writer.write("try:")
    with writer.block():
        write_compute()
        write_store()
    writer.write("finally:")
    with writer.block():
        _write_locked(writer, write_unclaim)


def _write_task_claimed_compute(
    writer: Writer,
    key: str,
    write_recheck: Callable[[], None],
    write_compute: Callable[[], None],
    write_store: Callable[[], None],
) -> None:
    """
    Write a context scope's lock-free claim-then-compute miss path, for an async wrapper.

    The claim is the calling task, replaced by a WatchedClaim only once another task waits,
    so an uncontended miss allocates nothing.
    """
    writer.write("while True:")
    with writer.block():
        writer.write(f"__claim__ = __claims__.get({key})")
        writer.write("if __claim__ is None:")
        with writer.block():
            writer.write("break")
        writer.write(f"await __wait_claim__(__claims__, {key}, __claim__)")
        writer.write("if not __slots__:")
        with writer.block():
            writer.write("__ended__(__store__)")
        write_recheck()
    writer.write(f"__claims__[{key}] = __task__()")
    writer.write("try:")
    with writer.block():
        write_compute()
        write_store()
    writer.write("finally:")
    with writer.block():
        writer.write(f"__claim__ = __claims__.pop({key})")
        writer.write("if __claim__.__class__ is __Watched__:")
        with writer.block():
            writer.write("__claim__.done.set()")


def _write_slot_recheck(writer: Writer, slot: _SlotCode) -> None:
    """Write a waiter's re-read of a slot, returning a value another caller stored."""
    slot.write_read(writer, "__value__")
    slot.write_branch(writer, "__value__", write_hit=lambda: writer.write("return __value__"))


class _ClaimCode(Protocol):
    """
    How a wrapper ensures overlapping callers compute each entry once.

    Gives both the namespace bindings its code needs and that code, so the two can't disagree.
    """

    def namespace(self) -> dict[str, Any]:
        """Return the namespace bindings this strategy's code needs."""
        ...

    def write_slot_miss(
        self, writer: Writer, slot: _SlotCode, store: SlotStore, write_compute: Callable[[], None]
    ) -> None:
        """Write a slot wrapper's miss; write_compute fills __value__."""
        ...

    def write_keyed(
        self,
        writer: Writer,
        slot: _SlotCode,
        store: SlotStore,
        write_key: Callable[[], None],
        write_compute: Callable[[], None],
    ) -> None:
        """Write a keyed wrapper's body up to its return of __value__."""
        ...


class _Unclaimed:
    """A context scope's sync wrapper: one thread uses an activation at a time, so no claims."""

    __slots__ = ()

    def namespace(self) -> dict[str, Any]:
        """Return no bindings: nothing is claimed."""
        return {}

    def write_slot_miss(
        self, writer: Writer, slot: _SlotCode, store: SlotStore, write_compute: Callable[[], None]
    ) -> None:
        """Write a miss that computes and stores the value."""
        write_compute()
        _write_slot_store(writer, slot, store)

    def write_keyed(
        self,
        writer: Writer,
        slot: _SlotCode,
        store: SlotStore,
        write_key: Callable[[], None],
        write_compute: Callable[[], None],
    ) -> None:
        """Write a keyed body that computes and stores each missing key."""
        slot.write_read(writer, "__cache__", store)
        slot.write_branch(writer, "__cache__", write_miss=lambda: _write_new_cache(writer, slot))
        write_key()
        _write_cache_check(writer)
        write_compute()
        writer.write("__cache__[__ck__] = __value__")


class _LockedClaims:
    """
    Claims kept in a per-wrapper dict behind a lock.

    A shared scope claims a slot under `None`, and checks a keyed hit with a single lookup,
    since its cache lives as long as the application and hits dominate. A context scope with
    `threads=True` claims under the activation's id, since threads can share an activation
    through a copied context. The id is unique while claimed, since the claimant holds the
    storage. A keyed wrapper's dict is made under the lock, since two threads could otherwise
    each build one and lose inserts.
    """

    __slots__ = ("is_async", "shared")

    def __init__(self, is_async: bool, shared: bool) -> None:
        """Describe the claims of an async or sync wrapper, in a shared or threaded scope."""
        self.is_async = is_async
        self.shared = shared

    def namespace(self) -> dict[str, Any]:
        """Return the lock, the claims dict and the claim helpers."""
        is_async = self.is_async
        # A thread lock even for async wrappers: it never guards an await.
        lock = threading.Lock()
        namespace: dict[str, Any] = {
            "__acquire__": lock.acquire,
            "__release__": lock.release,
            "__inflight__": {},
            "__Pending__": AsyncPending if is_async else Pending,
            "__claimant__": current_task if is_async else threading.get_ident,
            "__watch__": watch_task_claim if is_async else watch_thread_claim,
            "__ended__": _raise_ended,
        }
        if not self.shared:
            namespace["__id__"] = id
        return namespace

    def write_slot_miss(
        self, writer: Writer, slot: _SlotCode, store: SlotStore, write_compute: Callable[[], None]
    ) -> None:
        """Write a miss that claims the slot, or waits for its claimant and re-reads."""
        key = "None"
        if not self.shared:
            writer.write("__pk__ = __id__(__slots__)")
            key = "__pk__"
        _write_claimed_compute(
            writer,
            key,
            self.is_async,
            lambda: _write_slot_recheck(writer, slot),
            write_compute,
            lambda: _write_slot_store(writer, slot, store),
        )

    def write_keyed(
        self,
        writer: Writer,
        slot: _SlotCode,
        store: SlotStore,
        write_key: Callable[[], None],
        write_compute: Callable[[], None],
    ) -> None:
        """Write a keyed body that claims each missing key, or waits for its claimant."""
        slot.write_read(writer, "__cache__", store)
        write_key()
        write_check = _write_shared_cache_check if self.shared else _write_cache_check
        slot.write_branch(writer, "__cache__", write_hit=lambda: write_check(writer))

        def write_recheck() -> None:
            slot.write_read(writer, "__cache__")
            slot.write_branch(
                writer, "__cache__", write_miss=lambda: _write_new_cache(writer, slot)
            )
            _write_cache_check(writer)

        key = "__ck__"
        if not self.shared:
            writer.write("__pk__ = (__id__(__slots__), __ck__)")
            key = "__pk__"
        _write_claimed_compute(
            writer,
            key,
            self.is_async,
            write_recheck,
            write_compute,
            lambda: writer.write("__cache__[__ck__] = __value__"),
        )


class _TaskClaims:
    """
    A context scope's async wrapper: the owning task is the claim, kept lock-free.

    Claims live in a per-wrapper dict keyed by activation, since tasks can share one.
    """

    __slots__ = ()

    def namespace(self) -> dict[str, Any]:
        """Return the claims dict and the claim helpers."""
        return {
            "__id__": id,
            "__claims__": {},
            "__task__": current_task,
            "__wait_claim__": wait_for_claim,
            "__Watched__": WatchedClaim,
            "__ended__": _raise_ended,
        }

    def write_slot_miss(
        self, writer: Writer, slot: _SlotCode, store: SlotStore, write_compute: Callable[[], None]
    ) -> None:
        """Write a miss that claims the slot, or waits for its claimant and re-reads."""
        writer.write("__pk__ = __id__(__slots__)")
        _write_task_claimed_compute(
            writer,
            "__pk__",
            lambda: _write_slot_recheck(writer, slot),
            write_compute,
            lambda: _write_slot_store(writer, slot, store),
        )

    def write_keyed(
        self,
        writer: Writer,
        slot: _SlotCode,
        store: SlotStore,
        write_key: Callable[[], None],
        write_compute: Callable[[], None],
    ) -> None:
        """Write a keyed body that claims each missing key, or waits for its claimant."""
        slot.write_read(writer, "__cache__", store)
        write_key()
        slot.write_branch(
            writer,
            "__cache__",
            write_hit=lambda: _write_cache_check(writer),
            write_miss=lambda: _write_new_cache(writer, slot),
        )
        writer.write("__pk__ = (__id__(__slots__), __ck__)")
        _write_task_claimed_compute(
            writer,
            "__pk__",
            lambda: _write_cache_check(writer),
            write_compute,
            lambda: writer.write("__cache__[__ck__] = __value__"),
        )


def _claim_code(store: SlotStore, is_async: bool) -> _ClaimCode:
    """Return how a wrapper of `store`'s scope claims its misses."""
    if store.shared or store.threads:
        return _LockedClaims(is_async, shared=store.shared)
    return _TaskClaims() if is_async else _Unclaimed()


def _finalize(
    writer: Writer, func: Callable[..., Any], params: list[Parameter], namespace: dict[str, Any]
) -> Callable[..., Any]:
    """Compile the wrapper source and restore its metadata and defaults."""
    wrapper = compile_wrapper(writer.render(), func, namespace)
    wraps(func)(wrapper)
    wrapper.__defaults__ = defaults(params)
    wrapper.__kwdefaults__ = kwdefaults(params)
    wrapper.uncached = func
    return wrapper


type _WriteValue = Callable[[Writer, str], None]
"""Writes `__value__` on a miss, given the source of the call to the cached function."""


def _create_wrapper(
    func: Callable[..., Any],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
    is_async: bool,
    write_value: _WriteValue,
    bindings: dict[str, Any] | None = None,
) -> Callable[..., Any]:
    """
    Build a cache wrapper for `func`, freeing its slot once the wrapper is gone.

    Every kind of wrapper is built here. They differ only in how a miss computes its value.

    :param is_async: Whether the wrapper is `async def`, so its miss can await.
    :param write_value: Writes the miss, from the call's source.
    :param bindings: Extra namespace bindings `write_value`'s code needs.
    """
    params = _read_params(func, store)
    slot = _allocate_slot(store)
    call = f"__func__({render_arguments(params)})"
    claim = _claim_code(store, is_async)

    writer = Writer()
    writer.write(f"{'async def' if is_async else 'def'} wrapper({render_parameters(params)}):")
    with writer.block():

        def write_compute() -> None:
            write_value(writer, call)

        if _is_slot_eligible(params, cache_key, ignore_params):
            slot.write_read(writer, "__value__", store)
            slot.write_branch(
                writer,
                "__value__",
                write_miss=lambda: claim.write_slot_miss(writer, slot, store, write_compute),
            )
        else:
            claim.write_keyed(
                writer, slot, store, lambda: _write_key(writer, cache_key, params), write_compute
            )
        writer.write("return __value__")

    namespace = _build_namespace(func, store) | slot.namespace() | claim.namespace()
    if bindings is not None:
        namespace.update(bindings)
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    wrapper = _finalize(writer, func, params, namespace)
    weakref.finalize(wrapper, store.release, slot.index)
    return wrapper


def _write_call(writer: Writer, call: str) -> None:
    writer.write(f"__value__ = {call}")


def _write_await(writer: Writer, call: str) -> None:
    writer.write(f"__value__ = await {call}")


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
    return cast(
        Cached[P, T, T],
        _create_wrapper(func, store, cache_key, ignore_params, False, _write_call),
    )


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
        _create_wrapper(func, store, cache_key, ignore_params, True, _write_await),
    )


def _write_enter_generator(writer: Writer, call: str) -> None:
    writer.write(f"__value__ = __enter_generator__(__slots__, {call})")


def _write_enter_async_generator(writer: Writer, call: str) -> None:
    writer.write(f"__value__ = await __enter_async_generator__(__slots__, {call})")


def _write_check_sync(writer: Writer, call: str) -> None:
    """Enter a returned generator, refuse a returned async generator, and keep anything else."""
    writer.write(f"__value__ = {call}")
    writer.write("__kind__ = __value__.__class__")
    writer.write("if __kind__ is __Generator__:")
    with writer.block():
        writer.write("__value__ = __enter_generator__(__slots__, __value__)")
    writer.write("elif __kind__ is __AsyncGenerator__:")
    with writer.block():
        writer.write("__refuse_async__()")


def _write_check_async(writer: Writer, call: str) -> None:
    """Enter a returned generator of either kind, and keep anything else."""
    writer.write(f"__value__ = {call}")
    writer.write("__kind__ = __value__.__class__")
    writer.write("if __kind__ is __AsyncGenerator__:")
    with writer.block():
        writer.write("__value__ = await __enter_async_generator__(__slots__, __value__)")
    writer.write("elif __kind__ is __Generator__:")
    with writer.block():
        writer.write("__value__ = __enter_generator__(__slots__, __value__)")


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
        _create_wrapper(
            func,
            store,
            cache_key,
            ignore_params,
            False,
            _write_enter_generator,
            {"__enter_generator__": enter_generator},
        ),
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
        _create_wrapper(
            func,
            store,
            cache_key,
            ignore_params,
            True,
            _write_enter_async_generator,
            {"__enter_async_generator__": enter_async_generator},
        ),
    )


def create_checked_wrapper(
    func: Callable[..., Any],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
    *,
    is_async: bool,
) -> Cached[..., Any, Any]:
    """
    Create a wrapper for a function whose decorator hides a generator function.

    The function may still return a generator, or the decorator may have turned it into
    something else. Each miss checks: a generator is entered and its yielded value cached,
    anything else is cached as it is.

    :param func: The decorated function.
    :param store: The owning scope's slot storage.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :param is_async: Whether the wrapper is async, so it can enter an async generator. A sync
        wrapper refuses one with
        {py:exc}`AsyncGeneratorInSyncScopeError
        <stratae.lifecycle.exceptions.AsyncGeneratorInSyncScopeError>`.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """

    def refuse_async() -> NoReturn:
        raise AsyncGeneratorInSyncScopeError(func, store.name)

    bindings: dict[str, Any] = {
        "__Generator__": GeneratorType,
        "__AsyncGenerator__": AsyncGeneratorType,
        "__enter_generator__": enter_generator,
    }
    if is_async:
        bindings["__enter_async_generator__"] = enter_async_generator
        write_value = _write_check_async
    else:
        bindings["__refuse_async__"] = refuse_async
        write_value = _write_check_sync
    return cast(
        Cached[..., Any, Any],
        _create_wrapper(func, store, cache_key, ignore_params, is_async, write_value, bindings),
    )
