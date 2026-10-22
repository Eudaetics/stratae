"""
Codegen'd wrappers for scope-cached functions and context managers.

Generated wrappers match the original signature exactly, and a function whose result
doesn't vary by argument reads one slot instead of a keyed dict.
"""

import threading
import weakref
from asyncio import current_task
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import Parameter, isasyncgenfunction, isgeneratorfunction
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Hashable,
    Literal,
    NoReturn,
    Protocol,
    cast,
)

from stratae.codegen import Writer, read_signature, render_parameters
from stratae.codegen.util import wrapper_filename
from stratae.lifecycle._inflight import (
    AsyncPending,
    Pending,
    WatchedClaim,
    wait_for_claim,
    watch_task_claim,
    watch_thread_claim,
)
from stratae.lifecycle._slots import UNSET
from stratae.lifecycle._stack import (
    enter_async_context,
    enter_async_generator,
    enter_context,
    enter_generator,
)
from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.resource import resource_generator

if TYPE_CHECKING:
    from stratae.lifecycle.scope import AsyncScope, BaseScope, Scope


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

    def write_read(self, writer: Writer, target: str, scope: "BaseScope | None" = None) -> None:
        """Write a read of this slot, resolving __slots__ first when `scope` is given."""
        read = f"{'__entry__' if self.owned else target} = __slots__[{self.index}]"
        if scope is None:
            writer.write(read)
        else:
            _write_resolve_first_read(writer, scope, read)

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


def _allocate_slot(scope: "BaseScope") -> _SlotCode:
    """Allocate a slot, owned if it's reused in a context scope."""
    index, reused = scope.allocate_slot()
    return _SlotCode(index, reused and not scope.is_shared())


def _is_slot_eligible(
    params: list[Parameter], cache_key: Callable[..., Hashable] | None, ignore_params: bool
) -> bool:
    """Whether a function caches one value per activation, stored directly in its slot."""
    return ignore_params or (cache_key is None and not params)


def _render_forward_arguments(params: list[Parameter]) -> str:
    """Render the call arguments that forward parameters to the wrapped function."""
    args: list[str] = []
    for param in params:
        if param.kind is Parameter.VAR_POSITIONAL:
            args.append(f"*{param.name}")
        elif param.kind is Parameter.VAR_KEYWORD:
            args.append(f"**{param.name}")
        elif param.kind is Parameter.KEYWORD_ONLY:
            args.append(f"{param.name}={param.name}")
        else:
            args.append(param.name)
    return ", ".join(args)


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
        return f"__cache_key__({_render_forward_arguments(params)})"
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


def _write_resolve_first_read(writer: Writer, scope: "BaseScope", read: str) -> None:
    """
    Write the resolution of __slots__ and the first slot read.

    No explicit active check is paid. A shared scope reads its SharedVar's storage directly;
    an inactive scope's UNSET raises TypeError on subscript. A context scope's `get()` raises
    LookupError when inactive. An ended activation's emptied storage raises IndexError or
    KeyError. Every miss falls back to get_slots(), which raises the right error.
    """
    if scope.is_shared():
        writer.write("__slots__ = __var__.storage")
        writer.write("try:")
        with writer.block():
            writer.write(read)
        writer.write("except (TypeError, LookupError):")
        with writer.block():
            writer.write("__slots__ = __scope__.get_slots()")
            writer.write(read)
        return
    writer.write("try:")
    with writer.block():
        writer.write("__slots__ = __var__.get()")
        writer.write(read)
    writer.write("except LookupError:")
    with writer.block():
        writer.write("__slots__ = __scope__.get_slots()")
        writer.write(read)


type ClaimStyle = Literal["locked", "task", "thread"] | None


def _claim_style(scope: "BaseScope", is_async: bool) -> ClaimStyle:
    """
    How a wrapper ensures overlapping callers compute each entry once.

    `"locked"`: shared scope; claims live in a per-wrapper dict behind a lock.
    `"thread"`: context scope with `threads=True`; like `"locked"`, keyed by activation.
    `"task"`: context scope, async wrapper; the owning task is the claim, stored lock-free
    in a per-wrapper dict keyed by activation. `None`: context scope, sync wrapper; one
    thread uses an activation at a time, so it skips claiming.
    """
    if scope.is_shared():
        return "locked"
    if scope.threads:
        return "thread"
    return "task" if is_async else None


def _raise_ended(scope: "BaseScope") -> NoReturn:
    """Raise for a claim waiter whose activation ended while it was waiting."""
    raise ScopeInactiveError(f"Scope {scope.name!r} is not active: its activation ended.")


def _build_namespace(
    func: Callable[..., Any], scope: "BaseScope", slot: _SlotCode, is_async: bool = False
) -> dict[str, Any]:
    """Build the namespace bindings every codegen'd wrapper compiles against."""
    namespace: dict[str, Any] = {
        "__func__": func,
        "__scope__": scope,
        "__UNSET__": UNSET,
        "__var__": scope.activation_var(),
        **slot.namespace(),
    }
    style = _claim_style(scope, is_async)
    if style == "locked" or style == "thread":
        # A thread lock even for async wrappers: it never guards an await.
        lock = threading.Lock()
        namespace["__acquire__"] = lock.acquire
        namespace["__release__"] = lock.release
        namespace["__inflight__"] = {}
        namespace["__Pending__"] = AsyncPending if is_async else Pending
        namespace["__claimant__"] = current_task if is_async else threading.get_ident
        namespace["__watch__"] = watch_task_claim if is_async else watch_thread_claim
        namespace["__ended__"] = _raise_ended
    if style == "thread":
        namespace["__id__"] = id
    elif style == "task":
        namespace["__id__"] = id
        namespace["__claims__"] = {}
        namespace["__task__"] = current_task
        namespace["__wait_claim__"] = wait_for_claim
        namespace["__Watched__"] = WatchedClaim
        namespace["__ended__"] = _raise_ended
    return namespace


def _write_new_cache(writer: Writer, slot: _SlotCode) -> None:
    """Write the lazy creation of a keyed function's cache dict into its slot."""
    writer.write("__cache__ = {}")
    writer.write(slot.write("__cache__"))


def _write_slot_store(writer: Writer, slot: _SlotCode, scope: "BaseScope") -> None:
    """
    Write the store of __value__ into a slot-eligible function's slot.

    An activation that ended mid-computation must not be refilled. An emptied list raises
    IndexError for free; an emptied SlotDict wouldn't, so sparse stores check first.
    """
    if scope.storage == "dense":
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
            writer.write("__ended__(__scope__)")
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
            writer.write("__ended__(__scope__)")
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


def _write_claimed_slot_miss(
    writer: Writer,
    slot: _SlotCode,
    scope: "BaseScope",
    style: ClaimStyle,
    is_async: bool,
    write_compute: Callable[[], None],
) -> None:
    """
    Write a slot miss computed once across overlapping callers.

    A shared scope claims under `None`; a `"task"` or `"thread"` wrapper claims under the
    activation's id. The id is unique while claimed, since the claimant holds the storage.
    """

    def write_recheck() -> None:
        slot.write_read(writer, "__value__")
        slot.write_branch(writer, "__value__", write_hit=lambda: writer.write("return __value__"))

    def write_store() -> None:
        _write_slot_store(writer, slot, scope)

    if style == "task":
        writer.write("__pk__ = __id__(__slots__)")
        _write_task_claimed_compute(writer, "__pk__", write_recheck, write_compute, write_store)
        return
    key = "None"
    if style == "thread":
        writer.write("__pk__ = __id__(__slots__)")
        key = "__pk__"
    _write_claimed_compute(writer, key, is_async, write_recheck, write_compute, write_store)


def _write_keyed_body(
    writer: Writer,
    slot: _SlotCode,
    scope: "BaseScope",
    cache_key: Callable[..., Hashable] | None,
    params: list[Parameter],
    write_compute: Callable[[], None],
    is_async: bool = False,
) -> None:
    """
    Write the body of a keyed wrapper; write_compute fills __value__.

    A claiming wrapper computes each key once across overlapping callers. Unless it's a
    `"task"` wrapper, the dict's lazy creation happens under the lock, since two threads
    could otherwise each build one and lose inserts.
    """
    style = _claim_style(scope, is_async)
    if style is None:
        slot.write_read(writer, "__cache__", scope)
        slot.write_branch(writer, "__cache__", write_miss=lambda: _write_new_cache(writer, slot))
        _write_key(writer, cache_key, params)
        _write_cache_check(writer)
        write_compute()
        writer.write("__cache__[__ck__] = __value__")
        writer.write("return __value__")
        return
    slot.write_read(writer, "__cache__", scope)
    _write_key(writer, cache_key, params)
    write_check = _write_shared_cache_check if style == "locked" else _write_cache_check
    slot.write_branch(
        writer,
        "__cache__",
        write_hit=lambda: write_check(writer),
        write_miss=(lambda: _write_new_cache(writer, slot)) if style == "task" else None,
    )

    def write_store() -> None:
        writer.write("__cache__[__ck__] = __value__")

    if style == "task":
        writer.write("__pk__ = (__id__(__slots__), __ck__)")
        _write_task_claimed_compute(
            writer, "__pk__", lambda: _write_cache_check(writer), write_compute, write_store
        )
    else:

        def write_recheck() -> None:
            slot.write_read(writer, "__cache__")
            slot.write_branch(
                writer, "__cache__", write_miss=lambda: _write_new_cache(writer, slot)
            )
            _write_cache_check(writer)

        key = "__ck__"
        if style == "thread":
            writer.write("__pk__ = (__id__(__slots__), __ck__)")
            key = "__pk__"
        _write_claimed_compute(writer, key, is_async, write_recheck, write_compute, write_store)
    writer.write("return __value__")


def _write_slot_body(
    writer: Writer,
    slot: _SlotCode,
    scope: "BaseScope",
    write_compute: Callable[[], None],
    is_async: bool = False,
) -> None:
    """Write the body of a slot-eligible wrapper; write_compute fills __value__."""
    style = _claim_style(scope, is_async)

    def write_miss() -> None:
        if style is not None:
            _write_claimed_slot_miss(writer, slot, scope, style, is_async, write_compute)
        else:
            write_compute()
            _write_slot_store(writer, slot, scope)

    slot.write_read(writer, "__value__", scope)
    slot.write_branch(writer, "__value__", write_miss=write_miss)
    writer.write("return __value__")


def _aggregate_defaults(params: list[Parameter]):
    """Build the wrapper's __defaults__ from its positional parameters."""
    return (
        tuple(
            param.default
            for param in params
            if param.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
            and param.default is not Parameter.empty
        )
        or None
    )


def _aggregate_kwdefaults(params: list[Parameter]):
    """Build the wrapper's __kwdefaults__ from its keyword-only parameters."""
    return {
        param.name: param.default
        for param in params
        if param.kind is Parameter.KEYWORD_ONLY and param.default is not Parameter.empty
    } or None


def _finalize(
    writer: Writer, func: Callable[..., Any], params: list[Parameter], namespace: dict[str, Any]
) -> Callable[..., Any]:
    """Compile the wrapper source and restore its metadata and defaults."""
    exec(compile(writer.render(), wrapper_filename(func), "exec"), namespace)  # noqa: S102

    wrapper = namespace["wrapper"]
    wraps(func)(wrapper)
    wrapper.__defaults__ = _aggregate_defaults(params)
    wrapper.__kwdefaults__ = _aggregate_kwdefaults(params)
    wrapper.uncached = func
    return wrapper


def _create_sync_wrapper_impl(
    func: Callable[..., Any],
    scope: "BaseScope",
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
) -> Callable[..., Any]:
    """Build the wrapper shared by the sync and sync-in-async decorators."""
    params = list(read_signature(func).parameters.values())
    slot = _allocate_slot(scope)

    writer = Writer()
    writer.write(f"def wrapper({render_parameters(params)}):")
    with writer.block():
        call = f"__func__({_render_forward_arguments(params)})"

        def write_call() -> None:
            writer.write(f"__value__ = {call}")

        if _is_slot_eligible(params, cache_key, ignore_params):
            _write_slot_body(writer, slot, scope, write_call)
        else:
            _write_keyed_body(writer, slot, scope, cache_key, params, write_call)

    namespace = _build_namespace(func, scope, slot)
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    wrapper = _finalize(writer, func, params, namespace)
    weakref.finalize(wrapper, scope.release_slot, slot.index)
    return wrapper


def create_sync_wrapper[**P, T](
    func: Callable[P, T],
    scope: "Scope",
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, T, T]:
    """
    Create a sync wrapper that caches `func`'s result per scope activation.

    :param func: The function to cache.
    :param scope: The owning {py:class}`Scope <stratae.lifecycle.scope.Scope>`.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    return cast(
        Cached[P, T, T],
        _create_sync_wrapper_impl(func, scope, cache_key, ignore_params),
    )


def create_sync_in_async_wrapper[**P, T](
    func: Callable[P, T],
    scope: "AsyncScope",
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, T, T]:
    """
    Create a sync wrapper that caches `func`'s result in an async scope.

    :param func: The function to cache.
    :param scope: The owning {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    return cast(
        Cached[P, T, T],
        _create_sync_wrapper_impl(func, scope, cache_key, ignore_params),
    )


def create_async_wrapper[**P, T](
    func: Callable[P, Awaitable[T]],
    scope: "AsyncScope",
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, Awaitable[T], Awaitable[T]]:
    """
    Create an async wrapper that caches `func`'s result per scope activation.

    :param func: The async function to cache.
    :param scope: The owning {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    params = list(read_signature(func).parameters.values())
    slot = _allocate_slot(scope)

    writer = Writer()
    writer.write(f"async def wrapper({render_parameters(params)}):")
    with writer.block():
        call = f"await __func__({_render_forward_arguments(params)})"

        def write_call() -> None:
            writer.write(f"__value__ = {call}")

        if _is_slot_eligible(params, cache_key, ignore_params):
            _write_slot_body(writer, slot, scope, write_call, is_async=True)
        else:
            _write_keyed_body(writer, slot, scope, cache_key, params, write_call, is_async=True)

    namespace = _build_namespace(func, scope, slot, is_async=True)
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    wrapper = cast(
        Cached[P, Awaitable[T], Awaitable[T]],
        _finalize(writer, func, params, namespace),
    )
    weakref.finalize(wrapper, scope.release_slot, slot.index)
    return wrapper


def _create_cm_wrapper_impl(
    func: Callable[..., Any],
    scope: "BaseScope",
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
    is_async: bool,
    enter: Callable[..., Any],
    target: Callable[..., Any] | None = None,
) -> Callable[..., Any]:
    """
    Build the wrapper shared by the sync and async context-manager decorators.

    `enter` registers the made object in the activation and returns its value. `target`,
    when given, is called in place of `func` to make that object.
    """
    params = list(read_signature(func).parameters.values())
    slot = _allocate_slot(scope)

    writer = Writer()
    def_kw = "async def" if is_async else "def"

    def write_enter() -> None:
        writer.write(f"__ctx__ = __func__({_render_forward_arguments(params)})")
        writer.write(f"__value__ = {'await ' if is_async else ''}__enter__(__slots__, __ctx__)")

    writer.write(f"{def_kw} wrapper({render_parameters(params)}):")
    with writer.block():
        if _is_slot_eligible(params, cache_key, ignore_params):
            _write_slot_body(writer, slot, scope, write_enter, is_async)
        else:
            _write_keyed_body(writer, slot, scope, cache_key, params, write_enter, is_async)

    namespace = _build_namespace(target or func, scope, slot, is_async=is_async)
    namespace["__enter__"] = enter
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    wrapper = _finalize(writer, func, params, namespace)
    weakref.finalize(wrapper, scope.release_slot, slot.index)
    return wrapper


def create_synccm_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    scope: "Scope | AsyncScope",
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, T, AbstractContextManager[T]]:
    """
    Create a wrapper that enters a sync context manager and caches its yielded value.

    The context manager is exited when the scope's activation ends. A function returned
    directly by `resource` has its generator driven in place of the context manager.

    :param func: The `resource`-tagged function to wrap.
    :param scope: The owning {py:class}`Scope <stratae.lifecycle.scope.Scope>` or
        {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
    """
    generator = resource_generator(func)
    if generator is not None and isgeneratorfunction(generator):
        enter: Callable[..., Any] = enter_generator
    else:
        generator, enter = None, enter_context
    return cast(
        Cached[P, T, AbstractContextManager[T]],
        _create_cm_wrapper_impl(func, scope, cache_key, ignore_params, False, enter, generator),
    )


def create_asynccm_wrapper[**P, T](
    func: Callable[P, AbstractAsyncContextManager[T]],
    scope: "AsyncScope",
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, Awaitable[T], AbstractAsyncContextManager[T]]:
    """
    Create a wrapper that enters an async context manager and caches its yielded value.

    The context manager is exited when the scope's activation ends. A function returned
    directly by `async_resource` has its generator driven in place of the context manager.

    :param func: The `async_resource`-tagged function to wrap.
    :param scope: The owning {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`.
    :param cache_key: Derives a hashable key from the arguments.
    :param ignore_params: Cache one value per activation regardless of arguments.
    :returns: An async wrapper matching `func`'s signature, with `func` as its `uncached`
        attribute.
    """
    generator = resource_generator(func)
    if generator is not None and isasyncgenfunction(generator):
        enter: Callable[..., Any] = enter_async_generator
    else:
        generator, enter = None, enter_async_context
    return cast(
        Cached[P, Awaitable[T], AbstractAsyncContextManager[T]],
        _create_cm_wrapper_impl(func, scope, cache_key, ignore_params, True, enter, generator),
    )
