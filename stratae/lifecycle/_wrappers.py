"""
Wrappers for scope-managed functions and context managers.

Wrapper bodies are generated as source text and compiled rather than written as a
generic ``*args/**kwargs`` shim, so the compiled wrapper's signature and defaults
exactly match the original callable's, and so a cache hit costs one slot read
instead of a keyed dict lookup whenever the function's result doesn't vary by
argument values.
"""

import threading
import weakref
from asyncio import current_task
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import Parameter
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
from stratae.lifecycle._inflight import AsyncPending, Pending, WatchedClaim, wait_for_claim
from stratae.lifecycle._slots import UNSET
from stratae.lifecycle.exceptions import ScopeInactiveError

if TYPE_CHECKING:
    from stratae.lifecycle.scope import AsyncScope, BaseScope, Scope


class Cached[**P, T, U](Protocol):
    """
    The shape of a function returned by `Scope.cache`/`AsyncScope.cache`.

    Calling it directly (`__call__`) returns the cached value, computing it on the first
    call within the current scope activation. `uncached` is the original, pre-decoration
    function itself - unentered (for a `resource`/`async_resource`-tagged function) and
    uncached, callable whether or not the owning scope is active. Use it for one-off calls
    where you know the scope won't be active, or don't want the result cached.
    """

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Return the cached value for the current scope activation."""
        ...

    def uncached(self, *args: P.args, **kwargs: P.kwargs) -> U:
        """Call the original function directly, bypassing caching and scope activation."""
        ...


class _SlotCode:
    """
    Source text for reading and writing one wrapper's dedicated slot.

    A reused slot in a context scope can still hold a dead wrapper's value in other live
    activations, so an owned slot boxes its values in its own `__Owned__` type and treats
    anything else as a miss.
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
    """
    Determine whether a function's result never varies by argument values.

    That's true when ignore_params is set, or when there's no custom cache_key and the
    function takes no parameters at all - both cases cache exactly one value per scope
    activation, so the value can live directly in the function's slot with no dict.
    """
    return ignore_params or (cache_key is None and not params)


def _render_forward_arguments(params: list[Parameter]) -> str:
    """Render the call arguments used to forward parameters directly to the wrapped function."""
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
    Render the source expression used as the cache key for a non-slot-eligible function.

    The key never folds in which function it's for: each keyed function has its own
    dedicated dict (see _write_new_cache), so the key only distinguishes calls to
    *that* function. A custom cache_key is called with the parameters forwarded directly.
    Otherwise the key is a flat tuple of every parameter's local, in signature order -
    keyword-only params are ordinary locals, and only a true **kwargs (whose names vary
    call to call) needs the frozenset(items()) treatment. Flat is unambiguous: only *args
    varies the tuple's length, so equal keys always align the same values to the same
    parameters. A single-parameter function skips the tuple wrapper entirely - its bare
    local already distinguishes calls (and a lone *args local is already a tuple).
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
    """Write the __ck__ assignment once, reused by both the hit check and the miss store."""
    writer.write(f"__ck__ = {_render_key_expression(cache_key, params)}")


def _write_cache_check(writer: Writer) -> None:
    """Write the cache-hit fast path: return immediately if __ck__ is already cached."""
    writer.write("if __ck__ in __cache__:")
    with writer.block():
        writer.write("return __cache__[__ck__]")


def _write_cache_store(writer: Writer) -> None:
    """Write the cache-miss tail: store __value__ under __ck__ and return it."""
    writer.write("__cache__[__ck__] = __value__")
    writer.write("return __value__")


def _write_resolve_first_read(writer: Writer, scope: "BaseScope", read: str) -> None:
    """
    Write the fast-path resolution of __slots__ plus the first slot read into it.

    Which lookup is written depends on the scope's isolation (see _build_namespace for
    the matching namespace binding). A shared scope reads its SharedVar's storage attribute,
    unguarded - when the scope is inactive the attribute holds UNSET, whose subscript in the
    read raises TypeError, so the active path pays no explicit check. Storage read just
    before another thread deactivates the scope is emptied under it, so the read raises
    IndexError or KeyError instead. A context-isolated scope's ContextVar is bound straight
    into the wrapper's namespace at codegen time, and its get() shares one try with the
    read: an inactive scope raises LookupError from get(), and an ended activation's
    emptied storage raises IndexError or KeyError - both LookupErrors - from the read. Any
    miss falls back to get_slots(), which raises the right error.
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


type ClaimStyle = Literal["locked", "task"] | None


def _claim_style(scope: "BaseScope", is_async: bool) -> ClaimStyle:
    """
    How a wrapper claims its misses so overlapping callers compute each entry once, if at all.

    `"locked"`: a shared scope's callers can be on any thread, so its claims live in a
    per-wrapper dict guarded by a lock. `"task"`: a context scope's async wrapper - callers
    overlapping one activation are its child tasks, sharing one event loop. Its claim
    bookkeeping never awaits, so it needs no lock, and it lives in the activation's own
    claims dict, with the owning task itself as the claim. `None`: a context scope's sync
    wrapper computes without suspending, so only threads running a copied context could
    overlap it - it skips claiming to keep that miss path free of the bookkeeping.
    """
    if scope.is_shared():
        return "locked"
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
    if style == "locked":
        # A thread lock even for async wrappers: it only guards claim/store bookkeeping,
        # never an await, so it can't block the event loop for long.
        lock = threading.Lock()
        namespace["__acquire__"] = lock.acquire
        namespace["__release__"] = lock.release
        namespace["__inflight__"] = {}
        namespace["__Pending__"] = AsyncPending if is_async else Pending
        namespace["__ended__"] = _raise_ended
    elif style == "task":
        namespace["__task__"] = current_task
        namespace["__wait_claim__"] = wait_for_claim
        namespace["__Watched__"] = WatchedClaim
        namespace["__ended__"] = _raise_ended
    return namespace


def _write_new_cache(writer: Writer, slot: _SlotCode) -> None:
    """
    Write the creation of a keyed function's own dedicated cache dict into its slot.

    The dict is built lazily on first use: UNSET is cheaper to fill every slot with up
    front than allocating an empty dict for every keyed function in the scope whether or
    not it's ever actually called.
    """
    writer.write("__cache__ = {}")
    writer.write(slot.write("__cache__"))


def _write_slot_store(writer: Writer, slot: _SlotCode, scope: "BaseScope") -> None:
    """
    Write the store of __value__ into a slot-eligible function's slot.

    A computation still running when its activation ends finds that storage emptied, and
    mustn't refill it: a later call reaching the ended storage would hit the value instead
    of raising. Into an emptied list the store raises IndexError, caught at no cost to a
    live store; an emptied SlotDict would accept it, so a sparse store checks first.
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
    """
    Write `write_body` guarded by the wrapper's lock.

    Written as acquire/try/finally rather than `with`, which costs about twice as much.
    """
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
    """Write a shared scope's lock-guarded claim-then-compute miss path."""

    def write_claim() -> None:
        write_recheck()
        writer.write(f"__pending__ = __inflight__.get({key})")
        writer.write("if __pending__ is None:")
        with writer.block():
            writer.write(f"__inflight__[{key}] = __Pending__()")
            writer.write("break")
        writer.write("__done__ = __pending__.watch()")

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
        _write_locked(writer, lambda: writer.write(f"__inflight__.pop({key}).release()"))


def _write_task_claimed_compute(
    writer: Writer,
    key: str,
    write_recheck: Callable[[], None],
    write_compute: Callable[[], None],
    write_store: Callable[[], None],
) -> None:
    """
    Write a context scope's lockless claim-then-compute miss path, for an async wrapper.

    Only reached right after a miss, with no await since, so there's nothing to re-check
    until a wait. The claim is the calling task itself in the activation's claims dict,
    replaced by a WatchedClaim only once another task waits on it - so an uncontended miss
    adds a few dict operations and no allocation.
    """
    writer.write("__claims__ = __slots__[0].claims")
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
    Write a claiming slot miss, computed once across overlapping callers.

    Only reached once the lock-free fast-path read outside already saw UNSET. A shared
    scope's wrapper claims under `None`, since the slot holds one value per activation and
    its claims are its own; a context scope's claims share the activation's dict, so they
    claim under the slot number.
    """

    def write_recheck() -> None:
        slot.write_read(writer, "__value__")
        slot.write_branch(writer, "__value__", write_hit=lambda: writer.write("return __value__"))

    def write_store() -> None:
        _write_slot_store(writer, slot, scope)

    if style == "task":
        _write_task_claimed_compute(
            writer, str(slot.index), write_recheck, write_compute, write_store
        )
    else:
        _write_claimed_compute(writer, "None", is_async, write_recheck, write_compute, write_store)


def _write_claimed_keyed_body(
    writer: Writer,
    slot: _SlotCode,
    scope: "BaseScope",
    cache_key: Callable[..., Hashable] | None,
    params: list[Parameter],
    style: ClaimStyle,
    is_async: bool,
    write_compute: Callable[[], None],
) -> None:
    """
    Write a claiming keyed cache access, computing each key once across overlapping callers.

    Claims are per key, so computing one key never waits on another. In a shared scope the
    dict's lazy creation moves inside the lock too: two callers on different threads can
    each see the slot as UNSET and each build a fresh dict, silently discarding whichever's
    inserts came first, so only the hit check stays lock-free. A context scope's tasks can't
    interleave before claiming, so they build it without one. write_compute writes the
    result into __value__.
    """
    slot.write_read(writer, "__cache__", scope)
    _write_key(writer, cache_key, params)
    slot.write_branch(
        writer,
        "__cache__",
        write_hit=lambda: _write_cache_check(writer),
        write_miss=(lambda: _write_new_cache(writer, slot)) if style == "task" else None,
    )

    def write_store() -> None:
        writer.write("__cache__[__ck__] = __value__")

    if style == "task":
        writer.write(f"__pk__ = ({slot.index}, __ck__)")
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

        _write_claimed_compute(
            writer, "__ck__", is_async, write_recheck, write_compute, write_store
        )
    writer.write("return __value__")


def _write_slot_body(
    writer: Writer,
    slot: _SlotCode,
    scope: "BaseScope",
    write_compute: Callable[[], None],
    is_async: bool = False,
) -> None:
    """
    Write the whole body of a slot-eligible wrapper.

    write_compute writes the result into __value__ - a plain call for a function, a
    context-manager entry for a resource.
    """
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
    Write the whole body of a keyed wrapper.

    write_compute writes the result into __value__ - a plain call for a function, a
    context-manager entry for a resource.
    """
    style = _claim_style(scope, is_async)
    if style is not None:
        _write_claimed_keyed_body(
            writer, slot, scope, cache_key, params, style, is_async, write_compute
        )
        return
    slot.write_read(writer, "__cache__", scope)
    slot.write_branch(writer, "__cache__", write_miss=lambda: _write_new_cache(writer, slot))
    _write_key(writer, cache_key, params)
    _write_cache_check(writer)
    write_compute()
    _write_cache_store(writer)


def _aggregate_defaults(params: list[Parameter]):
    """Build the __defaults__ tuple for the wrapper from its positional parameters' defaults."""
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
    """Build the __kwdefaults__ dict for the wrapper from its keyword-only parameters' defaults."""
    return {
        param.name: param.default
        for param in params
        if param.kind is Parameter.KEYWORD_ONLY and param.default is not Parameter.empty
    } or None


def _finalize(
    writer: Writer, func: Callable[..., Any], params: list[Parameter], namespace: dict[str, Any]
) -> Callable[..., Any]:
    """
    Compile the generated wrapper source and restore its metadata and defaults.

    render_parameters emits the signature without defaults, so they are restored through
    __defaults__/__kwdefaults__ for calls that omit arguments.
    """
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
    """Build the codegen'd wrapper shared by the sync and sync-in-async cache decorators."""
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
    Create a synchronous wrapper function that caches based on the scope's activation.

    :param func: The function whose result should be cached.
    :param scope: The {py:class}`Scope <stratae.lifecycle.scope.Scope>` that owns the cached value.
    :param cache_key: Callable deriving a hashable cache key from `func`'s arguments. When omitted,
        the key is `func`'s arguments themselves.
    :param ignore_params: Cache a single value per scope activation regardless of arguments, instead
        of keying by argument values.
    :returns: A wrapper matching `func`'s signature that returns the cached value for the current
        scope activation, computing and storing it on the first call. Its `uncached` attribute is
        `func` itself.
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
    Create a synchronous wrapper function for use within an async scope.

    :param func: The function whose result should be cached.
    :param scope: The {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` that owns the
        cached value.
    :param cache_key: Callable deriving a hashable cache key from `func`'s arguments. When omitted,
        the key is `func`'s arguments themselves.
    :param ignore_params: Cache a single value per scope activation regardless of arguments, instead
        of keying by argument values.
    :returns: A wrapper matching `func`'s signature that returns the cached value for the current
        scope activation, computing and storing it on the first call. Its `uncached` attribute is
        `func` itself.
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
    Create an asynchronous wrapper function that caches based on the scope's activation.

    :param func: The async function whose result should be cached.
    :param scope: The {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` that owns the
        cached value.
    :param cache_key: Callable deriving a hashable cache key from `func`'s arguments. When omitted,
        the key is `func`'s arguments themselves.
    :param ignore_params: Cache a single value per scope activation regardless of arguments, instead
        of keying by argument values.
    :returns: An async wrapper matching `func`'s signature that returns the cached value for the
        current scope activation, computing and storing it on the first call. Its `uncached`
        attribute is `func` itself.
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


def _write_resolve_exit_stack(writer: Writer) -> None:
    """
    Write the read of the scope's exit stack from reserved slot 0 into __stack__.

    Slot 0 is populated eagerly when the scope activates (see Scope.activate/
    AsyncScope.activate), so this is a plain read - never UNSET.
    """
    writer.write("__stack__ = __slots__[0]")


def _create_cm_wrapper_impl(
    func: Callable[..., Any],
    scope: "BaseScope",
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
    is_async: bool,
    enter_expr: str,
) -> Callable[..., Any]:
    """Build the codegen'd wrapper shared by the sync and async context-manager decorators."""
    params = list(read_signature(func).parameters.values())
    slot = _allocate_slot(scope)

    writer = Writer()
    def_kw = "async def" if is_async else "def"

    def write_enter() -> None:
        writer.write(f"__ctx__ = __func__({_render_forward_arguments(params)})")
        _write_resolve_exit_stack(writer)
        writer.write(f"__value__ = {enter_expr}")

    writer.write(f"{def_kw} wrapper({render_parameters(params)}):")
    with writer.block():
        if _is_slot_eligible(params, cache_key, ignore_params):
            _write_slot_body(writer, slot, scope, write_enter, is_async)
        else:
            _write_keyed_body(writer, slot, scope, cache_key, params, write_enter, is_async)

    namespace = _build_namespace(func, scope, slot, is_async=is_async)
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

    Used for functions decorated with `resource`: the underlying context
    manager is entered once per cache key and registered with the scope's
    exit stack, so it is exited when the owning scope deactivates rather
    than when the wrapper returns.

    :param func: The context-manager-returning function to wrap.
    :param scope: The {py:class}`Scope <stratae.lifecycle.scope.Scope>`/
        {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` that owns the cached value
        and exit stack.
    :param cache_key: Callable deriving a hashable cache key from `func`'s arguments. When omitted,
        the key is `func`'s arguments themselves.
    :param ignore_params: Cache a single value per scope activation regardless of arguments, instead
        of keying by argument values.
    :returns: A wrapper matching `func`'s signature that returns the entered value for the current
        scope activation, entering the context manager on the first call. Its `uncached` attribute
        is `func` itself - the unentered context manager function.
    """
    return cast(
        Cached[P, T, AbstractContextManager[T]],
        _create_cm_wrapper_impl(
            func,
            scope,
            cache_key,
            ignore_params,
            is_async=False,
            enter_expr="__stack__.enter_context(__ctx__)",
        ),
    )


def create_asynccm_wrapper[**P, T](
    func: Callable[P, AbstractAsyncContextManager[T]],
    scope: "AsyncScope",
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Cached[P, Awaitable[T], AbstractAsyncContextManager[T]]:
    """
    Create a wrapper that enters an async context manager and caches its yielded value.

    Used for functions decorated with `async_resource`: the underlying
    context manager is entered once per cache key and registered with the
    scope's exit stack, so it is exited when the owning scope deactivates
    rather than when the wrapper returns.

    :param func: The async-context-manager-returning function to wrap.
    :param scope: The {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` that owns the
        cached value and exit stack.
    :param cache_key: Callable deriving a hashable cache key from `func`'s arguments. When omitted,
        the key is `func`'s arguments themselves.
    :param ignore_params: Cache a single value per scope activation regardless of arguments, instead
        of keying by argument values.
    :returns: An async wrapper matching `func`'s signature that returns the entered value for the
        current scope activation, entering the context manager on the first call. Its `uncached`
        attribute is `func` itself - the unentered async context manager function.
    """
    return cast(
        Cached[P, Awaitable[T], AbstractAsyncContextManager[T]],
        _create_cm_wrapper_impl(
            func,
            scope,
            cache_key,
            ignore_params,
            is_async=True,
            enter_expr="await __stack__.enter_async_context(__ctx__)",
        ),
    )
