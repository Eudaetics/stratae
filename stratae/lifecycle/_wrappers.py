"""Wrappers for lifecycle-managed functions and context managers."""

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Hashable, cast

from stratae.codegen import Writer, render_parameters
from stratae.codegen.util import wrapper_filename
from stratae.lifecycle._scope import UNSET

if TYPE_CHECKING:
    from stratae.lifecycle.async_lifecycle import AsyncLifecycle
    from stratae.lifecycle.lifecycle import Lifecycle


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
    dedicated dict (see _write_keyed_cache_guard), so the key only distinguishes calls to
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


def _write_resolve_slots(writer: Writer, lifecycle: Any, scope: str) -> None:
    """
    Write the fast-path lookup of the active scope's slot list into __slots__.

    Which lookup is written depends on the scope's isolation (see _bind_slot_lookup for
    the matching namespace binding): a context-isolated scope's ContextVar is bound
    straight into the wrapper's namespace at codegen time, while a shared scope reads its
    entry from the manager's _active dict - that by-name lookup doubles as the "is this
    scope active" check, but the dict object itself never changes identity, so it's bound
    in as __active_map__.
    """
    is_context = scope in lifecycle._cvars
    writer.write("try:")
    with writer.block():
        if is_context:
            writer.write("__slots__ = __cv__.get()")
        else:
            writer.write(f"__slots__ = __active_map__[{scope!r}]")
    writer.write("except LookupError:" if is_context else "except KeyError:")
    with writer.block():
        writer.write(f"__slots__ = __lifecycle__.get_slots({scope!r})")


def _bind_slot_lookup(namespace: dict[str, Any], lifecycle: Any, scope: str) -> None:
    """
    Bind the slot-list lookup object matching _write_resolve_slots into the namespace.

    Context-isolated scopes get the scope's ContextVar as __cv__; shared scopes get the
    manager's _active dict as __active_map__.
    """
    cv = lifecycle._cvars.get(scope)
    if cv is not None:
        namespace["__cv__"] = cv
    else:
        namespace["__active_map__"] = lifecycle._active


def _write_slot_guard(writer: Writer, slot: int, lifecycle: Any, scope: str) -> None:
    """
    Write the guard that checks the dedicated slot for a slot-eligible call's direct value.

    Reads the slot into __value__ once, rather than indexing __slots__ again on both the
    hit-path return and the miss-path store. The caller opens the miss block.
    """
    _write_resolve_slots(writer, lifecycle, scope)
    writer.write(f"__value__ = __slots__[{slot}]")
    writer.write("if __value__ is __UNSET__:")


def _write_keyed_cache_guard(writer: Writer, slot: int, lifecycle: Any, scope: str) -> None:
    """
    Write the guard that resolves a keyed function's own dedicated cache dict.

    The dict is built lazily on first use: UNSET is cheaper to fill every slot with up
    front than allocating an empty dict for every keyed function in the scope whether or
    not it's ever actually called.
    """
    _write_resolve_slots(writer, lifecycle, scope)
    writer.write(f"__cache__ = __slots__[{slot}]")
    writer.write("if __cache__ is __UNSET__:")
    with writer.block():
        writer.write("__cache__ = {}")
        writer.write(f"__slots__[{slot}] = __cache__")


def _write_slot_body(writer: Writer, slot: int, lifecycle: Any, scope: str, call: str) -> None:
    """Write the whole body of a slot-eligible wrapper around the rendered call expression."""
    _write_slot_guard(writer, slot, lifecycle, scope)
    with writer.block():
        writer.write(f"__value__ = {call}")
        writer.write(f"__slots__[{slot}] = __value__")
    writer.write("return __value__")


def _write_keyed_body(
    writer: Writer,
    slot: int,
    lifecycle: Any,
    scope: str,
    cache_key: Callable[..., Hashable] | None,
    params: list[Parameter],
    call: str,
) -> None:
    """Write the whole body of a keyed wrapper around the rendered call expression."""
    _write_keyed_cache_guard(writer, slot, lifecycle, scope)
    _write_key(writer, cache_key, params)
    _write_cache_check(writer)
    writer.write(f"__value__ = {call}")
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
    return wrapper


def _create_sync_wrapper_impl(
    func: Callable[..., Any],
    lifecycle: Any,
    scope: str,
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
) -> Callable[..., Any]:
    """Build the codegen'd wrapper shared by the sync and sync-in-async cache decorators."""
    params = list(signature(func).parameters.values())
    slot = lifecycle.allocate_slot(scope)

    writer = Writer()
    writer.write(f"def wrapper({render_parameters(params)}):")
    with writer.block():
        call = f"__func__({_render_forward_arguments(params)})"
        if _is_slot_eligible(params, cache_key, ignore_params):
            _write_slot_body(writer, slot, lifecycle, scope, call)
        else:
            _write_keyed_body(writer, slot, lifecycle, scope, cache_key, params, call)

    namespace: dict[str, Any] = {
        "__func__": func,
        "__lifecycle__": lifecycle,
        "__UNSET__": UNSET,
    }
    _bind_slot_lookup(namespace, lifecycle, scope)
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    return _finalize(writer, func, params, namespace)


def create_sync_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: "Lifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    """Create a synchronous wrapper function that caches based on the lifecycle scope."""
    return cast(
        Callable[P, T],
        _create_sync_wrapper_impl(func, lifecycle, scope, cache_key, ignore_params),
    )


def create_sync_in_async_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    """Create a synchronous wrapper function for use within an async lifecycle."""
    return cast(
        Callable[P, T],
        _create_sync_wrapper_impl(func, lifecycle, scope, cache_key, ignore_params),
    )


def create_async_wrapper[**P, T](
    func: Callable[P, Awaitable[T] | AsyncGenerator[T, None]],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, Awaitable[T]]:
    """Create an asynchronous wrapper function that caches based on the lifecycle scope."""
    params = list(signature(func).parameters.values())
    slot = lifecycle.allocate_slot(scope)

    writer = Writer()
    writer.write(f"async def wrapper({render_parameters(params)}):")
    with writer.block():
        call = f"await __func__({_render_forward_arguments(params)})"
        if _is_slot_eligible(params, cache_key, ignore_params):
            _write_slot_body(writer, slot, lifecycle, scope, call)
        else:
            _write_keyed_body(writer, slot, lifecycle, scope, cache_key, params, call)

    namespace: dict[str, Any] = {
        "__func__": func,
        "__lifecycle__": lifecycle,
        "__UNSET__": UNSET,
    }
    _bind_slot_lookup(namespace, lifecycle, scope)
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    return cast(Callable[P, Awaitable[T]], _finalize(writer, func, params, namespace))


def _select_key_func(
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
):
    if cache_key is not None:

        def make_key_with_cache_key(args: tuple[Any, ...], kwargs: dict[str, Any]):
            return cache_key(*args, **kwargs)

        return make_key_with_cache_key
    elif ignore_params:

        def make_key_ignore_params(*_: Any):
            return None

        return make_key_ignore_params
    else:

        def make_key_default(args: tuple[Any, ...], kwargs: dict[str, Any]):
            return None if not (args or kwargs) else (args, frozenset(kwargs.items()))

        return make_key_default


def _resolve_cache(lifecycle: Any, scope: str, slot: int) -> dict[Hashable, Any]:
    """Get the function's dedicated cache dict from its slot, creating it on first use."""
    slots = lifecycle.get_slots(scope)
    cache: dict[Hashable, Any] = slots[slot]
    if cache is UNSET:
        cache = slots[slot] = {}
    return cache


def create_synccm_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    lifecycle: "Lifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        ctx = func(*args, **kwargs)
        value = lifecycle.get_exit_stack(scope).enter_context(ctx)
        cache[key] = value
        return value

    return gen_wrapper


def create_synccm_in_async_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        ctx = func(*args, **kwargs)
        value = lifecycle.get_exit_stack(scope).enter_context(ctx)
        cache[key] = value
        return value

    return gen_wrapper


def create_asynccm_wrapper[**P, T](
    func: Callable[P, AbstractAsyncContextManager[T]],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, Awaitable[T]]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    async def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        ctx = func(*args, **kwargs)
        value = await lifecycle.get_exit_stack(scope).enter_async_context(ctx)
        cache[key] = value
        return value

    return gen_wrapper
