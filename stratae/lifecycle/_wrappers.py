"""
Wrappers for lifecycle-managed functions and context managers.

Wrapper bodies are generated as source text and compiled rather than written as a
generic ``*args/**kwargs`` shim, so the compiled wrapper's signature and defaults
exactly match the original callable's, and so a cache hit costs one slot read
instead of a keyed dict lookup whenever the function's result doesn't vary by
argument values.
"""

import weakref
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Hashable, cast

from stratae.codegen import Writer, render_parameters
from stratae.codegen.util import wrapper_filename
from stratae.lifecycle._scope import UNSET, SharedVar

if TYPE_CHECKING:
    from stratae.lifecycle.lifecycle import AsyncLifecycle, Lifecycle


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


def _write_resolve_first_read(writer: Writer, lifecycle: Any, scope: str, read: str) -> None:
    """
    Write the fast-path resolution of __slots__ plus the first slot read into it.

    Which lookup is written depends on the scope's isolation (see _bind_slot_lookup for
    the matching namespace binding): a context-isolated scope's ContextVar is bound
    straight into the wrapper's namespace at codegen time, with the read following the
    resolve. A shared scope reads its SharedVar's storage attribute, unguarded - when
    the scope is inactive the attribute holds UNSET, whose subscript in the read raises
    TypeError, so the active path pays no explicit check. The read can't raise TypeError
    otherwise: it subscripts a list or SlotDict with an int constant. Either miss falls
    back to get_slots(), which raises the right error.
    """
    if isinstance(lifecycle._vars[scope], SharedVar):
        writer.write("__slots__ = __var__.storage")
        writer.write("try:")
        with writer.block():
            writer.write(read)
        writer.write("except TypeError:")
        with writer.block():
            writer.write(f"__slots__ = __lifecycle__.get_slots({scope!r})")
            writer.write(read)
        return
    writer.write("try:")
    with writer.block():
        writer.write("__slots__ = __var__.get()")
    writer.write("except LookupError:")
    with writer.block():
        writer.write(f"__slots__ = __lifecycle__.get_slots({scope!r})")
    writer.write(read)


def _build_namespace(func: Callable[..., Any], lifecycle: Any, scope: str) -> dict[str, Any]:
    """Build the namespace bindings every codegen'd wrapper compiles against."""
    return {
        "__func__": func,
        "__lifecycle__": lifecycle,
        "__UNSET__": UNSET,
        "__var__": lifecycle._vars[scope],
    }


def _write_slot_guard(writer: Writer, slot: int, lifecycle: Any, scope: str) -> None:
    """
    Write the guard that checks the dedicated slot for a slot-eligible call's direct value.

    Reads the slot into __value__ once, rather than indexing __slots__ again on both the
    hit-path return and the miss-path store. The caller opens the miss block.
    """
    _write_resolve_first_read(writer, lifecycle, scope, f"__value__ = __slots__[{slot}]")
    writer.write("if __value__ is __UNSET__:")


def _write_keyed_cache_guard(writer: Writer, slot: int, lifecycle: Any, scope: str) -> None:
    """
    Write the guard that resolves a keyed function's own dedicated cache dict.

    The dict is built lazily on first use: UNSET is cheaper to fill every slot with up
    front than allocating an empty dict for every keyed function in the scope whether or
    not it's ever actually called.
    """
    _write_resolve_first_read(writer, lifecycle, scope, f"__cache__ = __slots__[{slot}]")
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

    namespace = _build_namespace(func, lifecycle, scope)
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    wrapper = _finalize(writer, func, params, namespace)
    weakref.finalize(wrapper, lifecycle.release_slot, scope, slot)
    return wrapper


def create_sync_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: "Lifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    """
    Create a synchronous wrapper function that caches based on the lifecycle scope.

    Args:
        func: The function whose result should be cached.
        lifecycle: The `Lifecycle` whose scope owns the cached value.
        scope: Name of the scope the cached value lives in.
        cache_key: Callable deriving a hashable cache key from `func`'s
            arguments. When omitted, the key is `func`'s arguments
            themselves.
        ignore_params: Cache a single value per scope activation regardless
            of arguments, instead of keying by argument values.

    Returns:
        A wrapper matching `func`'s signature that returns the cached value
        for the current scope activation, computing and storing it on the
        first call.

    """
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
    """
    Create a synchronous wrapper function for use within an async lifecycle.

    Args:
        func: The function whose result should be cached.
        lifecycle: The `AsyncLifecycle` whose scope owns the cached value.
        scope: Name of the scope the cached value lives in.
        cache_key: Callable deriving a hashable cache key from `func`'s
            arguments. When omitted, the key is `func`'s arguments
            themselves.
        ignore_params: Cache a single value per scope activation regardless
            of arguments, instead of keying by argument values.

    Returns:
        A wrapper matching `func`'s signature that returns the cached value
        for the current scope activation, computing and storing it on the
        first call.

    """
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
    """
    Create an asynchronous wrapper function that caches based on the lifecycle scope.

    Args:
        func: The async function, or async generator, whose result should
            be cached.
        lifecycle: The `AsyncLifecycle` whose scope owns the cached value.
        scope: Name of the scope the cached value lives in.
        cache_key: Callable deriving a hashable cache key from `func`'s
            arguments. When omitted, the key is `func`'s arguments
            themselves.
        ignore_params: Cache a single value per scope activation regardless
            of arguments, instead of keying by argument values.

    Returns:
        An async wrapper matching `func`'s signature that returns the
        cached value for the current scope activation, computing and
        storing it on the first call.

    """
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

    namespace = _build_namespace(func, lifecycle, scope)
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    wrapper = cast(Callable[P, Awaitable[T]], _finalize(writer, func, params, namespace))
    weakref.finalize(wrapper, lifecycle.release_slot, scope, slot)
    return wrapper


def _write_resolve_exit_stack(writer: Writer) -> None:
    """
    Write the lazy resolution of the scope's exit stack from reserved slot 0 into __stack__.

    Only ever written on a context-manager wrapper's miss path, so the stack (an ExitStack
    or AsyncExitStack, chosen per lifecycle type via the __stack_type__ namespace binding)
    is created the first time this scope activation actually enters a context manager.
    """
    writer.write("__stack__ = __slots__[0]")
    writer.write("if __stack__ is __UNSET__:")
    with writer.block():
        writer.write("__stack__ = __slots__[0] = __stack_type__()")


def _write_cm_slot_body(
    writer: Writer, slot: int, lifecycle: Any, scope: str, params: list[Parameter], enter_expr: str
) -> None:
    """Write the whole body of a slot-eligible context-manager wrapper."""
    _write_slot_guard(writer, slot, lifecycle, scope)
    with writer.block():
        writer.write(f"__ctx__ = __func__({_render_forward_arguments(params)})")
        _write_resolve_exit_stack(writer)
        writer.write(f"__value__ = {enter_expr}")
        writer.write(f"__slots__[{slot}] = __value__")
    writer.write("return __value__")


def _write_cm_keyed_body(
    writer: Writer,
    slot: int,
    lifecycle: Any,
    scope: str,
    cache_key: Callable[..., Hashable] | None,
    params: list[Parameter],
    enter_expr: str,
) -> None:
    """Write the whole body of a keyed context-manager wrapper."""
    _write_keyed_cache_guard(writer, slot, lifecycle, scope)
    _write_key(writer, cache_key, params)
    _write_cache_check(writer)
    writer.write(f"__ctx__ = __func__({_render_forward_arguments(params)})")
    _write_resolve_exit_stack(writer)
    writer.write(f"__value__ = {enter_expr}")
    _write_cache_store(writer)


def _create_cm_wrapper_impl(
    func: Callable[..., Any],
    lifecycle: Any,
    scope: str,
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
    is_async: bool,
    enter_expr: str,
) -> Callable[..., Any]:
    """Build the codegen'd wrapper shared by the sync and async context-manager decorators."""
    params = list(signature(func).parameters.values())
    slot = lifecycle.allocate_slot(scope)

    writer = Writer()
    def_kw = "async def" if is_async else "def"
    writer.write(f"{def_kw} wrapper({render_parameters(params)}):")
    with writer.block():
        if _is_slot_eligible(params, cache_key, ignore_params):
            _write_cm_slot_body(writer, slot, lifecycle, scope, params, enter_expr)
        else:
            _write_cm_keyed_body(writer, slot, lifecycle, scope, cache_key, params, enter_expr)

    namespace = _build_namespace(func, lifecycle, scope)
    namespace["__stack_type__"] = lifecycle.exit_stack_type()
    if cache_key is not None:
        namespace["__cache_key__"] = cache_key
    wrapper = _finalize(writer, func, params, namespace)
    weakref.finalize(wrapper, lifecycle.release_slot, scope, slot)
    return wrapper


def create_synccm_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    lifecycle: "Lifecycle | AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    """
    Create a wrapper that enters a sync context manager and caches its yielded value.

    Used for functions decorated with `resource`: the underlying context
    manager is entered once per cache key and registered with the scope's
    exit stack, so it is exited when the owning scope deactivates rather
    than when the wrapper returns.

    Args:
        func: The context-manager-returning function to wrap.
        lifecycle: The `Lifecycle`/`AsyncLifecycle` whose scope owns the
            cached value and exit stack.
        scope: Name of the scope the cached value and exit stack live in.
        cache_key: Callable deriving a hashable cache key from `func`'s
            arguments. When omitted, the key is `func`'s arguments
            themselves.
        ignore_params: Cache a single value per scope activation regardless
            of arguments, instead of keying by argument values.

    Returns:
        A wrapper matching `func`'s signature that returns the entered
        value for the current scope activation, entering the context
        manager on the first call.

    """
    return cast(
        Callable[P, T],
        _create_cm_wrapper_impl(
            func,
            lifecycle,
            scope,
            cache_key,
            ignore_params,
            is_async=False,
            enter_expr="__stack__.enter_context(__ctx__)",
        ),
    )


def create_asynccm_wrapper[**P, T](
    func: Callable[P, AbstractAsyncContextManager[T]],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, Awaitable[T]]:
    """
    Create a wrapper that enters an async context manager and caches its yielded value.

    Used for functions decorated with `async_resource`: the underlying
    context manager is entered once per cache key and registered with the
    scope's exit stack, so it is exited when the owning scope deactivates
    rather than when the wrapper returns.

    Args:
        func: The async-context-manager-returning function to wrap.
        lifecycle: The `AsyncLifecycle` whose scope owns the cached value
            and exit stack.
        scope: Name of the scope the cached value and exit stack live in.
        cache_key: Callable deriving a hashable cache key from `func`'s
            arguments. When omitted, the key is `func`'s arguments
            themselves.
        ignore_params: Cache a single value per scope activation regardless
            of arguments, instead of keying by argument values.

    Returns:
        An async wrapper matching `func`'s signature that returns the
        entered value for the current scope activation, entering the
        context manager on the first call.

    """
    return cast(
        Callable[P, Awaitable[T]],
        _create_cm_wrapper_impl(
            func,
            lifecycle,
            scope,
            cache_key,
            ignore_params,
            is_async=True,
            enter_expr="await __stack__.enter_async_context(__ctx__)",
        ),
    )
