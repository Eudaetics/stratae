"""
Wrappers for dependency injection in synchronous and asynchronous functions.

Wrapper bodies are generated as source text and compiled rather than written as a
generic *args/**kwargs shim, so the compiled wrapper's signature, defaults, and
annotations exactly match the dependency-free view of the original callable for
introspection by other tooling.
"""

from functools import wraps
from inspect import Parameter, Signature, isclass, signature
from typing import Any, Callable

from stratae.codegen import Writer, render_parameters, wrapper_filename
from stratae.depends.depends import DependsWrapper


def _kept_parameters(
    params: list[Parameter], resolved_deps: dict[str, DependsWrapper]
) -> list[Parameter]:
    """
    Return func's parameters minus the resolved dependencies.

    Resolved dependencies are supplied by the wrapper itself, not the caller, so they
    must not appear in the wrapper's public signature.
    """
    return [param for param in params if param.name not in resolved_deps]


def _render_dependency_source(name: str, dep: DependsWrapper) -> str:
    """
    Render the source expression that resolves a single dependency by name.

    A dependency's provide() may itself be sync or async. This adds await
    only if the dependency is async to prevent treating sync as async.
    """
    return f"{'await ' if dep.is_async else ''}__dep_{name}__.provide()"


def _render_argument_source(value: Any, param: Parameter):
    """
    Render a single call argument in the source text for the generated wrapper.

    A bare name would break *args, **kwargs, and keyword-only parameters, so each
    argument must be rendered in the same call convention as the original parameter.
    """
    if param.kind is Parameter.VAR_POSITIONAL:
        return f"*{value}"
    elif param.kind is Parameter.VAR_KEYWORD:
        return f"**{value}"
    elif param.kind is Parameter.KEYWORD_ONLY:
        return f"{param.name}={value}"
    else:
        return value


def _render_call_arguments(
    params: list[Parameter], resolved_deps: dict[str, DependsWrapper]
) -> str:
    """Build the exact call expression baked into the generated wrapper body."""
    args: list[str] = []
    for param in params:
        dep = resolved_deps.get(param.name)
        value = _render_dependency_source(param.name, dep) if dep is not None else param.name
        args.append(_render_argument_source(value, param))

    return ", ".join(args)


def _get_source(func: Callable[..., Any]):
    """
    Return the callable that carries func's real metadata.

    A class object has no __defaults__/__kwdefaults__/__annotations__ of its own;
    that metadata lives on __init__.
    """
    return func.__init__ if isclass(func) else func


def _get_annotations(params: list[Parameter]):
    """Return an __annotations__ mapping for the kept parameters."""
    return {
        param.name: param.annotation for param in params if param.annotation is not Parameter.empty
    }


def _finalize(
    writer: Writer,
    func: Callable[..., Any],
    kept: list[Parameter],
    resolved_deps: dict[str, DependsWrapper],
) -> Callable[..., Any]:
    """
    Compile the generated wrapper source and restore its metadata.

    exec() has no closures, so func and each dependency provider are threaded in by
    name through the namespace instead. Metadata is copied back afterward so tools
    that introspect the wrapper (help(), other DI/framework code) see it as the
    original callable, even though its runtime signature is the trimmed `kept` list.
    """
    namespace: dict[str, Any] = {
        "__func__": func,
        **{f"__dep_{name}__": dep for name, dep in resolved_deps.items()},
    }
    exec(compile(writer.render(), wrapper_filename(func), "exec"), namespace)  # noqa: S102

    metadata_source: Callable[..., Any] = _get_source(func)

    wrapper = namespace["wrapper"]
    wraps(func)(wrapper)
    wrapper.__signature__ = Signature(kept)
    wrapper.__defaults__ = metadata_source.__defaults__
    wrapper.__kwdefaults__ = metadata_source.__kwdefaults__
    wrapper.__annotations__ = _get_annotations(kept)
    if not isclass(func) and "return" in metadata_source.__annotations__:
        wrapper.__annotations__["return"] = metadata_source.__annotations__["return"]
    return wrapper


def _build_wrapper(
    func: Callable[..., Any],
    resolved_deps: dict[str, DependsWrapper],
    is_async: bool,
    write_body: Callable[[Writer, str], None],
):
    """Share the setup common to every wrapper kind."""
    params = list(signature(func).parameters.values())
    kept = _kept_parameters(params, resolved_deps)

    writer = Writer()
    writer.write(f"{'async ' if is_async else ''}def wrapper({render_parameters(kept)}):")
    with writer.block():
        write_body(writer, _render_call_arguments(params, resolved_deps))

    return _finalize(writer, func, kept, resolved_deps)


def create_sync_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """Create a synchronous wrapper; a plain call needs no await or yield handling."""
    return _build_wrapper(
        func, resolved_deps, False, lambda w, call: w.write(f"return __func__({call})")
    )


def create_sync_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """
    Create a synchronous generator wrapper.

    The call is delegated with yield from, rather than returned, so the wrapper
    itself stays a generator function; a bare return would collect no items.
    """
    return _build_wrapper(
        func, resolved_deps, False, lambda w, call: w.write(f"yield from __func__({call})")
    )


def create_async_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """
    Create an asynchronous wrapper.

    func must be awaited so the wrapper resolves to the coroutine's result, rather
    than returning an unawaited coroutine object to the caller.
    """
    return _build_wrapper(
        func, resolved_deps, True, lambda w, call: w.write(f"return await __func__({call})")
    )


def _write_async_gen_body(writer: Writer, call: str) -> None:
    """Write the body for an async generator wrapper."""
    writer.write(f"async for __item__ in __func__({call}):")
    with writer.block():
        writer.write("yield __item__")


def create_async_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """
    Create an async generator wrapper; see _write_async_gen_body for why it re-yields items.

    Async generators have no yield-from equivalent (there is no syntax for it), so
    items must be pulled and re-yielded one at a time via an async for loop.
    """
    return _build_wrapper(func, resolved_deps, True, write_body=_write_async_gen_body)
