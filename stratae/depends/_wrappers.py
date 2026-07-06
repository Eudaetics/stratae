"""Wrappers for dependency injection in synchronous and asynchronous functions."""

from functools import wraps
from inspect import Parameter, Signature, isclass, signature
from typing import Any, Callable

from stratae.codegen import Writer, render_parameters
from stratae.depends.depends import DependsWrapper


def _kept_parameters(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> list[Parameter]:
    """Return func's parameters that are not resolved dependencies, preserving order."""
    return [param for key, param in signature(func).parameters.items() if key not in resolved_deps]


def _render_dependency_source(name: str, dep: DependsWrapper) -> str:
    """Render the source expression that resolves a single dependency by name."""
    return (
        f"(await __dep_{name}__.dependency())" if dep.is_async else f"__dep_{name}__.dependency()"
    )


def _render_argument_source(value: Any, param: Parameter):
    if param.kind is Parameter.VAR_POSITIONAL:
        return f"*{value}"
    elif param.kind is Parameter.VAR_KEYWORD:
        return f"**{value}"
    elif param.kind is Parameter.KEYWORD_ONLY:
        return f"{param.name}={value}"
    else:
        return value


def _render_call_arguments(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> str:
    """Render the call arguments passed to the wrapped function."""
    args: list[str] = []
    for param in signature(func).parameters.values():
        dep = resolved_deps.get(param.name)
        value = _render_dependency_source(param.name, dep) if dep is not None else param.name
        args.append(_render_argument_source(value, param))

    return ", ".join(args)


def _get_source(func: Callable[..., Any]):
    return func.__init__ if isclass(func) else func


def _get_annotations(params: list[Parameter]):
    return {
        param.name: param.annotation for param in params if param.annotation is not Parameter.empty
    }


def _finalize(
    writer: Writer,
    func: Callable[..., Any],
    kept: list[Parameter],
    resolved_deps: dict[str, DependsWrapper],
) -> Callable[..., Any]:
    """Compile the generated wrapper source and restore its metadata, defaults, and annotations."""
    namespace: dict[str, Any] = {
        "__func__": func,
        **{f"__dep_{name}__": dep for name, dep in resolved_deps.items()},
    }
    exec(compile(writer.render(), "<generated>", "exec"), namespace)  # noqa: S102

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


def create_sync_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """Create a synchronous wrapper function that injects resolved dependencies."""
    kept = _kept_parameters(func, resolved_deps)

    writer = Writer()
    writer.write(f"def wrapper({render_parameters(kept)}):")
    with writer.block():
        writer.write(f"return __func__({_render_call_arguments(func, resolved_deps)})")

    return _finalize(writer, func, kept, resolved_deps)


def create_sync_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """Create a synchronous generator wrapper function that injects resolved dependencies."""
    kept = _kept_parameters(func, resolved_deps)

    writer = Writer()
    writer.write(f"def wrapper({render_parameters(kept)}):")
    with writer.block():
        writer.write(f"yield from __func__({_render_call_arguments(func, resolved_deps)})")
    return _finalize(writer, func, kept, resolved_deps)


def create_async_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """Create an asynchronous wrapper function that injects resolved dependencies."""
    kept = _kept_parameters(func, resolved_deps)

    writer = Writer()
    writer.write(f"async def wrapper({render_parameters(kept)}):")
    with writer.block():
        writer.write(f"return await __func__({_render_call_arguments(func, resolved_deps)})")

    return _finalize(writer, func, kept, resolved_deps)


def create_async_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> Callable[..., Any]:
    """Create an asynchronous generator wrapper function that injects resolved dependencies."""
    kept = _kept_parameters(func, resolved_deps)

    writer = Writer()
    writer.write(f"async def wrapper({render_parameters(kept)}):")
    with writer.block():
        writer.write(
            f"async for __item__ in __func__({_render_call_arguments(func, resolved_deps)}):"
        )
        with writer.block():
            writer.write("yield __item__")

    return _finalize(writer, func, kept, resolved_deps)
