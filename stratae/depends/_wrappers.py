"""
Codegen'd wrappers for injected functions, sync and async.

Generated wrappers expose the original signature minus injected parameters, so other
tooling can introspect them.
"""

from functools import wraps
from inspect import Parameter, Signature, isclass, markcoroutinefunction
from typing import Any, Callable

from stratae.codegen import Writer, read_signature, render_parameters, wrapper_filename
from stratae.depends._provide import Provider


def _kept_parameters(
    params: list[Parameter], resolved_deps: dict[str, Provider]
) -> list[Parameter]:
    """Return func's parameters minus the injected ones."""
    return [param for param in params if param.name not in resolved_deps]


def _render_dependency_source(name: str, dep: Provider) -> str:
    """Render the expression resolving one dependency, awaiting it if async."""
    return f"{'await ' if dep.is_async else ''}__dep_{name}__.provide()"


def _render_argument_source(value: Any, param: Parameter):
    """Render one call argument in its parameter's calling convention."""
    if param.kind is Parameter.VAR_POSITIONAL:
        return f"*{value}"
    elif param.kind is Parameter.VAR_KEYWORD:
        return f"**{value}"
    elif param.kind is Parameter.KEYWORD_ONLY:
        return f"{param.name}={value}"
    else:
        return value


def _render_call_arguments(params: list[Parameter], resolved_deps: dict[str, Provider]) -> str:
    """Build the exact call expression baked into the generated wrapper body."""
    args: list[str] = []
    for param in params:
        dep = resolved_deps.get(param.name)
        value = _render_dependency_source(param.name, dep) if dep is not None else param.name
        args.append(_render_argument_source(value, param))

    return ", ".join(args)


def _get_source(func: Callable[..., Any]):
    """Return the callable carrying func's metadata: `__init__` for a class."""
    return func.__init__ if isclass(func) else func


def _get_annotations(params: list[Parameter]):
    """Return an __annotations__ mapping for the kept parameters."""
    return {
        param.name: param.annotation for param in params if param.annotation is not Parameter.empty
    }


def _finalize(
    writer: Writer,
    func: Callable[..., Any],
    kept: Signature,
    resolved_deps: dict[str, Provider],
) -> Callable[..., Any]:
    """
    Compile the wrapper source and copy func's metadata onto it.

    The return annotation is kept, since frameworks like FastAPI read it.
    """
    namespace: dict[str, Any] = {
        "__func__": func,
        **{f"__dep_{name}__": dep for name, dep in resolved_deps.items()},
    }
    exec(compile(writer.render(), wrapper_filename(func), "exec"), namespace)  # noqa: S102

    metadata_source: Callable[..., Any] = _get_source(func)

    wrapper = namespace["wrapper"]
    wraps(func)(wrapper)
    wrapper.__signature__ = kept
    wrapper.__defaults__ = metadata_source.__defaults__
    wrapper.__kwdefaults__ = metadata_source.__kwdefaults__
    wrapper.__annotations__ = _get_annotations(list(kept.parameters.values()))
    if kept.return_annotation is not Signature.empty:
        wrapper.__annotations__["return"] = kept.return_annotation
    wrapper.__stratae_resolved__ = True
    return wrapper


def _build_wrapper(
    func: Callable[..., Any],
    resolved_deps: dict[str, Provider],
    is_async: bool,
    write_body: Callable[[Writer, str], None],
):
    """Share the setup common to every wrapper kind."""
    sig = read_signature(func)
    params = list(sig.parameters.values())
    kept = _kept_parameters(params, resolved_deps)

    writer = Writer()
    writer.write(f"{'async ' if is_async else ''}def wrapper({render_parameters(kept)}):")
    with writer.block():
        write_body(writer, _render_call_arguments(params, resolved_deps))

    return _finalize(writer, func, sig.replace(parameters=kept), resolved_deps)


def create_sync_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create a synchronous wrapper."""
    return _build_wrapper(
        func, resolved_deps, False, lambda w, call: w.write(f"return __func__({call})")
    )


def create_sync_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create a synchronous generator wrapper, delegating with `yield from`."""
    return _build_wrapper(
        func, resolved_deps, False, lambda w, call: w.write(f"yield from __func__({call})")
    )


def _has_async_deps(resolved_deps: dict[str, Provider]) -> bool:
    return any(dep.is_async for dep in resolved_deps.values())


def create_async_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create an asynchronous wrapper."""
    if _has_async_deps(resolved_deps):
        return _build_wrapper(
            func, resolved_deps, True, lambda w, call: w.write(f"return await __func__({call})")
        )
    return markcoroutinefunction(create_sync_wrapper(func, resolved_deps))


def _write_stop_on_exhaustion(writer: Writer, statement: str) -> None:
    """Write `statement`, returning from the wrapper once the inner generator is exhausted."""
    writer.write("try:")
    with writer.block():
        writer.write(statement)
    writer.write("except StopAsyncIteration:")
    with writer.block():
        writer.write("return")


def _write_async_gen_body(writer: Writer, call: str) -> None:
    """
    Write an async generator wrapper body that delegates like `yield from`.

    Forwards sent values, thrown exceptions, and close, which `async for` would drop.
    """
    writer.write(f"__gen__ = __func__({call})")
    _write_stop_on_exhaustion(writer, "__value__ = await __gen__.__anext__()")
    writer.write("while True:")
    with writer.block():
        writer.write("try:")
        with writer.block():
            writer.write("__sent__ = yield __value__")
        writer.write("except GeneratorExit:")
        with writer.block():
            writer.write("await __gen__.aclose()")
            writer.write("raise")
        writer.write("except BaseException as __exc__:")
        with writer.block():
            _write_stop_on_exhaustion(writer, "__value__ = await __gen__.athrow(__exc__)")
        writer.write("else:")
        with writer.block():
            _write_stop_on_exhaustion(writer, "__value__ = await __gen__.asend(__sent__)")


def create_async_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create an async generator wrapper, itself an async generator function."""
    return _build_wrapper(func, resolved_deps, True, write_body=_write_async_gen_body)
