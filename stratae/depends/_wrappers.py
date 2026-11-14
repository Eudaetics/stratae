"""
Codegen'd wrappers for injected functions, sync and async.

Generated wrappers expose the original signature minus injected parameters, so other
tooling can introspect them.
"""

from collections.abc import Callable
from functools import wraps
from inspect import Parameter, Signature, markcoroutinefunction
from typing import Any

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
from stratae.depends._provide import Provider


def _kept_parameters(
    params: list[Parameter], resolved_deps: dict[str, Provider]
) -> list[Parameter]:
    """Return func's parameters minus the injected ones."""
    return [param for param in params if param.name not in resolved_deps]


def _render_dependency_source(name: str, dep: Provider) -> str:
    """Render the expression resolving one dependency, awaiting it if async."""
    return f"{'await ' if dep.is_async else ''}__dep_{name}__.provide()"


def _render_call_arguments(params: list[Parameter], resolved_deps: dict[str, Provider]) -> str:
    """Build the call's arguments: each injected parameter's provider, or the parameter."""

    def value(param: Parameter) -> str:
        dep = resolved_deps.get(param.name)
        return param.name if dep is None else _render_dependency_source(param.name, dep)

    return render_arguments(params, value)


def _get_annotations(params: list[Parameter]) -> dict[str, Any]:
    """Return an __annotations__ mapping for the kept parameters."""
    return {
        param.name: param.annotation for param in params if param.annotation is not Parameter.empty
    }


type _Source = Callable[[str, str], Lines]
"""The wrapper's source, from its parameters and the arguments its call passes on."""


def _compile(
    func: Callable[..., Any], resolved_deps: dict[str, Provider], source: _Source
) -> Callable[..., Any]:
    """
    Compile `source` for `func`, and copy func's metadata onto it.

    The return annotation is kept, since frameworks like FastAPI read it.
    """
    sig = read_signature(func)
    params = list(sig.parameters.values())
    kept_params = _kept_parameters(params, resolved_deps)
    kept = sig.replace(parameters=kept_params)
    lines = source(render_parameters(kept_params), _render_call_arguments(params, resolved_deps))
    namespace: dict[str, Any] = {
        "__func__": func,
        **{f"__dep_{name}__": dep for name, dep in resolved_deps.items()},
    }
    wrapper = compile_wrapper(lines, func, namespace)

    wraps(func)(wrapper)
    wrapper.__signature__ = kept
    wrapper.__defaults__ = defaults(kept_params)
    wrapper.__kwdefaults__ = kwdefaults(kept_params)
    wrapper.__annotations__ = _get_annotations(kept_params)
    if kept.return_annotation is not Signature.empty:
        wrapper.__annotations__["return"] = kept.return_annotation
    wrapper.__stratae_resolved__ = True
    return wrapper


def create_sync_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create a synchronous wrapper."""
    return _compile(
        func, resolved_deps, lambda params, args: def_wrapper(params, [f"return __func__({args})"])
    )


def create_sync_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create a synchronous generator wrapper, delegating with `yield from`."""
    return _compile(
        func,
        resolved_deps,
        lambda params, args: def_wrapper(params, [f"yield from __func__({args})"]),
    )


def _has_async_deps(resolved_deps: dict[str, Provider]) -> bool:
    return any(dep.is_async for dep in resolved_deps.values())


def create_async_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create an asynchronous wrapper."""
    if not _has_async_deps(resolved_deps):
        return markcoroutinefunction(create_sync_wrapper(func, resolved_deps))
    return _compile(
        func,
        resolved_deps,
        lambda params, args: async_def_wrapper(params, [f"return await __func__({args})"]),
    )


def _stop_on_exhaustion(statement: str) -> Lines:
    """Run `statement`, returning from the wrapper once the inner generator is exhausted."""
    return [*block("try:", [statement]), *block("except StopAsyncIteration:", ["return"])]


def _async_gen_body(arguments: str) -> Lines:
    """
    Return an async generator wrapper body that delegates like `yield from`.

    Forwards sent values, thrown exceptions, and close, which `async for` would drop.
    """
    return [
        f"__gen__ = __func__({arguments})",
        *_stop_on_exhaustion("__value__ = await __gen__.__anext__()"),
        *block(
            "while True:",
            [
                *block("try:", ["__sent__ = yield __value__"]),
                *block("except GeneratorExit:", ["await __gen__.aclose()", "raise"]),
                *block(
                    "except BaseException as __exc__:",
                    _stop_on_exhaustion("__value__ = await __gen__.athrow(__exc__)"),
                ),
                *block("else:", _stop_on_exhaustion("__value__ = await __gen__.asend(__sent__)")),
            ],
        ),
    ]


def create_async_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> Callable[..., Any]:
    """Create an async generator wrapper, itself an async generator function."""
    return _compile(
        func,
        resolved_deps,
        lambda params, args: async_def_wrapper(params, _async_gen_body(args)),
    )
