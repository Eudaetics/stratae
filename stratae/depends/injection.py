"""
Dependency injection.

{py:func}`Depends` marks a callable as the provider for a parameter, and {py:func}`inject`
fills those parameters by calling their providers. Providers can declare injected
parameters of their own. The dependency graph is resolved once, at decoration time. Sync
functions may only depend on sync providers; async functions may use both. Modules using
`from __future__ import annotations` aren't supported.

```{note}
A provider runs on every call. To compute it once per scope, decorate it with
{py:meth}`Scope.cache <stratae.lifecycle.scope.Scope.cache>` before passing it to
{py:func}`Depends`.
```

````{example} Injecting a Stripe client backed by an API key provider
```{code-block} python
from typing import Annotated, Any
from stratae.depends import Depends, inject

class PaymentIntents:
    def __init__(self, api_key: str):
        self._api_key = api_key

    def create(self, params: dict[str, Any]) -> str:
        return f"pi_{params['amount']}_{self._api_key[-6:]}"

class StripeClient:
    def __init__(self, api_key: str):
        self.payment_intents = PaymentIntents(api_key)

def get_api_key() -> str:
    return "fake-api-key-123456"

def get_stripe_client(
    api_key: Annotated[str, Depends(get_api_key)]
) -> StripeClient:
    return StripeClient(api_key)

type StripeClientDep = Annotated[StripeClient, Depends(get_stripe_client)]

@inject
def create_payment(amount_cents: int, stripe: StripeClientDep) -> str:
    return stripe.payment_intents.create({
        "amount": amount_cents,
        "currency": "usd",
        "automatic_payment_methods": {"enabled": True},
    })

print(create_payment(2000))
```
```{output}
pi_2000_123456
```
````
"""

import __future__

from inspect import (
    Parameter,
    Signature,
    isasyncgenfunction,
    isclass,
    iscoroutinefunction,
    isfunction,
    isgeneratorfunction,
    ismethod,
    unwrap,
)
from typing import Annotated, Any, Awaitable, Callable, get_origin, overload

from stratae.codegen import read_signature
from stratae.depends._provide import Provider
from stratae.depends._wrappers import (
    create_async_gen_wrapper,
    create_async_wrapper,
    create_sync_gen_wrapper,
    create_sync_wrapper,
)
from stratae.depends.exceptions import (
    CircularDependencyError,
    InjectionSignatureError,
)


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]]) -> Provider: ...  # noqa: S1542


@overload
def Depends[**P, R](dependency: Callable[P, R]) -> Provider: ...  # noqa: S1542


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]]) -> Provider:  # noqa: S1542
    """
    Mark a callable as the provider for an injected parameter.

    Use it in an annotation, e.g. `Annotated[int, Depends(get_value)]`. The same callable
    always returns the same {py:class}`Provider <stratae.depends._provide.Provider>`.

    :param dependency: Callable producing the parameter's value. It may have injected
        parameters of its own.
    :returns: The callable's {py:class}`Provider <stratae.depends._provide.Provider>`.
    """
    return Provider(dependency=dependency)


@overload
def inject[F: Callable[..., Any]](*, sig: F) -> Callable[[Callable[..., Any]], F]: ...


@overload
def inject[R](func: Callable[..., R]) -> Callable[..., R]: ...


@overload
def inject[R]() -> Callable[[Callable[..., R]], Callable[..., R]]: ...


def inject(
    func: Callable[..., Any] | None = None,
    *,
    sig: Callable[..., Any] | None = None,
) -> Any:
    """
    Fill a function's injected parameters from their providers at call time.

    Callers don't pass injected parameters. The decorated function is typed as
    `Callable[..., R]`; pass `sig` to give type checkers a precise signature.

    :param func: The function to wrap, when used as a bare `@inject`.
    :param sig: Stub function whose signature type checkers see instead. No runtime effect.
    :returns: The wrapped function, or a decorator when called with arguments. A function
        with no injected parameters is returned unchanged.
    :raises InjectionSignatureError: If an injected parameter has a default or a string
        annotation, a sync function depends on an async provider, a module uses
        `from __future__ import annotations`, or the target is a callable object.
    :raises CircularDependencyError: If providers depend on each other in a cycle.
    """

    def decorator(f: Callable[..., Any]) -> Any:
        return _resolve_function(f)

    if sig is not None or func is None:
        return decorator
    return decorator(func)


def _resolve_function[**P, R](
    func: Callable[P, R],
    _resolving: set[Callable[..., Any]] | None = None,
) -> Callable[..., R]:
    """Resolve a function to its dependencies."""
    if getattr(func, "__stratae_resolved__", False):
        return func
    if _resolving is None:
        _resolving = set()
    if func in _resolving:
        raise CircularDependencyError(f"Circular dependency detected for {func}.")

    _resolving.add(func)
    sig = read_signature(func)
    _reject_future_annotations(func, sig)
    _reject_quoted_dependencies(func, sig)
    resolved_deps: dict[str, Provider] = _resolve_parameters(sig, _resolving)

    _reject_callable_objects(func, resolved_deps)
    _validate_sync_async_constraint(func, resolved_deps)
    return _create_wrapper(func, resolved_deps)


def _reject_callable_objects(func: Callable[..., Any], resolved_deps: dict[str, Provider]) -> None:
    """
    Raise if `func` has injected parameters but is a callable object.

    Injecting would replace the object with a function, and it's ambiguous whether
    `__call__` or `__init__` is meant.
    """
    if resolved_deps and not (isfunction(func) or ismethod(func) or isclass(func)):
        raise InjectionSignatureError(
            f"Cannot inject into {func!r}: inject supports functions, methods, and classes. "
            "Decorate its __call__ method with @inject instead, or wrap it in a function."
        )


def _annotated_function(func: Callable[..., Any]) -> Any:
    """Return the function whose globals `func`'s annotations come from."""
    return unwrap(func.__init__ if isclass(func) else func)


def _uses_future_annotations(func: Callable[..., Any]) -> bool:
    """Whether `func`'s module uses `from __future__ import annotations`."""
    code = getattr(_annotated_function(func), "__code__", None)
    return code is not None and bool(code.co_flags & __future__.annotations.compiler_flag)


def _reject_future_annotations(func: Callable[..., Any], sig: Signature) -> None:
    """Raise if `func` has a required parameter in a module with stringified annotations."""
    variadic = (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD)
    required = any(
        param.default is Parameter.empty and param.kind not in variadic
        for param in sig.parameters.values()
    )
    if required and _uses_future_annotations(func):
        raise InjectionSignatureError(
            f"{func.__qualname__} is from a module using 'from __future__ import annotations', "
            "which inject doesn't support: its annotations are strings, so injected "
            "parameters can't be found."
        )


def _is_quoted_dependency(annotation: Any, namespace: dict[str, Any]) -> bool:
    """Whether `annotation` is a string evaluating to an injected annotation."""
    if not isinstance(annotation, str):
        return False
    try:
        value = eval(annotation, namespace)  # noqa: S307
    except Exception:
        # An unresolvable forward reference, so not injected.
        return False
    value = _unwrap_type(value)
    return get_origin(value) is Annotated and _get_annotated_info(value) is not None


def _reject_quoted_dependencies(func: Callable[..., Any], sig: Signature) -> None:
    """
    Raise if one of `func`'s injected parameters is annotated with a string.

    Catches hand-quoted annotations and generated `__init__`s, such as a dataclass's.
    """
    namespace: dict[str, Any] = getattr(_annotated_function(func), "__globals__", {})
    for param in sig.parameters.values():
        if _is_quoted_dependency(param.annotation, namespace):
            raise InjectionSignatureError(
                f"{func.__qualname__} parameter {param.name!r} has a string annotation, which "
                "inject doesn't support: it reads annotation objects, so the dependency "
                "can't be found. Unquote it, or remove 'from __future__ import annotations'."
            )


def _resolve_parameters(sig: Signature, _resolving: set[Callable[..., Any]]) -> dict[str, Provider]:
    """Resolve a list of parameters."""
    return {
        name: value
        for name, param in sig.parameters.items()
        if (value := _resolve_parameter(param, _resolving)) is not None
    }


def _get_annotated_info(annotation: Annotated[Any, ...]) -> Provider | None:
    """Extract the Provider from an Annotated parameter."""
    depends_wrapper = next(
        (x for x in reversed(annotation.__metadata__) if isinstance(x, Provider)),
        None,
    )
    return depends_wrapper


def _unwrap_type(annotation: Any) -> Any:
    """Unwrap Annotated types to get the actual type."""
    return getattr(annotation, "__value__", annotation)


def _resolve_parameter(param: Parameter, _resolving: set[Callable[..., Any]]) -> Provider | None:
    """Resolve a single parameter to its dependency, if it has one."""
    annotation = _unwrap_type(param.annotation)
    if get_origin(annotation) is not Annotated:
        return None

    depends = _get_annotated_info(annotation)
    if depends is None:
        return None
    elif param.default is not Parameter.empty:
        raise InjectionSignatureError(f"Cannot use a default with injected parameter {param.name}")

    if not depends.resolved:
        depends.update(_resolve_function(depends.dependency, _resolving))
        depends.resolved = True
    return depends


def _validate_sync_async_constraint(
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
) -> None:
    """Raise if a sync function has async dependencies."""
    if iscoroutinefunction(func) or isasyncgenfunction(func):
        return

    if any(v.is_async for v in resolved_deps.values()):
        raise InjectionSignatureError(
            f"Sync function '{func.__name__}' cannot have async dependencies."
        )


def _create_wrapper(
    func: Callable[..., Any],
    resolved_deps: dict[str, Provider],
) -> Callable[..., Any]:
    """Create a wrapper function that injects resolved dependencies."""
    if not resolved_deps:
        return func

    return _wrapper_factory(func)(func, resolved_deps)


def _wrapper_factory(func: Callable[..., Any]) -> Callable[..., Callable[..., Any]]:
    """Pick the wrapper factory for the function's type."""
    if iscoroutinefunction(func):
        return create_async_wrapper
    elif isasyncgenfunction(func):
        return create_async_gen_wrapper
    elif isgeneratorfunction(func):
        return create_sync_gen_wrapper
    else:
        return create_sync_wrapper
