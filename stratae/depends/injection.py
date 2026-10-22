"""
Core dependency injection module.

{py:func}`Depends` marks a callable as the provider for an injected
parameter. Wrapping a callable in {py:func}`Depends` registers it as a
{py:class}`Provider <stratae.depends._provide.Provider>`, the singleton
that tracks that provider's resolution and override state. Passing the
same callable twice returns the same
{py:class}`Provider <stratae.depends._provide.Provider>`. Every
injection site sharing a provider shares one registration.

The {py:func}`inject` decorator inspects a function's signature at
decoration time, finds parameters annotated with {py:func}`Depends`, and
wraps the function. The wrapped function's parameters are resolved by calling
their providers at call time rather than being passed in by the caller.
Providers may themselves declare injected parameters, which are resolved
recursively when the using function is decorated. Sync functions may only
depend on sync providers, which is validated at decoration time, raising
{py:exc}`InjectionSignatureError <stratae.depends.exceptions.InjectionSignatureError>`.
Async functions may mix sync and async providers. Modules using
`from __future__ import annotations` aren't supported: injection reads the
annotation objects themselves, so a function or provider from such a module
with a required parameter raises
{py:exc}`InjectionSignatureError <stratae.depends.exceptions.InjectionSignatureError>`,
as does any injected parameter whose annotation is a string, such as a
dataclass field from such a module.

```{note}
This module only handles wiring: calling a provider and passing its
result to whatever declared it as a dependency. A provider runs again on
every call unless it caches its own result. For singleton or
scoped values (computed once per request, once per process, etc.),
layer this with {py:mod}`stratae.lifecycle`. Decorate the provider with
{py:meth}`Scope.cache <stratae.lifecycle.scope.Scope.cache>`
(or its {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`
counterpart) to compute it once per scope activation, then pass that
cached provider to {py:func}`Depends` as usual.
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

See {py:func}`Depends` and {py:func}`inject` for the rest of the module's API.

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

    Used inside an annotation, e.g. `Annotated[int, Depends(get_value)]`,
    or through a type alias such as
    `type Value = Annotated[int, Depends(get_value)]`. Passing the same callable
    twice returns the same {py:class}`Provider <stratae.depends._provide.Provider>`,
    so every injection site of a provider shares one registration.

    :param dependency: Callable invoked to produce the parameter's value. It
        may declare injected parameters of its own, resolved recursively
        when the using function is decorated.
    :returns: The {py:class}`Provider <stratae.depends._provide.Provider>`
        registered for the callable.
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
    Resolve a function's injected parameters and strip them from calls.

    Each parameter annotated `Annotated[T, Depends(provider)]` is filled by
    its provider when the function is called; callers do not pass it. The
    dependency graph, including providers' own injected parameters, is
    resolved once at decoration time.

    By default the decorated function is typed as `Callable[..., R]`; its
    parameters are not preserved. Injected parameters are stripped at
    resolution time and there's no way to describe that generically. The
    `sig` option is provided as a fallback to correct the signature for
    use in type checking and IDE support. To give the decorated function
    a precise signature, pass a stub function via `sig`. The decorated
    function is typed as `sig`, not as the original function. It is
    recommended to create type stubs instead, although this is provided
    as a fallback.

    :param func: The function to wrap, when used as a bare `@inject`.
    :param sig: Stub function whose signature is presented to type checkers
        and IDEs in place of the decorated function's. Has no runtime
        effect.
    :returns: The wrapped function, or a decorator when called with
        arguments. A function with no injected parameters is returned
        unchanged.
    :raises InjectionSignatureError: An injected parameter has a default
        value, a sync function depends on an async provider, a function
        or provider from a module using `from __future__ import annotations`
        has a required parameter, an injected parameter's annotation is a
        string, or injected parameters are found on a
        callable object rather than a function, method, or class.
    :raises CircularDependencyError: Providers depend on each other in a
        cycle.
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
    Raise if `func` has injected parameters but is a callable object, not a function or class.

    Injecting into an object - one with `__call__`, or a `functools.partial` - would replace
    it with a function, losing what the object is, and leave unclear whether it's the
    object's `__call__` or `__init__` being injected.
    """
    if resolved_deps and not (isfunction(func) or ismethod(func) or isclass(func)):
        raise InjectionSignatureError(
            f"Cannot inject into {func!r}: inject supports functions, methods, and classes. "
            "Decorate its __call__ method with @inject instead, or wrap it in a function."
        )


def _annotated_function(func: Callable[..., Any]) -> Any:
    """Return the function whose code and globals `func`'s parameter annotations come from."""
    return unwrap(func.__init__ if isclass(func) else func)


def _uses_future_annotations(func: Callable[..., Any]) -> bool:
    """Whether `func`'s module uses `from __future__ import annotations`."""
    code = getattr(_annotated_function(func), "__code__", None)
    return code is not None and bool(code.co_flags & __future__.annotations.compiler_flag)


def _reject_future_annotations(func: Callable[..., Any], sig: Signature) -> None:
    """
    Raise if `func` could have an injected parameter whose annotation is only a string.

    Every annotation in such a module is a string, and an injected parameter never has
    a default, so any required parameter might be one.
    """
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
    """Whether `annotation` is a string that evaluates to an injected parameter's annotation."""
    if not isinstance(annotation, str):
        return False
    try:
        value = eval(annotation, namespace)  # noqa: S307
    except Exception:
        # A forward reference or TYPE_CHECKING-only name - not resolvable, so not injected.
        return False
    value = _unwrap_type(value)
    return get_origin(value) is Annotated and _get_annotated_info(value) is not None


def _reject_quoted_dependencies(func: Callable[..., Any], sig: Signature) -> None:
    """
    Raise if one of `func`'s parameters is an injected parameter written as a string.

    Catches what `_reject_future_annotations` can't see from code flags: a hand-quoted
    annotation, or a class whose `__init__` was generated, like a dataclass's, from a
    module using `from __future__ import annotations`. Each string is evaluated in the
    globals `func` was defined in, the same way `typing.get_type_hints` would.
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
    """Check if a function has async dependencies."""
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
    """Determine the correct create wrapper function based on the function type."""
    if iscoroutinefunction(func):
        return create_async_wrapper
    elif isasyncgenfunction(func):
        return create_async_gen_wrapper
    elif isgeneratorfunction(func):
        return create_sync_gen_wrapper
    else:
        return create_sync_wrapper
