"""
Read function signatures, and render them into source text.

A generated wrapper mirrors the function it wraps: {py:func}`render_parameters` writes its
parameter list, {py:func}`render_arguments` the call forwarding them, and {py:func}`defaults`
and {py:func}`kwdefaults` give it the same defaults.
"""

import sys
from collections.abc import Callable
from inspect import Parameter, Signature, signature
from typing import Any

# On 3.14+, signature() raises NameError on forward references unless given FORWARDREF.
_SIGNATURE_OPTIONS: dict[str, Any] = (
    {"annotation_format": __import__("annotationlib").Format.FORWARDREF}
    if sys.version_info >= (3, 14)
    else {}
)


def read_signature(func: Callable[..., Any]) -> Signature:
    """Return `func`'s signature, leaving annotations that can't be resolved unresolved."""
    return signature(func, **_SIGNATURE_OPTIONS)


_PREFIX = {
    Parameter.VAR_POSITIONAL: "*",
    Parameter.VAR_KEYWORD: "**",
}


def render_parameters(parameters: list[Parameter]) -> str:
    """
    Render a parameter list as source text, without defaults.

    The parameters are in signature order, as `inspect.Signature` keeps them. A `/` follows
    the last positional-only parameter, and a bare `*` comes before the first keyword-only one
    unless `*args` already does.
    """
    parts: list[str] = []
    previous: Any = None
    for param in parameters:
        kind = param.kind
        if previous is Parameter.POSITIONAL_ONLY and kind is not Parameter.POSITIONAL_ONLY:
            parts.append("/")
        if kind is Parameter.KEYWORD_ONLY and previous not in _BEFORE_KEYWORD_ONLY:
            parts.append("*")
        parts.append(_PREFIX.get(kind, "") + param.name)
        previous = kind
    if previous is Parameter.POSITIONAL_ONLY:
        parts.append("/")
    return ", ".join(parts)


_BEFORE_KEYWORD_ONLY = (Parameter.VAR_POSITIONAL, Parameter.KEYWORD_ONLY)
"""The kinds a keyword-only parameter can follow without a bare `*` before it."""


def _name(param: Parameter) -> str:
    """Return the parameter's own name, as the source of the argument forwarding it."""
    return param.name


def render_arguments(parameters: list[Parameter], value: Callable[[Parameter], str] = _name) -> str:
    """
    Render a call's arguments for `parameters`, each in its parameter's calling convention.

    :param parameters: The parameters of the function being called.
    :param value: Renders the source of each argument. Defaults to the parameter's name, which
        forwards the wrapper's own parameter.
    :returns: Source such as `a, *args, key=key, **kwargs`.
    """
    args: list[str] = []
    for param in parameters:
        source = value(param)
        if param.kind is Parameter.VAR_POSITIONAL:
            args.append(f"*{source}")
        elif param.kind is Parameter.VAR_KEYWORD:
            args.append(f"**{source}")
        elif param.kind is Parameter.KEYWORD_ONLY:
            args.append(f"{param.name}={source}")
        else:
            args.append(source)
    return ", ".join(args)


def defaults(parameters: list[Parameter]) -> tuple[Any, ...] | None:
    """
    Return the `__defaults__` of a function with `parameters`: its positional defaults.

    Read from the parameters rather than copied from a function, since a function's signature
    can come from another one it wraps, whose defaults it doesn't have.
    """
    return (
        tuple(
            param.default
            for param in parameters
            if param.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
            and param.default is not Parameter.empty
        )
        or None
    )


def kwdefaults(parameters: list[Parameter]) -> dict[str, Any] | None:
    """Return the `__kwdefaults__` of a function with `parameters`: its keyword-only defaults."""
    return {
        param.name: param.default
        for param in parameters
        if param.kind is Parameter.KEYWORD_ONLY and param.default is not Parameter.empty
    } or None
