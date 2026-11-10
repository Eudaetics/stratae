"""
Read function signatures, and render them into source text.

A generated wrapper mirrors the function it wraps: {py:func}`render_parameters` writes its
parameter list, {py:func}`render_arguments` the call forwarding them, and {py:func}`defaults`
and {py:func}`kwdefaults` give it the same defaults.
"""

import sys
from collections.abc import Callable
from enum import Enum, auto
from inspect import Parameter, Signature, signature
from typing import Any

from stratae.codegen.exceptions import InvalidTransitionError

# On 3.14+, signature() raises NameError on forward references unless given FORWARDREF.
_SIGNATURE_OPTIONS: dict[str, Any] = (
    {"annotation_format": __import__("annotationlib").Format.FORWARDREF}
    if sys.version_info >= (3, 14)
    else {}
)


def read_signature(func: Callable[..., Any]) -> Signature:
    """Return `func`'s signature, leaving annotations that can't be resolved unresolved."""
    return signature(func, **_SIGNATURE_OPTIONS)


class _Section(Enum):
    """Section of the parameter list currently being rendered."""

    START = auto()
    POSITIONAL_ONLY = auto()
    STANDARD = auto()
    VAR_POSITIONAL = auto()
    KEYWORD_ONLY = auto()
    VAR_KEYWORD = auto()


_KIND_TO_SECTION_MAP = {
    Parameter.POSITIONAL_ONLY: _Section.POSITIONAL_ONLY,
    Parameter.POSITIONAL_OR_KEYWORD: _Section.STANDARD,
    Parameter.VAR_POSITIONAL: _Section.VAR_POSITIONAL,
    Parameter.KEYWORD_ONLY: _Section.KEYWORD_ONLY,
    Parameter.VAR_KEYWORD: _Section.VAR_KEYWORD,
}

_PREFIX = {
    _Section.VAR_POSITIONAL: "*",
    _Section.VAR_KEYWORD: "**",
}

_TRANSITION: dict[tuple[_Section, _Section], tuple[str, ...]] = {
    (_Section.START, _Section.POSITIONAL_ONLY): (),
    (_Section.START, _Section.STANDARD): (),
    (_Section.START, _Section.VAR_POSITIONAL): (),
    (_Section.START, _Section.KEYWORD_ONLY): ("*",),
    (_Section.START, _Section.VAR_KEYWORD): (),
    (_Section.POSITIONAL_ONLY, _Section.STANDARD): ("/",),
    (_Section.POSITIONAL_ONLY, _Section.VAR_POSITIONAL): ("/",),
    (_Section.POSITIONAL_ONLY, _Section.KEYWORD_ONLY): ("/", "*"),
    (_Section.POSITIONAL_ONLY, _Section.VAR_KEYWORD): ("/",),
    (_Section.STANDARD, _Section.VAR_POSITIONAL): (),
    (_Section.STANDARD, _Section.KEYWORD_ONLY): ("*",),
    (_Section.STANDARD, _Section.VAR_KEYWORD): (),
    (_Section.VAR_POSITIONAL, _Section.KEYWORD_ONLY): (),
    (_Section.VAR_POSITIONAL, _Section.VAR_KEYWORD): (),
    (_Section.KEYWORD_ONLY, _Section.VAR_KEYWORD): (),
}


def _advance(section: _Section, param: Parameter) -> tuple[_Section, tuple[str, ...]]:
    """Look up the next section and the markers to emit for the transition."""
    next_section = _KIND_TO_SECTION_MAP[param.kind]
    if next_section is section:
        return section, ()
    try:
        markers = _TRANSITION[(section, next_section)]
    except KeyError:
        raise InvalidTransitionError(section.name, next_section.name, param.name) from None
    return next_section, markers


def render_parameters(parameters: list[Parameter]) -> str:
    """Render a parameter list as source text, without defaults."""
    parts: list[str] = []
    section: _Section = _Section.START
    for param in parameters:
        section, markers = _advance(section, param)
        parts.extend(markers)
        parts.append(_PREFIX.get(section, "") + param.name)
    if section is _Section.POSITIONAL_ONLY:
        parts.append("/")
    return ", ".join(parts)


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
