"""Render function signatures into source text."""

from enum import Enum, auto
from inspect import Parameter

from stratae.codegen.exceptions import InvalidTransitionError


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
        raise InvalidTransitionError(
            f"Cannot transition from {section.name} to {next_section.name} "
            f"at parameter {param.name!r}."
        ) from None
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
