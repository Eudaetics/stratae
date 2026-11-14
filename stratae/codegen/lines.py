"""
Source code as lists of lines.

A fragment of generated code is a list of lines, each indented relative to the fragment. So
fragments compose like any other list. Nesting one inside a statement indents it one level.
"""

from collections.abc import Iterable

type Lines = list[str]
"""Lines of source, without line endings."""


def block(header: str, body: Iterable[str]) -> Lines:
    """Return `header` followed by `body`, indented one level under it, as PEP 8 indents."""
    return [header, *(f"    {line}" for line in body)]
