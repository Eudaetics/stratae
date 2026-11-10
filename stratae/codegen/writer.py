"""Minimal line-based writer for generating indented source code."""

from collections.abc import Generator
from contextlib import contextmanager

_INDENT = "    "
"""One level of indent, as PEP 8 recommends."""


class Writer:
    """Accumulates lines of source text, indented four spaces per `block()` nesting."""

    __slots__ = ("_lines", "_nesting")

    def __init__(self) -> None:
        """Create an empty writer."""
        self._nesting = 0
        self._lines: list[str] = []

    @contextmanager
    def block(self) -> Generator[None, None, None]:
        """Indent lines written inside the `with` block one more level."""
        self._nesting += 1
        try:
            yield
        finally:
            self._nesting -= 1

    def write(self, line: str):
        """Append a stripped line at the current indent; blank lines get no indent."""
        line = line.strip()
        self._lines.append(f"{_INDENT * self._nesting}{line}" if line else "")

    def __str__(self):
        """Return the rendered source."""
        return self.render()

    def render(self):
        """Join all written lines into the final source text."""
        return "\n".join(self._lines)
