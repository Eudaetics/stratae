"""Minimal line-based writer for generating indented source code."""

from collections.abc import Generator
from contextlib import contextmanager

from stratae.codegen.exceptions import InvalidIndentWidthError


class Writer:
    """Accumulates lines of source text, indented by `block()` nesting."""

    __slots__ = ("_lines", "_nesting", "_whitespace")

    def __init__(self, width: int = 4, use_tabs: bool = False) -> None:
        """
        Create a writer.

        :param width: Spaces per indent level. Ignored if `use_tabs` is True.
        :param use_tabs: Indent with tabs instead of spaces.
        :raises InvalidIndentWidthError: If `width` is less than 1 without `use_tabs`.
        """
        if not use_tabs and width < 1:
            raise InvalidIndentWidthError()
        self._whitespace = " " * width if not use_tabs else "\t"
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
        self._lines.append(f"{self._whitespace * self._nesting}{line}" if line else "")

    def __str__(self):
        """Return the rendered source."""
        return self.render()

    def render(self):
        """Join all written lines into the final source text."""
        return "\n".join(self._lines)
