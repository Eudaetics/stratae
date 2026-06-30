"""
Minimal line-based writer for generating indented source code.

`Writer` accumulates lines of text with a consistent indentation level per
line, intended for programmatically generating source code (e.g. Python
files produced by a code generator).
"""

from contextlib import contextmanager
from typing import Generator


class Writer:
    """
    Accumulates lines of text with consistent indentation.

    Indentation is controlled via the `indent` context manager, increasing
    the nesting level for the duration of a `with` block. Lines are appended
    with `write`, and the final source text is produced by `render`.
    """

    __slots__ = ("_nesting", "_lines", "_whitespace")

    def __init__(self, width: int = 4, use_tabs: bool = False) -> None:
        """
        Initialize the writer.

        Args:
            width: Number of spaces per indent level. Ignored if `use_tabs`
                is True.
            use_tabs: If True, indent with a single tab character per level
                instead of spaces.

        Raises:
            ValueError: If `use_tabs` is False and `width` is less than 1.

        """
        if not use_tabs and width < 1:
            raise ValueError(
                "Indent width must be a positive integer."
                "The recommended indent width per PEP 8 is 4 spaces."
            )
        self._whitespace = " " * width if not use_tabs else "\t"
        self._nesting = 0
        self._lines: list[str] = []

    @contextmanager
    def indent(self) -> Generator[None]:
        """
        Increase the indentation level for the duration of a block.

        Lines written via `write` while inside this block are indented one
        additional level. The nesting level is always restored on exit,
        including when the block raises an exception.
        """
        self._nesting += 1
        try:
            yield
        finally:
            self._nesting -= 1

    def write(self, line: str):
        """
        Append a line of text at the current indentation level.

        The line is stripped of leading and trailing whitespace before the
        indent prefix is applied. Lines that are empty after stripping are
        written without any indentation.
        """
        line = line.strip()
        self._lines.append(f"{self._whitespace * self._nesting}{line}" if line else "")

    def __str__(self):
        """Return the rendered source. Equivalent to `render()`."""
        return self.render()

    def render(self):
        """Join all written lines into the final source text."""
        return "\n".join(self._lines)
