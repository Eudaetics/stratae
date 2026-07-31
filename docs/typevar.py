"""
MyST renderer mixin that unqualifies scoped type variable names in annotations.

autodoc2 fully qualifies scoped type variables with their enclosing scope
(e.g. ``stratae.events.binding.bind.P``), which makes signatures unreadable. A
scoped type variable is detected by resolution: its dotted name is not in
autodoc2's analysis database, but its parent scope (the function or class
declaring it) is. Documented objects resolve as themselves and are left
untouched, as are external references, whose parents are unknown.
"""

import re

from autodoc2.render.myst_ import MystRenderer

_DOTTED_NAME = re.compile(r"[\w.]+")


class TypeVarRenderer(MystRenderer):
    """MyST renderer mixin that unqualifies scoped type variable names."""

    def format_annotation(self, annotation: None | str) -> str:
        """Format an annotation, unqualifying any scoped type variable names."""
        formatted = super().format_annotation(annotation)
        return _DOTTED_NAME.sub(self._unqualify, formatted)

    def _unqualify(self, match: re.Match[str]) -> str:
        """Reduce a dotted name to its last segment if it is a scoped type variable."""
        full_name = match.group()
        parent, dot, name = full_name.rpartition(".")
        if dot and self.get_item(full_name) is None and self.get_item(parent) is not None:
            return name
        return full_name
