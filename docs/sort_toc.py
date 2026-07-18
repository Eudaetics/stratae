"""
MyST renderer mixin that sorts submodule and subpackage toctrees by name.

Sorting applies only to the module/package listing itself, without affecting
the source ordering of objects within each page.
"""

from typing import Iterable

from autodoc2.render.myst_ import MystRenderer
from autodoc2.utils import ItemData


class SortedToctreeRenderer(MystRenderer):
    """MyST renderer mixin that sorts module/package toctrees by name."""

    def get_children(
        self, item: ItemData, types: None | set[str] = None, *, omit_hidden: bool = True
    ) -> Iterable[ItemData]:
        """Get children as the base renderer does, sorting module/package lists by name."""
        children = super().get_children(item, types, omit_hidden=omit_hidden)
        if types is not None and types <= {"module", "package"}:
            return sorted(children, key=lambda child: child["full_name"])
        return children
