"""
MyST renderer mixin that titles toctree entries with short module names.

Toctree entries are titled with the short module name, so the sidebar shows
``overriding`` rather than ``stratae.depends.overriding``. Page headings keep
the full dotted name.
"""

from typing import Iterable

from autodoc2.render.myst_ import MystRenderer
from autodoc2.utils import ItemData


class ShortTitleToctreeRenderer(MystRenderer):
    """MyST renderer mixin that titles toctree entries with short module names."""

    def render_package(self, item: ItemData) -> Iterable[str]:
        """Render a package page, titling toctree entries with short names."""
        return self._retitle_toctrees(super().render_package(item))

    def render_module(self, item: ItemData) -> Iterable[str]:
        """Render a module page, titling toctree entries with short names."""
        return self._retitle_toctrees(super().render_module(item))

    def _retitle_toctrees(self, lines: Iterable[str]) -> Iterable[str]:
        """Rewrite bare dotted names in toctree blocks as short-titled entries."""
        in_toctree = False
        for line in lines:
            if line.startswith("```{toctree}"):
                in_toctree = True
            elif in_toctree and line.startswith("```"):
                in_toctree = False
            elif in_toctree and "." in line and not line.startswith(":"):
                short_name = line.rsplit(".", 1)[1]
                line = f"{short_name} <{line}>"
            yield line
