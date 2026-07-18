"""MyST renderer combining Stratae's autodoc2 customizations."""

from short_toc import ShortTitleToctreeRenderer
from sort_toc import SortedToctreeRenderer
from typevar import TypeVarRenderer


class Renderer(TypeVarRenderer, SortedToctreeRenderer, ShortTitleToctreeRenderer):
    """MyST renderer with unqualified typevars, sorted and short-titled toctrees."""
