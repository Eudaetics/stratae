"""Sphinx configuration for Stratae documentation."""

from importlib.metadata import PackageNotFoundError, version

project = "Stratae"
copyright = "2026, Scott Wahl"
author = "Scott Wahl"

try:
    release = version("stratae")
except PackageNotFoundError:
    release = "0.0.0"
version = release

extensions = [
    "myst_parser",
    "autodoc2",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

autodoc2_packages = [
    {"path": "../stratae/check", "module": "stratae.check"},
    {"path": "../stratae/codegen", "module": "stratae.codegen"},
    {"path": "../stratae/context", "module": "stratae.context"},
    {"path": "../stratae/depends", "module": "stratae.depends"},
    {"path": "../stratae/events", "module": "stratae.events"},
    {"path": "../stratae/integrations", "module": "stratae.integrations"},
    {"path": "../stratae/lifecycle", "module": "stratae.lifecycle"},
    {"path": "../stratae/serde", "module": "stratae.serde"},
]
autodoc2_render_plugin = "myst"
autodoc2_hidden_objects = ["private"]
autodoc2_hidden_regexes = [
    r"\.__slots__$",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
myst_heading_anchors = 3

html_theme = "shibuya"
html_theme_options = {
    "github_url": "https://github.com/Eudaetics/stratae",
}
