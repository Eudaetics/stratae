"""Sphinx configuration for Stratae documentation."""

import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

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

maximum_signature_line_length = 90
python_use_unqualified_type_names = True

autodoc2_packages = [
    {"path": "../stratae/checks.py", "module": "stratae.checks"},
    {"path": "../stratae/context.py", "module": "stratae.context"},
    {"path": "../stratae/depends", "module": "stratae.depends"},
    {"path": "../stratae/events", "module": "stratae.events"},
    {"path": "../stratae/integrations", "module": "stratae.integrations"},
    {"path": "../stratae/lifecycle", "module": "stratae.lifecycle"},
    {"path": "../stratae/serde", "module": "stratae.serde"},
]
autodoc2_render_plugin = "renderer.Renderer"
autodoc2_hidden_objects = ["private", "inherited"]
autodoc2_hidden_regexes = [
    r".*\.__slots__",
    r".*\.__all__",
]
autodoc2_docstring_parser_regexes = [
    (r".*", "napoleon"),
]
autodoc2_index_template = None

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
myst_heading_anchors = 3

html_static_path = ["_static"]
html_css_files = ["custom.css"]

html_theme = "shibuya"
html_theme_options = {
    "github_url": "https://github.com/Eudaetics/stratae",
}
