"""
Docstring parser that converts Google-style docstrings for autodoc2.

autodoc2 renders docstrings without autodoc's processing events, so napoleon
cannot hook in normally. This parser runs each docstring through napoleon's
Google-style converter before standard RST parsing.
"""

from typing import cast

from docutils import nodes
from docutils.parsers.rst import Parser as RstParser
from sphinx.config import Config
from sphinx.ext.napoleon import Config as NapoleonConfig
from sphinx.ext.napoleon import docstring


class NapoleonParser(RstParser):
    """RST parser that first converts Google-style docstring sections via napoleon."""

    def parse(self, inputstring: str, document: nodes.document) -> None:
        """Convert the docstring from Google style to RST, then parse it as RST."""
        config = NapoleonConfig(
            napoleon_use_param=True,
            napoleon_use_rtype=True,
            napoleon_custom_sections=[("Type Parameters", "params_style")],
        )
        converted = str(docstring.GoogleDocstring(str(inputstring), cast(Config, config)))
        super().parse(converted, document)


Parser = NapoleonParser
