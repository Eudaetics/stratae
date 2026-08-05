"""Root pytest configuration for testing docs in docstrings and markdown."""

from sybil import Sybil
from sybil.parsers.myst import PythonCodeBlockParser, SkipParser

python_examples = Sybil(
    parsers=[PythonCodeBlockParser(), SkipParser()],
    patterns=["*.py"],
    path="stratae",
)

doc_examples = Sybil(
    parsers=[PythonCodeBlockParser(), SkipParser()],
    patterns=["*.md"],
    excludes=["apidocs/*", "apidocs/*/*"],
    path="docs",
)

pytest_collect_file = (python_examples + doc_examples).pytest()
