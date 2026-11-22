"""Fixtures for the signature generator's tests: modules written to disk, read but not run."""

import ast
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from itertools import count
from pathlib import Path

import pytest

from stratae.scripts.signatures import Stub, generate

_packages = count()

DEPS = """\
from typing import Annotated

from stratae.depends import Depends


class Stripe: ...


def get_stripe() -> Stripe:
    return Stripe()


type StripeDep = Annotated[Stripe, Depends(get_stripe)]
"""
"""The `deps` module beside each case's module, defining a `type` alias for a dependency."""


@dataclass(frozen=True)
class Case:
    """A module written into a fresh package, beside a `deps` module, and where it's found."""

    path: Path
    header: str

    def stub(self) -> Stub:
        """Generate the module's stub, which a case can always read."""
        stub = generate(self.path)
        assert isinstance(stub, Stub)
        return stub

    def body(self, source: str) -> str:
        """Return `source` after the header every case's module starts with."""
        assert source.startswith(self.header)
        return source.removeprefix(self.header)

    @property
    def name(self) -> str:
        """The module's import name."""
        return f"{self.path.parent.name}.module"

    def rewrite(self, source: str) -> None:
        """Replace the module's source."""
        self.path.write_text(source)


type Write = Callable[..., Case]
"""Write a case's module from dedented source, and any other modules by name."""


@pytest.fixture
def write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Write:
    """
    Write cases into fresh packages under `tmp_path`, found from it as a search root.

    The working directory is `tmp_path`, as a project's root is when the command runs there.
    """
    monkeypatch.chdir(tmp_path)

    def make(source: str, **modules: str) -> Case:
        package = f"case_{next(_packages)}"
        root = tmp_path / package
        root.mkdir()
        (root / "__init__.py").write_text("")
        (root / "deps.py").write_text(DEPS)
        for name, text in modules.items():
            (root / f"{name}.py").write_text(textwrap.dedent(text))
        header = (
            "from typing import Annotated\n\n"
            "from stratae.depends import Depends, inject\n\n"
            f"from {package}.deps import StripeDep\n\n\n"
        )
        path = root / "module.py"
        path.write_text(header + textwrap.dedent(source))
        return Case(path, header)

    return make


def unparsed(source: str) -> str:
    """Return dedented `source` as the generator writes it, laid out by `ast.unparse`."""
    return ast.unparse(ast.parse(textwrap.dedent(source)))
