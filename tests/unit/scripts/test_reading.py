"""
Tests for telling injected parameters apart from the source alone, as `inject` would.

Each case is checked against `inject` itself: the test imports the module it wrote, which the
generator never does, and compares the parameters `inject` kept with those in the stub.

This test suite verifies the following behaviors:

Injected:
- `Annotated[..., Depends(...)]` inline, with `Depends` imported however it is: directly,
  renamed, or through a module alias or dotted module name.
- Through a plain or annotated alias, a `type` alias, a plain alias of a `type` alias, a
  generic `type` alias subscripted, and an alias imported from another module, absolutely or
  relatively.
- Nested `Annotated`, `typing.Annotated` through a module alias, and a provider kept in a
  variable.

Not injected, as at runtime:
- A `type` alias of a `type` alias, which `inject` unwraps only once.
- An optional dependency, a quoted annotation, other metadata, and a local `Depends` that
  isn't Stratae's.

Names that lead nowhere:
- Import cycles and assignments referring to each other end as not injected.
- Attributes of classes, and of expressions other than names, aren't followed.

Finding modules:
- A package or module off the search roots is found where the import system puts it, as
  one installed in editable mode is, without importing it. One loaded without a spec isn't.

Modules that can't be read:
- Each is warned about, and names imported from them are taken as not injected, even one
  named `Annotated`. A module that doesn't parse can't be read either.
"""

import ast
import importlib
import importlib.machinery
import inspect
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

import pytest

from tests.unit.scripts.conftest import Write, unparsed

_INJECTED = [
    pytest.param("", "Annotated[int, Depends(get_one)]", id="inline"),
    pytest.param(
        "from stratae.depends import Depends as D", "Annotated[int, D(get_one)]", id="renamed"
    ),
    pytest.param(
        "import stratae.depends as sd", "Annotated[int, sd.Depends(get_one)]", id="module"
    ),
    pytest.param(
        "import stratae.depends", "Annotated[int, stratae.depends.Depends(get_one)]", id="dotted"
    ),
    pytest.param("One = Annotated[int, Depends(get_one)]", "One", id="plain alias"),
    pytest.param("type One = Annotated[int, Depends(get_one)]", "One", id="type alias"),
    pytest.param(
        "from typing import TypeAlias\nOne: TypeAlias = Annotated[int, Depends(get_one)]",
        "One",
        id="annotated alias",
    ),
    pytest.param(
        "type One = Annotated[int, Depends(get_one)]\nTwo = One", "Two", id="alias of alias"
    ),
    pytest.param("type One[T] = Annotated[T, Depends(get_one)]", "One[int]", id="generic alias"),
    pytest.param("", "StripeDep", id="imported alias"),
    pytest.param("from .deps import StripeDep as Rel", "Rel", id="relative import"),
    pytest.param("from . import deps", "deps.StripeDep", id="module attribute"),
    pytest.param("", 'Annotated[Annotated[int, Depends(get_one)], "doc"]', id="nested"),
    pytest.param("import typing as t", "t.Annotated[int, Depends(get_one)]", id="typing module"),
    pytest.param("one = Depends(get_one)", "Annotated[int, one]", id="provider variable"),
]

_NOT_INJECTED = [
    pytest.param(
        "type One = Annotated[int, Depends(get_one)]\ntype Two = One", "Two", id="alias twice"
    ),
    pytest.param("", "StripeDep | None", id="optional"),
    pytest.param("", '"int"', id="quoted"),
    pytest.param("", 'Annotated[int, "doc"]', id="other metadata"),
    pytest.param(
        "def Depends(f): return f", "Annotated[int, Depends(get_one)]", id="local Depends"
    ),
    pytest.param("", "int", id="plain"),
]


def _case(write: Write, setup: str, annotation: str) -> tuple[set[str], set[str]]:
    """Return the parameters kept in `pay`'s stub, and the ones `inject` keeps at runtime."""
    case = write(
        "def get_one() -> int:\n"
        "    return 1\n\n\n"
        f"{setup}\n\n\n"
        "@inject\n"
        f"def pay(value: {annotation}, amount: int) -> int:\n"
        "    return amount\n"
    )
    result = case.stub()
    stub = next(
        node
        for node in ast.parse(result.source).body
        if isinstance(node, ast.FunctionDef) and node.name == "pay"
    )
    kept = {arg.arg for arg in stub.args.args}
    sys.path.insert(0, str(case.path.parents[1]))
    try:
        module = importlib.import_module(f"{case.path.parent.name}.module")
    finally:
        sys.path.remove(str(case.path.parents[1]))
    return kept, set(inspect.signature(module.pay).parameters)


@pytest.mark.parametrize(("setup", "annotation"), _INJECTED)
def test_injected_parameters_are_found(write: Write, setup: str, annotation: str) -> None:
    """
    A parameter `inject` injects should be found from the source and left out of the stub.

    Given: A function whose first parameter is injected, written one of many ways
    When: Its stub is generated, and the module imported for comparison
    Then: The stub keeps only `amount`, as `inject` does
    """
    # Act
    kept, runtime = _case(write, setup, annotation)

    # Assert
    assert kept == runtime == {"amount"}


@pytest.mark.parametrize(("setup", "annotation"), _NOT_INJECTED)
def test_other_parameters_are_kept(write: Write, setup: str, annotation: str) -> None:
    """
    A parameter `inject` doesn't inject should be kept in the stub, as at runtime.

    Given: A function whose first parameter only looks injected, or isn't
    When: Its stub is generated, and the module imported for comparison
    Then: The stub keeps both parameters, as `inject` does
    """
    # Act
    kept, runtime = _case(write, setup, annotation)

    # Assert
    assert kept == runtime == {"value", "amount"}


def test_unreadable_modules_are_warned_about(
    write: Write, caplog: pytest.LogCaptureFixture
) -> None:
    """
    An imported module that can't be found should be warned about, its names not injected.

    Given: A function annotated with names from a module that isn't installed, and an
        attribute of one
    When: Its stub is generated
    Then: The parameters are kept, and one warning names the module and each of them
    """
    # Arrange
    case = write("""\
        from not_installed_anywhere import Thing, tools

        @inject
        def pay(thing: Thing, tool: tools.Tool, stripe: StripeDep, amount: int) -> int:
            return amount
        """)

    # Act
    result = case.stub()

    # Assert
    assert (
        unparsed("def pay(thing: Thing, tool: tools.Tool, amount: int) -> int: ...")
        in result.source
    )
    unread = "couldn't read module not_installed_anywhere, so pay's parameter"
    assert [message.split(": ", 1)[1] for message in caplog.messages] == [
        f"{unread} thing is taken as not injected",
        f"{unread} tool is taken as not injected",
    ]


def test_star_imports_and_package_modules(write: Write, tmp_path: Path) -> None:
    """
    Names from a star import should be followed, and a package's `__init__` read as itself.

    Given: A module star-importing an alias from a sibling, in a package re-exporting it
    When: Its stub is generated
    Then: The aliased parameter is found injected through the star import
    """
    # Arrange
    case = write(
        """\
        from .aliases import *

        @inject
        def pay(stripe: Aliased, amount: int) -> int:
            return amount
        """,
        aliases="from .deps import StripeDep as Aliased\n",
    )

    # Act
    result = case.stub()

    # Assert
    assert unparsed("def pay(amount: int) -> int: ...") in result.source


def test_cycles_end_as_not_injected(write: Write) -> None:
    """
    Names that only lead back to themselves should be taken as not injected.

    Given: An annotation imported through a cycle between two modules, and one assigned in a
        loop of assignments
    When: Its stub is generated
    Then: Both parameters are kept
    """
    # Arrange
    case = write(
        """\
        from .left import Looped

        Ping = Pong
        Pong = Ping

        @inject
        def pay(looped: Looped, ping: Ping, amount: int) -> int:
            return amount
        """,
        left="from .right import Looped\n",
        right="from .left import Looped\n",
    )

    # Act
    result = case.stub()

    # Assert
    assert unparsed("def pay(looped: Looped, ping: Ping, amount: int) -> int: ...") in result.source


def test_attributes_of_classes_and_expressions_are_not_followed(write: Write) -> None:
    """
    A class's attribute, or an attribute of an expression, should be taken as not injected.

    Given: A parameter annotated with a nested class, under a decorator taken from a list
    When: Its stub is generated
    Then: The parameter is kept, and the decorator doesn't stop `inject` being found
    """
    # Arrange
    case = write("""\
        from functools import wraps

        class Box:
            class Inner: ...

        class Wrapper:
            def wrap(self, func):
                return func

        handlers = [Wrapper()]

        @handlers[0].wrap
        @inject
        def pay(inner: Box.Inner, stripe: StripeDep) -> int:
            return 1
        """)

    # Act
    result = case.stub()

    # Assert
    assert unparsed("def pay(inner: Box.Inner) -> int: ...") in result.source


class _EditableFinder:
    """Finds one package or module where an editable install's finder would: off `sys.path`."""

    def __init__(self, name: str, location: Path) -> None:
        self.name, self.location = name, location

    def find_spec(
        self, name: str, path: Sequence[str] | None, target: ModuleType | None = None
    ) -> importlib.machinery.ModuleSpec | None:
        if name != self.name:
            return None
        if self.location.is_file():
            return importlib.machinery.ModuleSpec(name, None, origin=str(self.location))
        spec = importlib.machinery.ModuleSpec(name, None, is_package=True)
        spec.submodule_search_locations = [str(self.location)]
        return spec


def test_editable_packages_are_found_without_importing(
    write: Write, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A package off the search roots should be found through the import system's finders.

    Given: An alias in a package that only a finder knows about, which raises on import
    When: A function using the alias has its stub generated
    Then: The alias is found injected, and the package was never imported
    """
    # Arrange
    folder = tmp_path / "elsewhere" / "editable_pkg"
    folder.mkdir(parents=True)
    (folder / "__init__.py").write_text("raise RuntimeError('imported')\n")
    (folder / "aliases.py").write_text(
        "from typing import Annotated\n"
        "from stratae.depends import Depends\n"
        "def get_one() -> int: ...\n"
        "type One = Annotated[int, Depends(get_one)]\n"
    )
    monkeypatch.setattr(sys, "meta_path", [_EditableFinder("editable_pkg", folder), *sys.meta_path])
    case = write("""\
        from editable_pkg.aliases import One

        @inject
        def pay(one: One, amount: int) -> int:
            return amount
        """)

    # Act
    result = case.stub()

    # Assert
    assert unparsed("def pay(amount: int) -> int: ...") in result.source
    assert "editable_pkg" not in sys.modules
    assert caplog.messages == []


def test_editable_modules_and_specless_ones(
    write: Write, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A single-file module found by a finder should be read, and a specless one skipped.

    Given: An alias in a module only a finder knows about, and a loaded module with no spec
    When: A function using the alias and a name from the specless module has its stub made
    Then: The alias is found injected, and the specless module is warned about
    """
    # Arrange
    module = tmp_path / "elsewhere" / "editable_mod.py"
    module.parent.mkdir()
    module.write_text(
        "from typing import Annotated\n"
        "from stratae.depends import Depends\n"
        "def get_one() -> int: ...\n"
        "type One = Annotated[int, Depends(get_one)]\n"
    )
    specless = ModuleType("specless_mod")
    specless.__spec__ = None
    monkeypatch.setattr(sys, "meta_path", [_EditableFinder("editable_mod", module), *sys.meta_path])
    monkeypatch.setitem(sys.modules, "specless_mod", specless)
    case = write("""\
        from editable_mod import One
        from specless_mod import Thing

        @inject
        def pay(one: One, thing: Thing) -> int:
            return 1
        """)

    # Act
    result = case.stub()

    # Assert
    assert unparsed("def pay(thing: Thing) -> int: ...") in result.source
    assert len(caplog.messages) == 1
    assert "specless_mod" in caplog.messages[0]


def test_unreadable_names_are_never_trusted(write: Write, caplog: pytest.LogCaptureFixture) -> None:
    """
    A name from a module that can't be read, or doesn't parse, shouldn't count as injected.

    Given: `Annotated` imported from a module that isn't installed, and an alias imported
        from a sibling module with a syntax error
    When: Its stub is generated
    Then: Both parameters are kept, and both modules are warned about
    """
    # Arrange
    case = write(
        """\
        from missing_lib import Annotated as Looks
        from .broken import Aliased

        def get_one() -> int:
            return 1

        @inject
        def pay(looks: Looks[int, Depends(get_one)], aliased: Aliased, amount: int) -> int:
            return amount
        """,
        broken="def broken(:\n",
    )

    # Act
    result = case.stub()

    # Assert
    kept = "looks: Looks[int, Depends(get_one)], aliased: Aliased, amount: int"
    assert unparsed(f"def pay({kept}) -> int: ...") in result.source
    assert len(caplog.messages) == 2
    assert "missing_lib" in caplog.messages[0]
    assert "broken" in caplog.messages[1]
