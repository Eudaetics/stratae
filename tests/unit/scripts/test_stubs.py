"""
Tests for generating a `.pyi` stub typing a module's injected functions.

Each `# pyright: ignore[...]` here marks an expected type error. The project enables
`reportUnnecessaryTypeIgnoreComment`, so if one of these stops being an error, the type check
fails. A line without one must type-check.

This test suite verifies the following behaviors:

Typing, through the generated `stub_sample.pyi`:
- Importers see injected functions, methods and constructors without their injected
  parameters, and a cached injected provider's override still solves its value type, an
  async generator's included.
- Every other name keeps the type it has in the source.
- The sample's stub says what the generator writes, though it's formatted.

Stubbing:
- Imports, assignments, aliases and `__all__` are kept as written, and imports nothing uses
  are dropped unless exported. Functions keep their signatures, decorators but `inject`, and
  docstrings. Classes keep their docstrings and attributes, with values replaced unless
  `Final`, and `if` blocks keep their branches. Other statements are dropped, and so is an
  `if` left empty.
- Statements keep the source's order, laid out by `ast.unparse`.
- Quoted annotations are unquoted, nested ones too, but not `Literal` values, from `typing`
  or `typing_extensions`, `Annotated` metadata or defaults.
- A `try` block keeps what its body binds, and what only its other clauses bind. A name bound
  again outside its body is warned about.
- `__all__` is kept, assigned among other names or extended, and the imports it names stay.
- An annotated value is kept only if it's `Final`, however `Final` is imported.
- An async generator, injected or not, becomes a plain function, but an async function whose
  only `yield` is in a nested function stays async.
- Each kind of parameter keeps its kind, with `/` and `*` where they belong, even when every
  kept parameter is positional-only.
- Classes defined only for type checkers are stubbed too, injections removed.
- A public function without a return annotation is warned about, but not a private one or
  `__init__`.

The command:
- It writes each stale `.pyi`, and only those. A module with nothing injected, at module
  level, in a class, or in a block, gets no stub. A stub it generated for a module no longer
  injecting is removed, failing a check, but a hand-written one is left alone.
- `--check` writes nothing, and fails when a stub is missing or says something else, but not
  when it's only laid out differently.
- Modules are named by path, folder or import name, and one not found is a problem.
- A module that doesn't parse or decode, is missing, or is under no search root is a
  problem, and fails the command.
- It runs as `python -m stratae.scripts.signatures`.
"""

import ast
import asyncio
import runpy
import sys
from pathlib import Path
from typing import Literal, assert_type

import pytest

from stratae.depends import override
from stratae.scripts.signatures import Stub, Unneeded, Unreadable, generate, main
from tests.unit.scripts import stub_sample
from tests.unit.scripts.conftest import Case, Write, unparsed
from tests.unit.scripts.stub_sample import (
    TIMEOUT,
    Account,
    Order,
    Session,
    Stripe,
    get_session,
    open_ledger,
    refund,
    request,
)


def _stub_body(case: Case, source: str) -> str:
    """Return a stub's source after its two-line header, checking the header names the module."""
    first, _, _, body = source.split("\n", 3)
    assert f"generated stub of {case.path.parent.name}.module;" in first
    return body


def test_importers_see_parameters_without_injected_ones() -> None:
    """
    Importers of a module with a generated stub should see its injected parameters removed.

    Given: The stub sample's injected function, method, constructor and cached provider
    When: They're called and overridden with right and wrong arguments
    Then: Right calls keep their types, and wrong ones are type errors
    """
    # Act / Assert
    with request:
        assert_type(Order(1).total(), int)
        assert Order(1).total() == 1
        assert asyncio.run(refund(Order(2))) == "Session order 2 late"
        assert_type(Account("jane", "team").orders(), list[Order])
    with override(get_session).to(Session(Stripe())), override(open_ledger).to(Session(Stripe())):
        pass
    override(get_session).to("wrong")  # pyright: ignore[reportArgumentType]
    override(open_ledger).to("wrong")  # pyright: ignore[reportArgumentType]
    Account("jane", "other")  # pyright: ignore[reportArgumentType]
    with pytest.raises(TypeError):
        refund(Order(1), session=None)  # pyright: ignore[reportCallIssue]
    assert_type(TIMEOUT, Literal[30])
    assert_type(Order(1).label, str)


def test_sample_stub_says_what_is_written(caplog: pytest.LogCaptureFixture) -> None:
    """
    The stub sample's `.pyi` should say what the generator writes, however it's formatted.

    Given: The stub sample and its stub, formatted
    When: Its stub is generated
    Then: Both parse to the same tree, so the sample's typing tests the generator's output
    """
    # Arrange
    path = Path(stub_sample.__file__)

    # Act
    result = generate(path)

    # Assert
    assert isinstance(result, Stub)
    written = ast.parse(path.with_suffix(".pyi").read_text())
    assert ast.dump(ast.parse(result.source)) == ast.dump(written)
    assert caplog.messages == []


def test_stub_keeps_declarations_and_drops_the_rest(write: Write) -> None:
    """
    A stub should keep what declares names, without bodies, injections or unused imports.

    Given: A module with imports, constants, classes, an injected function and statements
    When: Its stub is generated
    Then: Declarations are kept as written, values and bodies replaced, the rest dropped
    """
    # Arrange
    case = write('''\
        import os as os
        import json
        from typing import TYPE_CHECKING, Final
        from .deps import Stripe

        if TYPE_CHECKING:
            from decimal import Decimal

        if TYPE_CHECKING:
            import sys

        __all__ = ["json", "pay", "Box"]

        LIMIT: Final = 10
        size: int = 3
        count: int

        class Box:
            """A box."""

            width: int = 1

            def area(self) -> int:
                return self.width

        class Empty:
            pass

        @inject
        def pay(stripe: StripeDep, amount: "Decimal") -> Stripe:
            return stripe

        for _ in range(2):
            pass
        print(size)
        ''')

    # Act
    result = case.stub()

    # Assert
    assert (
        _stub_body(case, result.source)
        == unparsed('''\
        import os as os
        import json
        from typing import TYPE_CHECKING, Final
        from .deps import Stripe
        if TYPE_CHECKING:
            from decimal import Decimal
        __all__ = ["json", "pay", "Box"]
        LIMIT: Final = 10
        size: int = ...
        count: int

        class Box:
            """A box."""
            width: int = ...
            def area(self) -> int: ...

        class Empty: ...

        def pay(amount: Decimal) -> Stripe: ...
        ''')
        + "\n"
    )


def test_stub_unquotes_annotations_but_not_values(write: Write) -> None:
    """
    A stub should unquote forward references, but not strings that are values.

    Given: Annotations quoting a type, nested in a generic, in `Literal` imported plainly,
        renamed and from `typing_extensions`, and in `Annotated`, and a default that's a string
    When: The stub is generated
    Then: Types are unquoted, and `Literal` values, `Annotated` metadata and the default stay
        quoted
    """
    # Arrange
    case = write("""\
        from typing import Literal
        from typing import Literal as L

        from typing_extensions import Literal as TL

        class Box: ...

        @inject
        def pack(
            box: "Box", boxes: list["Box"], kind: Literal["a"], tagged: Annotated["Box", "tag"],
            label: str = "box", short: L["b"] = "b", extended: TL["c"] = "c",
        ) -> "list[Box]":
            return [box]
        """)

    # Act
    result = case.stub()

    # Assert
    assert (
        unparsed("""\
        def pack(
            box: Box,
            boxes: list[Box],
            kind: Literal["a"],
            tagged: Annotated[Box, "tag"],
            label: str = "box",
            short: L["b"] = "b",
            extended: TL["c"] = "c",
        ) -> list[Box]: ...
        """)
        in result.source
    )


def test_stub_drops_empty_if_and_unused_aliases(write: Write) -> None:
    """
    A stub should drop an `if` holding nothing it keeps, and imported names it doesn't use.

    Given: An `if __name__` block running code, aliased imports, one used, and a `from`
        import of two names, one used only in a value the stub drops
    When: The stub is generated
    Then: The block, the unused alias and the unused name are gone, the rest kept
    """
    # Arrange
    case = write("""\
        import csv as c
        import json as j
        from typing import Any, cast

        @inject
        def dump(value: int) -> str:
            return j.dumps(value)

        DUMPER: type[j.JSONEncoder] = j.JSONEncoder
        ANYTHING: Any = cast(Any, None)

        if __name__ == "__main__":
            print(dump(1))
        """)

    # Act
    result = case.stub()

    # Assert
    assert "import json as j\nfrom typing import Any\n" in result.source
    assert "csv" not in result.source
    assert "__main__" not in result.source


def test_stub_strips_classes_for_type_checkers_and_keeps_branches(write: Write) -> None:
    """
    Classes defined only for type checkers should be stubbed, and `elif`/`else` kept.

    Given: An `if`/`elif`/`else` defining a class with an injected method in each branch
    When: The stub is generated
    Then: Each branch is kept, and each method loses its injected parameter
    """
    # Arrange
    case = write("""\
        import sys

        if sys.version_info >= (3, 14):
            class Hidden:
                @inject
                def pay(self, stripe: StripeDep, amount: int) -> str: ...
        elif sys.version_info >= (3, 13):
            class Hidden:
                @inject
                def pay(self, stripe: StripeDep, amount: float) -> str: ...
        else:
            class Hidden:
                @inject
                def pay(self, stripe: StripeDep, amount: str) -> str: ...
        """)

    # Act
    result = case.stub()

    # Assert
    assert (
        unparsed("""\
        if sys.version_info >= (3, 14):
            class Hidden:
                def pay(self, amount: int) -> str: ...
        elif sys.version_info >= (3, 13):
            class Hidden:
                def pay(self, amount: float) -> str: ...
        else:
            class Hidden:
                def pay(self, amount: str) -> str: ...
        """)
        in result.source
    )


def test_stub_warns_about_missing_return_annotations(
    write: Write, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A public function without a return annotation should be warned about, as Unknown.

    Given: Public, private and `__init__` functions without return annotations
    When: The stub is generated
    Then: Only the public function and method are warned about
    """
    # Arrange
    case = write("""\
        @inject
        def shown(amount: int):
            return amount

        def _hidden(amount: int):
            return amount

        class Box:
            def __init__(self, width: int):
                self.width = width

            def area(self):
                return self.width
        """)

    # Act
    case.stub()

    # Assert
    assert len(caplog.messages) == 2
    assert "shown has no return annotation" in caplog.messages[0]
    assert "area has no return annotation" in caplog.messages[1]


def test_check_writes_nothing_and_ignores_layout(write: Write) -> None:
    """
    `--check` should fail on a missing or changed stub, and pass on one only laid out anew.

    Given: A module with an injected function and no stub
    When: The command checks, writes, checks after the stub is reflowed, writes again, and
        checks after its imports are swapped
    Then: The first check fails without writing, the reflowed stub passes and isn't
        rewritten, and the swapped one fails
    """
    # Arrange
    case = write("""\
        from decimal import Decimal
        from fractions import Fraction

        @inject
        def pay(stripe: StripeDep, amount: Decimal, share: Fraction) -> str:
            return str(amount * share)
        """)
    stub = case.path.with_suffix(".pyi")

    # Act
    first = main(["--check", str(case.path)])
    missing = not stub.exists()
    written = main([str(case.path)])
    reflowed = stub.read_text().replace(
        "def pay(amount: Decimal, share: Fraction) -> str: ...",
        "def pay(\n    amount: Decimal,\n    share: Fraction,\n) -> str:\n    ...",
    )
    stub.write_text(reflowed)
    kept = main(["--check", str(case.path)]), main([str(case.path)]), stub.read_text()
    stub.write_text(
        reflowed.replace(
            "from decimal import Decimal\nfrom fractions import Fraction",
            "from fractions import Fraction\nfrom decimal import Decimal",
        )
    )
    swapped = main(["--check", str(case.path)])

    # Assert
    assert (first, missing, written) == (1, True, 0)
    assert kept == (0, 0, reflowed)
    assert swapped == 1


def test_try_blocks_keep_what_their_body_binds(
    write: Write, caplog: pytest.LogCaptureFixture
) -> None:
    """
    A `try` block, which a stub can't hold, should give what its body binds.

    Given: A `try` importing an optional module, exported, with a fallback binding the same
        name, and a handler defining a name of its own
    When: The stub is generated
    Then: The body's import and the handler's own definition are kept, and the rebinding is
        warned about
    """
    # Arrange
    case = write("""\
        __all__ = ["fast", "fallback"]

        try:
            import json as fast
        except ImportError:
            fast = None

            @inject
            def fallback() -> None: ...
        """)

    # Act
    result = case.stub()

    # Assert
    assert (
        _stub_body(case, result.source)
        == unparsed("""\
        __all__ = ["fast", "fallback"]
        import json as fast
        def fallback() -> None: ...
        """)
        + "\n"
    )
    assert len(caplog.messages) == 1
    assert "fast is bound again outside its try block's body" in caplog.messages[0]


def test_all_keeps_what_it_names_however_assigned(write: Write) -> None:
    """
    `__all__` should keep the imports it exports, assigned among others or extended.

    Given: A module assigning `__all__` in a chained assignment, then extending it, each with
        a name it only imports
    When: The stub is generated
    Then: Both assignments and both imports are kept
    """
    # Arrange
    case = write("""\
        from json import dumps, loads

        __all__ = exported = ["pay", "loads"]
        __all__ += ["dumps"]

        @inject
        def pay() -> None: ...
        """)

    # Act
    result = case.stub()

    # Assert
    assert "from json import dumps, loads\n" in result.source
    assert unparsed('__all__ = exported = ["pay", "loads"]') in result.source
    assert unparsed('__all__ += ["dumps"]') in result.source


def test_annotated_values_kept_only_when_final(write: Write) -> None:
    """
    An annotated assignment's value should be kept only when it's `Final`, however imported.

    Given: Values annotated with `Final` renamed, `typing.Final`, and a class named like it
    When: The stub is generated
    Then: Both `Final` values are kept, and the other is replaced by `...`
    """
    # Arrange
    case = write("""\
        import typing
        from typing import Final as Fixed

        class MyFinalThing: ...

        LIMIT: Fixed = 10
        SIZE: typing.Final[int] = 3
        thing: MyFinalThing = MyFinalThing()

        @inject
        def pay() -> None: ...
        """)

    # Act
    result = case.stub()

    # Assert
    assert "LIMIT: Fixed = 10\n" in result.source
    assert "SIZE: typing.Final[int] = 3\n" in result.source
    assert "thing: MyFinalThing = ...\n" in result.source


def test_unparsable_module_is_a_problem(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    A module that doesn't parse should be a problem, and get no stub.

    Given: A Python file with a syntax error
    When: Its stub is generated
    Then: The result names the problem
    """
    # Arrange
    path = tmp_path / "bad.py"
    path.write_text("def broken(:\n")
    monkeypatch.chdir(tmp_path)

    # Act
    result = generate(path)

    # Assert
    assert isinstance(result, Unreadable)
    assert "can't parse" in result.problem


@pytest.mark.parametrize(
    ("name", "content", "problem"),
    [
        pytest.param("bad.py", b"def broken(:\n", "can't parse", id="syntax error"),
        pytest.param("bad.py", b"\xff\xfe\n", "can't parse: Non-UTF-8", id="undecodable"),
        pytest.param("missing.py", None, "can't read", id="missing"),
    ],
)
def test_unreadable_modules_fail_the_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    name: str,
    content: bytes | None,
    problem: str,
) -> None:
    """
    A module that can't be read should be reported, and fail the command.

    Given: A file with a syntax error, one that can't be decoded, or no file
    When: The command runs on it, from the folder holding it
    Then: It exits non-zero and names the problem
    """
    # Arrange
    path = tmp_path / name
    if content is not None:
        path.write_bytes(content)
    monkeypatch.chdir(tmp_path)

    # Act
    status = main([str(path)])

    # Assert
    assert status == 1
    assert problem in capsys.readouterr().err


def test_module_off_every_root_is_a_problem(tmp_path: Path) -> None:
    """
    A module under no search root should be a problem, since its import name is unknown.

    Given: A module outside the working directory and `sys.path`
    When: Its stub is generated
    Then: The result says it can't be named
    """
    # Arrange
    path = tmp_path / "loose.py"
    path.write_text("x = 1\n")

    # Act
    result = generate(path)

    # Assert
    assert isinstance(result, Unreadable)
    assert "can't name it: it isn't under the working directory or sys.path" in result.problem


def test_modules_are_named_by_path_or_import_name(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """
    The command should find a module by import name, and report a name it can't find.

    Given: The stub sample, current, and a module that doesn't exist
    When: The command checks each by import name
    Then: The sample passes, and the missing one is a problem, failing the command
    """
    # Arrange
    monkeypatch.chdir(Path(stub_sample.__file__).parents[3])

    # Act
    found = main(["--check", "tests.unit.scripts.stub_sample"])
    missing = main(["--check", "tests.unit.scripts.no_such_module"])

    # Assert
    assert (found, missing) == (0, 1)
    assert "tests.unit.scripts.no_such_module: can't find it, as a path or a module" in (
        capsys.readouterr().err
    )


def test_folders_are_searched_for_modules(write: Write, caplog: pytest.LogCaptureFixture) -> None:
    """
    A folder given to the command should have a stub written for each injecting module in it.

    Given: A package holding a module with an injected function without a return annotation,
        a module with nothing injected, and a class holding the only injected method
    When: The command runs on the package's folder
    Then: The injecting modules get stubs, the other none, and the annotation is warned about
    """
    # Arrange
    case = write(
        """\
        @inject
        def shown(amount: int):
            return amount
        """,
        held="""\
        from stratae.depends import inject

        class Box:
            @inject
            def area(self) -> int: ...
        """,
    )

    # Act
    status = main([str(case.path.parent)])

    # Assert
    assert status == 0
    assert case.path.with_suffix(".pyi").exists()
    assert case.path.with_name("held.pyi").exists()
    assert not case.path.with_name("deps.pyi").exists()
    assert "shown has no return annotation" in caplog.text


def test_module_with_nothing_injected_gets_no_stub(write: Write) -> None:
    """
    A module with nothing injected should get no stub, since a stub could only lose types.

    Given: A module whose only function isn't injected, and one injecting only in a function
    When: Their stubs are generated
    Then: Neither gets one
    """
    # Arrange
    plain = write("""\
        def plain(stripe: StripeDep, amount: int) -> int:
            return amount
        """)
    nested = write("""\
        def outer() -> None:
            @inject
            def inner(stripe: StripeDep) -> None: ...
        """)

    # Act / Assert
    assert generate(plain.path) == Unneeded(plain.path.resolve().with_suffix(".pyi"))
    assert generate(nested.path) == Unneeded(nested.path.resolve().with_suffix(".pyi"))


def test_left_over_stubs_are_removed_but_not_hand_written_ones(write: Write) -> None:
    """
    A generated stub beside a module no longer injecting should go, but not one by hand.

    Given: A module whose stub was generated, then whose function stopped being injected, and
        a module with nothing injected beside a hand-written stub
    When: The command checks both, and then runs on both
    Then: The check fails and keeps the files, and the run removes only the generated stub
    """
    # Arrange
    case = write("""\
        @inject
        def pay(stripe: StripeDep, amount: int) -> str: ...
        """)
    main([str(case.path)])
    case.rewrite(case.path.read_text().replace("@inject\n", ""))
    generated = case.path.with_suffix(".pyi")
    hand = case.path.with_name("deps.pyi")
    hand.write_text("class Stripe: ...\n")
    modules = [str(case.path), str(hand.with_suffix(".py"))]

    # Act
    checked = main(["--check", *modules])
    kept = generated.exists(), hand.exists()
    ran = main(modules)

    # Assert
    assert (checked, kept, ran) == (1, (True, True), 0)
    assert not generated.exists()
    assert hand.read_text() == "class Stripe: ...\n"


def test_module_runs_as_a_command(write: Write, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Running the module should check the modules named and exit with the check's status.

    Given: A module whose stub is current
    When: `python -m stratae.scripts.signatures --check` runs on it
    Then: It exits zero
    """
    # Arrange
    case = write("""\
        @inject
        def plain(amount: int) -> int:
            return amount
        """)
    main([str(case.path)])
    monkeypatch.setattr(sys, "argv", ["signatures", "--check", str(case.path)])
    monkeypatch.delitem(sys.modules, "stratae.scripts.signatures")

    # Act
    with pytest.raises(SystemExit) as exited:
        runpy.run_module("stratae.scripts.signatures", run_name="__main__")

    # Assert
    assert exited.value.code == 0


def test_async_generators_become_plain_functions(write: Write) -> None:
    """
    An async generator's stub should be a plain function, since `async def` there is a coroutine.

    Given: An injected async generator, one that isn't injected and yields in a block, and an
        async function whose only `yield`s are in a nested function and a lambda
    When: The stub is generated
    Then: Both generators become plain functions, and the coroutine stays async
    """
    # Arrange
    case = write("""\
        from collections.abc import AsyncGenerator, AsyncIterator

        @inject
        async def stream(stripe: StripeDep) -> AsyncGenerator[int]:
            yield 1

        async def lines() -> AsyncIterator[str]:
            if True:
                yield "line"

        async def fetch() -> int:
            def numbers():
                yield 1
            pick = lambda: (yield)
            return 1
        """)

    # Act
    result = case.stub()

    # Assert
    assert unparsed("def stream() -> AsyncGenerator[int]: ...") in result.source
    assert unparsed("def lines() -> AsyncIterator[str]: ...") in result.source
    assert unparsed("async def fetch() -> int: ...") in result.source


def test_parameters_keep_their_kinds(write: Write) -> None:
    """
    A stub should keep each kind of parameter, with `/` and `*` where they belong.

    Given: An injected generic function with every kind of parameter, and one whose kept
        parameters are all positional-only
    When: The stub is generated
    Then: Each kind is kept, and `/` closes the positional-only ones
    """
    # Arrange
    case = write("""\
        @inject
        def echo[T](value: T, /, stripe: StripeDep, other: int, *rest: int, **extra: str) -> T:
            return value

        @inject
        def pay(stripe: StripeDep, amount: int, /) -> str:
            return str(amount)
        """)

    # Act
    result = case.stub()

    # Assert
    assert unparsed("def echo[T](value: T, /, other: int, *rest: int, **extra: str) -> T: ...") in (
        result.source
    )
    assert unparsed("def pay(amount: int, /) -> str: ...") in result.source
