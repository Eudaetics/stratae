"""The command: generate or check the stubs of the modules it's given."""

import ast
import functools
import sys
from argparse import ArgumentParser
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from ._modules import Unreadable, find, search_roots
from ._stub import MARK, Stub, Unneeded, generate


@dataclass(frozen=True)
class _Change:
    """A file to change: how, and what to say once it's done, or while it's still to do."""

    path: Path
    make: Callable[[], None]
    done: str
    pending: str


def main(argv: Sequence[str] | None = None) -> int:
    """
    Generate or check the stub of each module named.

    Every stub is worked out, and compared with the file beside its module, before any file
    is written. Each file is then replaced whole, so none is ever left half-written. A stub
    generated for a module that no longer injects anything is removed.

    :param argv: The arguments, `sys.argv[1:]` by default.
    :returns: The exit status: 1 if a module couldn't be found or read, or a check found a
        stub to change.
    """
    options = _parser().parse_args(argv)
    results = [result for name in options.modules for result in _results(name)]
    problems = _problems(results)
    pending = (_check if options.check else _make)(_all_changes(results))
    return int(bool(problems or pending))


def _parser() -> ArgumentParser:
    parser = ArgumentParser(prog="python -m stratae.scripts.signatures")
    parser.add_argument(
        "modules", nargs="+", help="modules by path, folder or import name; folders recurse"
    )
    parser.add_argument("--check", action="store_true", help="write nothing; fail if stale")
    return parser


def _results(name: str) -> list[Stub | Unneeded | Unreadable]:
    """Return the result for the module named, by path or import name, or each in a folder."""
    path = Path(name)
    if path.is_dir():
        return [generate(module) for module in sorted(path.rglob("*.py"))]
    if path.suffix == ".py" or path.is_file():
        return [generate(path)]
    found = find(name, search_roots())
    if found is None:
        return [Unreadable(path, f"{name}: can't find it, as a path or a module")]
    return [generate(found)]


def _changes(result: Stub | Unneeded | Unreadable) -> list[_Change]:
    """Return the change a result asks for: writing a stale stub, or removing one left over."""
    match result:
        case Stub() if _stale(result):
            write = functools.partial(_write, result)
            return [_Change(result.path, write, "stub updated", "stub is missing or out of date")]
        case Unneeded() if _generated(result):
            left = "stub is left from functions no longer injected"
            return [_Change(result.path, result.path.unlink, "stub removed", left)]
        case _:
            return []


def _problems(results: Iterable[Stub | Unneeded | Unreadable]) -> list[str]:
    """Report each module that couldn't be found or read, and return what's wrong with each."""
    problems = [result.problem for result in results if isinstance(result, Unreadable)]
    for problem in problems:
        print(problem, file=sys.stderr)
    return problems


def _all_changes(results: Iterable[Stub | Unneeded | Unreadable]) -> list[_Change]:
    return [change for result in results for change in _changes(result)]


def _check(changes: list[_Change]) -> list[_Change]:
    """Report each change a run would make, and return them, since they're still to do."""
    for change in changes:
        print(f"{change.path}: {change.pending}", file=sys.stderr)
    return changes


def _make(changes: list[_Change]) -> list[_Change]:
    """Make each change, and return what's still to do, which is nothing."""
    for change in changes:
        change.make()
        print(f"{change.path}: {change.done}")
    return []


def _stale(stub: Stub) -> bool:
    """Whether the file at the stub's path is missing, or says something else."""
    try:
        current = ast.parse(stub.path.read_bytes())
    except (OSError, SyntaxError):
        return True
    return ast.dump(current) != ast.dump(ast.parse(stub.source))


def _generated(unneeded: Unneeded) -> bool:
    """Whether a stub this command generated is where an unneeded one would go."""
    try:
        with unneeded.path.open("rb") as file:
            return file.read(len(MARK)) == MARK.encode()
    except OSError:
        return False


def _write(stub: Stub) -> None:
    """Replace the file whole, in UTF-8."""
    temporary = stub.path.with_name(f".{stub.path.name}.stratae")
    temporary.write_text(stub.source, encoding="utf-8", newline="\n")
    temporary.replace(stub.path)
