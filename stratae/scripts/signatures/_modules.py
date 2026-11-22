"""Find modules' source and parse it into values, without importing anything."""

import ast
import functools
import importlib.util
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FromImport:
    """A name bound by `from module import name`, with `module` absolute."""

    module: str
    name: str


@dataclass(frozen=True)
class ModuleImport:
    """A module bound by `import module`."""

    module: str


@dataclass(frozen=True)
class Defined:
    """A name defined in the module itself, by the statement defining it."""

    node: ast.stmt


type Binding = FromImport | ModuleImport | Defined
"""What a module binds a name to."""


@dataclass(frozen=True)
class Module:
    """
    A module's source, parsed.

    `bindings` holds the last binding of each name at module level, `if` and `try` blocks
    included. `stars` holds the modules it star-imports, the last first.
    """

    name: str
    path: Path
    tree: ast.Module
    bindings: Mapping[str, Binding]
    stars: tuple[str, ...]

    @property
    def is_package(self) -> bool:
        """Whether the module is a package's `__init__`."""
        return self.path.stem == "__init__"


type Load = Callable[[str], Module | None]
"""Loads a module by import name, or gives `None` if it can't be found or read."""


def parse(name: str, path: Path) -> Module:
    """
    Parse the module `name` from its file, decoded as Python decodes source.

    :raises SyntaxError: If the source doesn't parse, or can't be decoded.
    """
    tree = ast.parse(path.read_bytes(), filename=str(path))
    package = name if path.stem == "__init__" else name.rpartition(".")[0]
    statements = list(flattened(tree.body))
    return Module(
        name, path, tree, _last_bindings(statements, package), _stars(statements, package)
    )


def _last_bindings(statements: list[ast.stmt], package: str) -> dict[str, Binding]:
    """Return the last binding of each name the statements bind."""
    return {name: binding for node in statements for name, binding in _bindings(node, package)}


def _stars(statements: list[ast.stmt], package: str) -> tuple[str, ...]:
    """Return the modules the statements star-import, the last first."""
    stars: list[str] = []
    for node in statements:
        if isinstance(node, ast.ImportFrom) and node.names[0].name == "*":
            stars.append(absolute(node, package))
    return tuple(reversed(stars))


def flattened(body: list[ast.stmt]) -> Iterator[ast.stmt]:
    """Yield `body`'s statements, with those in its `if` and `try` blocks in their place."""
    for node in body:
        yield node
        for clause in clauses(node):
            yield from flattened(clause)


def clauses(node: ast.stmt) -> list[list[ast.stmt]]:
    """Return the body of each clause of an `if` or `try`, or none for any other statement."""
    match node:
        case ast.If():
            return [node.body, node.orelse]
        case ast.Try() | ast.TryStar():
            handlers = [handler.body for handler in node.handlers]
            return [node.body, *handlers, node.orelse, node.finalbody]
        case _:
            return []


def binds(node: ast.stmt) -> set[str]:
    """Return the names a statement binds, by import or definition."""
    match node:
        case ast.Import() | ast.ImportFrom():
            return {_imported(alias, node) for alias in _named_aliases(node)}
        case ast.Assign(targets=targets):
            return _target_names(targets)
        case ast.AnnAssign(target=ast.Name(id=name)) | ast.TypeAlias(name=ast.Name(id=name)):
            return {name}
        case ast.FunctionDef() | ast.AsyncFunctionDef() | ast.ClassDef():
            return {node.name}
        case _:
            return set()


def _named_aliases(node: ast.Import | ast.ImportFrom) -> list[ast.alias]:
    """Return an import's aliases, but a star, which binds no name of its own."""
    return [alias for alias in node.names if alias.name != "*"]


def _target_names(targets: list[ast.expr]) -> set[str]:
    """Return the names assignment targets bind, unpacked ones included."""
    names: set[str] = set()
    for target in targets:
        names |= {
            node.id
            for node in ast.walk(target)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store)
        }
    return names


def _imported(alias: ast.alias, node: ast.Import | ast.ImportFrom) -> str:
    """Return the name an import's alias binds."""
    if alias.asname is not None:
        return alias.asname
    return alias.name.split(".")[0] if isinstance(node, ast.Import) else alias.name


def _bindings(node: ast.stmt, package: str) -> list[tuple[str, Binding]]:
    """Return each name a statement binds, and what it's bound to."""
    match node:
        case ast.Import():
            return [(_imported(alias, node), _module_import(alias)) for alias in node.names]
        case ast.ImportFrom():
            source = absolute(node, package)
            return [
                (_imported(alias, node), FromImport(source, alias.name))
                for alias in _named_aliases(node)
            ]
        case _:
            return [(name, Defined(node)) for name in binds(node)]


def _module_import(alias: ast.alias) -> ModuleImport:
    """Return what `import a.b` binds: `a`, or `a.b` itself when it's renamed."""
    return ModuleImport(alias.name if alias.asname else alias.name.split(".")[0])


def absolute(node: ast.ImportFrom, package: str) -> str:
    """Return the absolute name of the module a `from` import imports from."""
    if not node.level:
        return node.module or ""
    parts = package.split(".")
    base = parts[: len(parts) - node.level + 1]
    return ".".join([*base, *([node.module] if node.module else [])])


def search_roots() -> list[Path]:
    """Return where modules are found: the working directory, then `sys.path`."""
    return [Path(entry or ".").resolve() for entry in [str(Path.cwd()), *sys.path]]


def module_name(path: Path, roots: Sequence[Path]) -> str | None:
    """Return the import name of the file `path`, from the first of `roots` holding it."""
    resolved = path.resolve().with_suffix("")
    root = next((root for root in roots if resolved.is_relative_to(root)), None)
    if root is None:
        return None
    parts = resolved.relative_to(root).parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def find(name: str, roots: Sequence[Path]) -> Path | None:
    """
    Return the source file of the module `name`, on `roots` or where the import system has it.

    The import system's finders are only asked where a top-level package is, which imports
    nothing. That finds packages installed in editable mode, whose source isn't on `sys.path`.
    """
    top, *rest = name.split(".")
    for root in roots:
        found = _source_file(root.joinpath(top, *rest))
        if found is not None:
            return found
    for folder in _installed(top):
        found = _source_file(folder.joinpath(*rest))
        if found is not None:
            return found
    return None


def loader(roots: Sequence[Path]) -> Load:
    """Return a function loading modules from `roots`, reading each at most once."""

    @functools.cache
    def load(name: str) -> Module | None:
        path = find(name, roots)
        if path is None:
            return None
        try:
            return parse(name, path)
        except SyntaxError:
            return None

    return load


def _source_file(base: Path) -> Path | None:
    """Return the source file of the module at `base`: a file, or a package's `__init__`."""
    candidates = (base.with_suffix(".py"), base / "__init__.py", base.with_suffix(".pyi"))
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def _installed(top: str) -> list[Path]:
    """
    Return the folders the import system finds the top-level module `top` in.

    A plain module is given as its own folder, so its file is found there. A module already
    loaded without a spec, as `__main__` can be, isn't found.
    """
    try:
        spec = importlib.util.find_spec(top)
    except ValueError:
        return []
    if spec is None:
        return []
    if spec.submodule_search_locations is not None:
        return [Path(location) for location in spec.submodule_search_locations]
    return [Path(spec.origin).with_suffix("")] if spec.origin else []


@dataclass(frozen=True)
class Unreadable:
    """A module that couldn't be read, and why."""

    path: Path
    problem: str


def opened(path: Path) -> tuple[Module, Load] | Unreadable:
    """
    Parse the module at `path`, and return it with a loader for the modules it imports.

    A module is named from the first search root holding it, so one under none can't be named.
    """
    roots = search_roots()
    name = module_name(path, roots)
    if name is None:
        return Unreadable(
            path, f"{path}: can't name it: it isn't under the working directory or sys.path"
        )
    try:
        module = parse(name, path.resolve())
    except SyntaxError as error:
        return Unreadable(path, f"{path}:{error.lineno}: can't parse: {error.msg}")
    except OSError as error:
        return Unreadable(path, f"{path}: can't read: {error.strerror}")
    return module, loader(roots)
