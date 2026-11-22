"""
Write a `.pyi` beside a module, typing its injected functions without injections.

A type checker reads the stub in place of the module, so the stub declares everything the
module does. The stub is the module's syntax tree with its bodies, values and injected
parameters taken out. It's then pruned of the imports nothing in it uses, and unparsed.
"""

import ast
import copy
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from ._modules import Load, Module, Unreadable, binds, clauses, flattened, opened
from ._names import Unread
from ._rules import (
    ANNOTATED,
    LITERAL,
    Def,
    Verdict,
    arguments,
    decorator,
    is_final,
    refers_to,
    verdicts,
)

MARK = "# stratae: generated stub of "
"""How each generated `.pyi` starts, so one left behind can be told from one written by hand."""

HEADER = (
    MARK + "{module}; regenerate it rather than editing it.\n"
    "# Its injected functions are typed without their injected parameters.\n\n"
)
"""The comment heading each generated `.pyi`."""

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Stub:
    """A module's stub: where it goes, and its text."""

    path: Path
    source: str


@dataclass(frozen=True)
class Unneeded:
    """A module with nothing injected, which needs no stub, and where one would go."""

    path: Path


def generate(path: Path) -> Stub | Unneeded | Unreadable:
    """
    Return the `.pyi` stub for the module at `path`, if it has an injected function.

    Imports, assignments and type aliases are kept, since checkers read their expressions in
    stubs too. Imports the stub doesn't use or export are dropped. Functions keep their
    signatures, decorators and docstrings, with bodies replaced by `...`. Injected ones lose
    their injected parameters and their `inject` decorator. An async generator becomes a plain
    function, since an `async def` without `yield` declares a coroutine. Classes and `if`
    blocks keep their structure. A `try` block keeps what its body binds. Forward references
    are unquoted, and other statements are dropped.

    Each public function without a return annotation is logged as a warning, since importers
    see it returning Unknown. So is each name a `try` block's other clauses bind again, and
    each parameter whose annotation comes from a module that couldn't be read.

    :param path: The module's source file.
    :returns: The stub, to go beside the module, or why the module couldn't be read. A module
        with nothing injected needs none, since a stub could only lose what it says.
    """
    found = opened(path)
    if isinstance(found, Unreadable):
        return found
    module, load = found
    if not _injects(load, module, module.tree.body):
        return Unneeded(module.path.with_suffix(".pyi"))
    body = _body(load, module, module.tree.body)
    stub = ast.Module(_prune(body, _needed(body)), type_ignores=[])
    text = HEADER.format(module=module.name) + ast.unparse(stub) + "\n"
    return Stub(module.path.with_suffix(".pyi"), text)


def _with[Node: ast.AST](node: Node, **fields: object) -> Node:
    """Return a copy of `node` with `fields` replaced, leaving `node` as it was."""
    new = copy.copy(node)
    for name, value in fields.items():
        setattr(new, name, value)
    return new


def _ellipsis() -> ast.Expr:
    return ast.Expr(ast.Constant(...))


def _injects(load: Load, module: Module, statements: list[ast.stmt]) -> bool:
    """Whether a function `statements` define is injected, or a method of a class they define."""
    for node in flattened(statements):
        match node:
            case ast.ClassDef() if _injects(load, module, node.body):
                return True
            case ast.FunctionDef() | ast.AsyncFunctionDef() if decorator(load, node, module):
                return True
            case _:
                continue
    return False


# Stubbing each statement.


def _body(load: Load, module: Module, statements: Iterable[ast.stmt]) -> list[ast.stmt]:
    return [stub for node in statements for stub in _statement(load, module, node)]


def _statement(load: Load, module: Module, node: ast.stmt) -> list[ast.stmt]:
    """Return the stub of one statement: itself, a stub of it, or nothing."""
    match node:
        case ast.Import() | ast.ImportFrom() | ast.Assign() | ast.AugAssign() | ast.TypeAlias():
            return [node]
        case ast.AnnAssign():
            return [_annotated(load, module, node)]
        case ast.FunctionDef() | ast.AsyncFunctionDef():
            return [_function(load, module, node)]
        case ast.ClassDef():
            return [_with(node, body=[*_docstring(node), *_body(load, module, node.body)])]
        case ast.If():
            return _if(load, module, node)
        case ast.Try() | ast.TryStar():
            return _try(load, module, node)
        case _:
            return []


def _annotated(load: Load, module: Module, node: ast.AnnAssign) -> ast.AnnAssign:
    """Keep an annotated assignment's annotation, and its value only if it's `Final`."""
    final = node.value is None or is_final(load, node.annotation, module)
    value = node.value if final else ast.Constant(...)
    return _with(node, annotation=_unquoted(load, module, node.annotation), value=value)


def _function(load: Load, module: Module, node: Def) -> Def:
    """Stub a function, without its injected parameters and `inject` if it's injected."""
    found = decorator(load, node, module)
    _warn_missing_return(module, node)
    stub = _with(
        node,
        decorator_list=[expr for expr in node.decorator_list if expr is not found],
        args=_arguments(load, module, node.args, _kept(load, module, node, found)),
        returns=None if node.returns is None else _unquoted(load, module, node.returns),
        body=_docstring(node) or [_ellipsis()],
    )
    if isinstance(stub, ast.AsyncFunctionDef) and _yields(node):
        return _plain(stub)
    return stub


_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
"""The functions whose `yield` is their own, not the function's around them."""


def _yields(node: ast.AST) -> bool:
    """Whether `node` yields, not counting the functions nested in it."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.Yield, ast.YieldFrom)):
            return True
        if not isinstance(child, _SCOPES) and _yields(child):
            return True
    return False


def _plain(stub: ast.AsyncFunctionDef) -> ast.FunctionDef:
    """
    Return an async generator's stub as a plain function, as stubs declare one.

    A stub's body has no `yield`, so an `async def` there declares a coroutine. A checker
    would then read calling it as giving an awaitable of the generator, not the generator.
    """
    plain = ast.FunctionDef(
        name=stub.name,
        args=stub.args,
        body=stub.body,
        decorator_list=stub.decorator_list,
        returns=stub.returns,
        type_comment=stub.type_comment,
        type_params=stub.type_params,
    )
    return ast.copy_location(plain, stub)


def _kept(load: Load, module: Module, node: Def, found: ast.expr | None) -> set[str]:
    """Return the parameters callers pass: all, unless `inject` is `found` on the function."""
    if found is None:
        return {arg.arg for arg in arguments(node)}
    found_verdicts = verdicts(load, node, module)
    _warn_unread(module, node, found_verdicts)
    return {name for name, verdict in found_verdicts.items() if verdict is not True}


def _warn_unread(module: Module, node: Def, found: dict[str, Verdict]) -> None:
    """Warn about each parameter whose annotation comes from a module that couldn't be read."""
    for name, verdict in found.items():
        if isinstance(verdict, Unread):
            _log.warning(
                "%s:%d: couldn't read module %s, so %s's parameter %s is taken as not injected",
                module.path,
                node.lineno,
                verdict.module,
                node.name,
                name,
            )


def _arguments(load: Load, module: Module, args: ast.arguments, kept: set[str]) -> ast.arguments:
    """Return `args` with only the parameters in `kept`, each default still beside its own."""

    def keep(params: list[ast.arg]) -> list[ast.arg]:
        return [_param(load, module, param) for param in params if param.arg in kept]

    def keep_one(param: ast.arg | None) -> ast.arg | None:
        if param is None or param.arg not in kept:
            return None
        return _param(load, module, param)

    positional = [*args.posonlyargs, *args.args]
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    positional_defaults = [
        default
        for param, default in zip(positional, defaults, strict=True)
        if param.arg in kept and default is not None
    ]
    keyword = zip(args.kwonlyargs, args.kw_defaults, strict=True)
    keyword_defaults = [default for param, default in keyword if param.arg in kept]
    return ast.arguments(
        posonlyargs=keep(args.posonlyargs),
        args=keep(args.args),
        vararg=keep_one(args.vararg),
        kwonlyargs=keep(args.kwonlyargs),
        kw_defaults=keyword_defaults,
        kwarg=keep_one(args.kwarg),
        defaults=positional_defaults,
    )


def _param(load: Load, module: Module, param: ast.arg) -> ast.arg:
    """Return `param` with its annotation unquoted."""
    if param.annotation is None:
        return param
    return _with(param, annotation=_unquoted(load, module, param.annotation))


def _warn_missing_return(module: Module, node: Def) -> None:
    """Warn about a public function without a return annotation, which importers see as Unknown."""
    public = not node.name.startswith("_") or (node.name.endswith("__") and node.name != "__init__")
    if node.returns is None and public:
        _log.warning(
            "%s:%d: %s has no return annotation, so the stub's importers see it returning Unknown",
            module.path,
            node.lineno,
            node.name,
        )


def _docstring(node: Def | ast.ClassDef) -> list[ast.stmt]:
    """Return `node`'s docstring, kept so editors reading the stub still show it, or nothing."""
    match node.body[0]:
        case ast.Expr(value=ast.Constant(value=str())) as docstring:
            return [docstring]
        case _:
            return []


def _if(load: Load, module: Module, node: ast.If) -> list[ast.stmt]:
    """Stub an `if`, whose `elif` is an `if` in its `else`, or nothing if neither holds a stub."""
    body, orelse = _body(load, module, node.body), _body(load, module, node.orelse)
    if not body and not orelse:
        return []
    return [_with(node, body=body, orelse=orelse)]


def _try(load: Load, module: Module, node: ast.Try | ast.TryStar) -> list[ast.stmt]:
    """
    Stub a `try` block by what its body binds, since a stub can't hold a `try`.

    What only its other clauses bind is kept too. A name its other clauses bind again keeps
    the body's type, and is warned about.
    """
    bound = _bound(node.body)
    others = [statement for clause in clauses(node)[1:] for statement in clause]
    _warn_rebound(module, node, sorted(_bound(others) & bound))
    kept = [statement for statement in others if not binds(statement) & bound]
    return _body(load, module, [*node.body, *kept])


def _warn_rebound(module: Module, node: ast.Try | ast.TryStar, names: list[str]) -> None:
    """Warn about each name a `try` block binds again outside its body."""
    for name in names:
        _log.warning(
            "%s:%d: %s is bound again outside its try block's body, which the stub can't say, "
            "so it keeps the body's type",
            module.path,
            node.lineno,
            name,
        )


def _bound(statements: Iterable[ast.stmt]) -> set[str]:
    return {name for statement in statements for name in binds(statement)}


class _Unquote(ast.NodeTransformer):
    """
    Unquote the forward references in an annotation, since a stub needs none.

    Strings in `Literal[...]` are values, and `Annotated[...]`'s metadata isn't a type, so
    both are left as they are.
    """

    def __init__(self, load: Load, module: Module) -> None:
        self._load = load
        self._module = module

    def visit_Constant(self, node: ast.Constant) -> ast.AST:
        """Parse a quoted type into the expression it names, and unquote inside it too."""
        if not isinstance(node.value, str):
            return node
        return self.visit(ast.parse(node.value, mode="eval").body)

    def visit_Subscript(self, node: ast.Subscript) -> ast.AST:
        """Leave `Literal`'s values and `Annotated`'s metadata alone."""
        if refers_to(self._load, node.value, self._module, LITERAL):
            return node
        if refers_to(self._load, node.value, self._module, ANNOTATED) and isinstance(
            node.slice, ast.Tuple
        ):
            first, *metadata = node.slice.elts
            node.slice.elts = [self.visit(first), *metadata]
            return node
        return self.generic_visit(node)


def _unquoted(load: Load, module: Module, annotation: ast.expr) -> ast.expr:
    """Return a copy of `annotation` with its forward references unquoted."""
    tree = ast.Expression(copy.deepcopy(annotation))
    _Unquote(load, module).visit(tree)
    return tree.body


# Pruning the imports nothing uses, and filling the bodies left empty.


def _needed(body: list[ast.stmt]) -> frozenset[str]:
    """Return every name a stub reads, or lists in `__all__`."""
    tree = ast.Module(body, type_ignores=[])
    read = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }
    return frozenset(read | _exported(tree))


def _exported(tree: ast.Module) -> set[str]:
    """Return the strings assigned or added to `__all__`, anywhere in `tree`."""
    exported: set[str] = set()
    for node in ast.walk(tree):
        value = _all_value(node)
        if value is not None:
            exported |= _strings(value)
    return exported


def _all_value(node: ast.AST) -> ast.expr | None:
    """Return what `node` assigns or adds to `__all__`, if it's an assignment to it."""
    match node:
        case ast.Assign(targets=targets, value=value) if any(map(_is_all, targets)):
            return value
        case ast.AugAssign(target=target, value=value) if _is_all(target):
            return value
        case _:
            return None


def _is_all(target: ast.expr) -> bool:
    return isinstance(target, ast.Name) and target.id == "__all__"


def _strings(expr: ast.expr) -> set[str]:
    """Return the string constants in `expr`."""
    return {
        node.value
        for node in ast.walk(expr)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def _prune(statements: list[ast.stmt], needed: frozenset[str]) -> list[ast.stmt]:
    """Drop unneeded imports and `if` blocks left empty, and fill each empty body with `...`."""
    return [kept for statement in statements for kept in _pruned(statement, needed)]


def _pruned(statement: ast.stmt, needed: frozenset[str]) -> list[ast.stmt]:
    match statement:
        case ast.Import() | ast.ImportFrom():
            return _pruned_import(statement, needed)
        case ast.ClassDef():
            return [_with(statement, body=_filled(_prune(statement.body, needed)))]
        case ast.If():
            return _pruned_if(statement, needed)
        case _:
            return [statement]


def _pruned_import(node: ast.Import | ast.ImportFrom, needed: frozenset[str]) -> list[ast.stmt]:
    """Keep the names of an import it needs, or drop it if it needs none."""
    names = [alias for alias in node.names if _needs(node, alias, needed)]
    if not names:
        return []
    return [_with(node, names=names)]


def _pruned_if(node: ast.If, needed: frozenset[str]) -> list[ast.stmt]:
    """Prune an `if`'s clauses, or drop it if neither holds anything."""
    body, orelse = _prune(node.body, needed), _prune(node.orelse, needed)
    if not body and not orelse:
        return []
    return [_with(node, body=_filled(body), orelse=orelse)]


def _filled(body: list[ast.stmt]) -> list[ast.stmt]:
    """Return `body`, or `...` if it's empty, since a body can't be."""
    return body or [_ellipsis()]


def _needs(node: ast.Import | ast.ImportFrom, alias: ast.alias, needed: frozenset[str]) -> bool:
    """Whether to keep a name an import binds: a star, an `x as x` re-export, or one needed."""
    if alias.name == "*" or alias.asname == alias.name:
        return True
    return bool(binds(_with(node, names=[alias])) & needed)
