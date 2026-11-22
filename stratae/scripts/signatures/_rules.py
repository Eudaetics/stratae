"""
Decide from the source which parameters `inject` injects, by the rules `inject` follows.

A parameter is injected when its annotation, through plain aliases and at most one `type`
alias, is an `Annotated` whose metadata holds a provider: a `Depends(...)` call, or a name
assigned one. Which definitions are `Annotated`, `Depends`, `inject` and the other forms
read here is the data below.
"""

import ast
from collections.abc import Iterator
from dataclasses import dataclass

from ._modules import Load, Module
from ._names import Found, Seen, Symbol, Unread, identity, resolve

type Def = ast.FunctionDef | ast.AsyncFunctionDef
"""A function definition."""

ANNOTATED = frozenset({("typing", "Annotated"), ("typing_extensions", "Annotated")})
FINAL = frozenset({("typing", "Final"), ("typing_extensions", "Final")})
LITERAL = frozenset({("typing", "Literal"), ("typing_extensions", "Literal")})
DEPENDS = frozenset({("stratae.depends.injection", "Depends"), ("stratae.depends", "Depends")})
INJECT = frozenset({("stratae.depends.injection", "inject"), ("stratae.depends", "inject")})
"""
The definitions each rule recognises, by module and name.

The second of each pair is how a name reads when Stratae's own source can't be found.
"""


@dataclass(frozen=True)
class AnnotatedForm:
    """An `Annotated[...]`, and whether its metadata, nested ones included, holds a provider."""

    provided: bool


@dataclass(frozen=True)
class AliasForm:
    """A `type` alias, whose value an annotation is unwrapped to once."""

    value: ast.expr
    module: Module


type Form = AnnotatedForm | AliasForm | Unread | None
"""What an annotation is, as far as injection goes. `None` is anything else."""

type Verdict = bool | Unread
"""Whether a parameter is injected, or the name that couldn't be read to tell."""


def refers_to(
    load: Load, expr: ast.expr, module: Module, definitions: frozenset[tuple[str, str]]
) -> bool:
    """Whether `expr` names one of `definitions`."""
    return identity(resolve(load, expr, module)) in definitions


def form(load: Load, expr: ast.expr, module: Module, seen: Seen = frozenset()) -> Form:
    """Evaluate `expr` far enough to tell an `Annotated` or a `type` alias from the rest."""
    if isinstance(expr, ast.Subscript):
        return _subscript(load, expr, module, seen)
    symbol = resolve(load, expr, module)
    match symbol:
        case Found(module=home, node=ast.TypeAlias(value=value)):
            return AliasForm(value, home)
        case Unread():
            return symbol
        case _:
            assigned = _assigned(symbol, seen)
            return None if assigned is None else form(load, *assigned)


def _subscript(load: Load, expr: ast.Subscript, module: Module, seen: Seen) -> Form:
    """Evaluate a subscript: an `Annotated[...]`, or a generic `type` alias given arguments."""
    if refers_to(load, expr.value, module, ANNOTATED):
        return _annotated(load, expr.slice, module, seen)
    base = form(load, expr.value, module, seen)
    return base if isinstance(base, (AliasForm, Unread)) else None


def _annotated(load: Load, given: ast.expr, module: Module, seen: Seen) -> AnnotatedForm:
    """Evaluate what `Annotated` is given: whether its metadata, or a nested one's, provides."""
    first, *metadata = given.elts if isinstance(given, ast.Tuple) else [given]
    inner = form(load, first, module, seen)
    nested = isinstance(inner, AnnotatedForm) and inner.provided
    return AnnotatedForm(nested or any(is_provider(load, meta, module) for meta in metadata))


def _assigned(symbol: Symbol | None, seen: Seen) -> tuple[ast.expr, Module, Seen] | None:
    """
    Return the value a name is assigned, where, and the names followed with it.

    A name assigned with an annotation counts. One already followed gives nothing, so cycles
    end.
    """
    match symbol:
        case Found(
            module=home,
            name=name,
            node=ast.Assign(value=value) | ast.AnnAssign(value=ast.expr() as value),
        ) if (home.name, name) not in seen:
            return value, home, seen | {(home.name, name)}
        case _:
            return None


def is_provider(load: Load, expr: ast.expr, module: Module, seen: Seen = frozenset()) -> bool:
    """Whether `expr` is a `Depends(...)` call, or a name assigned one."""
    if isinstance(expr, ast.Call):
        return refers_to(load, expr.func, module, DEPENDS)
    assigned = _assigned(resolve(load, expr, module), seen)
    return assigned is not None and is_provider(load, *assigned)


def injected(load: Load, annotation: ast.expr | None, module: Module) -> Verdict:
    """Whether a parameter annotated with `annotation` is injected."""
    if annotation is None:
        return False
    found = form(load, annotation, module)
    if isinstance(found, AliasForm):
        found = form(load, found.value, found.module)
    match found:
        case AnnotatedForm(provided=provided):
            return provided
        case Unread():
            return found
        case _:
            return False


def decorator(load: Load, node: Def, module: Module) -> ast.expr | None:
    """Return the `inject` decorator on `node`, called or bare, if it has one."""
    for expr in node.decorator_list:
        target = expr.func if isinstance(expr, ast.Call) else expr
        if refers_to(load, target, module, INJECT):
            return expr
    return None


def is_final(load: Load, annotation: ast.expr, module: Module) -> bool:
    """Whether `annotation` is `Final`, bare or subscripted."""
    base = annotation.value if isinstance(annotation, ast.Subscript) else annotation
    return refers_to(load, base, module, FINAL)


def arguments(node: Def) -> Iterator[ast.arg]:
    """Yield every parameter of `node`, in order."""
    args = node.args
    every = [*args.posonlyargs, *args.args, args.vararg, *args.kwonlyargs, args.kwarg]
    yield from (arg for arg in every if arg is not None)


def verdicts(load: Load, node: Def, module: Module) -> dict[str, Verdict]:
    """Return whether each of `node`'s parameters is injected, by name."""
    return {arg.arg: injected(load, arg.annotation, module) for arg in arguments(node)}
