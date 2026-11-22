"""Resolve names in a module's source to what they refer to, following imports."""

import ast
from dataclasses import dataclass

from ._modules import Defined, FromImport, Load, Module, ModuleImport


@dataclass(frozen=True)
class Found:
    """A name defined in a module that was read, by the statement defining it."""

    module: Module
    name: str
    node: ast.stmt


@dataclass(frozen=True)
class Package:
    """A module, by its import name."""

    name: str


@dataclass(frozen=True)
class Unread:
    """A name, dotted if it's an attribute, from a module that couldn't be found or read."""

    module: str
    name: str


type Symbol = Found | Package | Unread
"""What a name refers to. A name no module binds, a builtin or one undefined, has none."""

type Seen = frozenset[tuple[str, str]]
"""The names already followed, by module and name, so cycles end."""


def identity(symbol: Symbol | None) -> tuple[str, str] | None:
    """
    Return the module and name a symbol is defined as, if it's a name in a module.

    An unread dotted name is split at its last dot, as a module path.
    """
    match symbol:
        case Found(module=module, name=name):
            return module.name, name
        case Unread(module=module, name=name):
            head, _, tail = f"{module}.{name}".rpartition(".")
            return head, tail
        case _:
            return None


def lookup(load: Load, module: Module, name: str, seen: Seen = frozenset()) -> Symbol | None:
    """Return what `name` refers to in `module`, following its imports."""
    key = (module.name, name)
    if key in seen:
        return None
    seen = seen | {key}
    match module.bindings.get(name):
        case FromImport(module=source, name=original):
            return member(load, source, original, seen)
        case ModuleImport(module=target):
            return Package(target)
        case Defined(node=node):
            return Found(module, name, node)
        case _:
            return _unbound(load, module, name, seen)


def member(load: Load, module: str, name: str, seen: Seen = frozenset()) -> Symbol | None:
    """Return what `name` refers to in the module `module`."""
    found = load(module)
    return Unread(module, name) if found is None else lookup(load, found, name, seen)


def resolve(load: Load, expr: ast.expr, module: Module) -> Symbol | None:
    """Return what a name or dotted name refers to, or `None` for any other expression."""
    match expr:
        case ast.Name(id=name):
            return lookup(load, module, name)
        case ast.Attribute(value=value, attr=attr):
            return _attribute(load, resolve(load, value, module), attr)
        case _:
            return None


def _attribute(load: Load, base: Symbol | None, attr: str) -> Symbol | None:
    """Return what an attribute of `base` refers to, when `base` is a module."""
    match base:
        case Package(name=package):
            return member(load, package, attr)
        case Unread(module=module, name=name):
            return Unread(module, f"{name}.{attr}")
        case _:
            return None


def _unbound(load: Load, module: Module, name: str, seen: Seen) -> Symbol | None:
    """Return what a name `module` doesn't bind refers to: a star import's, or a submodule."""
    for star in module.stars:
        symbol = member(load, star, name, seen)
        if symbol is not None:
            return symbol
    submodule = f"{module.name}.{name}"
    return Package(submodule) if module.is_package and load(submodule) else None
