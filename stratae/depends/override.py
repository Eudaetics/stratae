"""
Context managers for temporarily replacing injected dependency values.

`override` swaps a single provider's value and `overrides` swaps several
at once. Both are used as context managers; the previous state is
restored on exit. Overrides are context-local, so concurrent tasks can
hold different overrides for the same provider without interfering.
"""

from contextvars import Token
from typing import Any, Callable

from stratae.depends._provide import Provider

_OverrideMap = dict[Callable[..., Any], Any]


class _ReusableAwaitable:
    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __await__(self):
        if False:
            yield
        return self.value


class _Override:
    __slots__ = ("dep", "value", "token")

    def __init__(self, dep: Provider, value: Any):
        self.dep = dep
        self.value = value
        self.token: Token[Any]

    def __enter__(self):
        with self.dep.lock:
            if self.dep.override_count == 0:
                self.dep.provide = self.dep.provide_override
            self.dep.override_count += 1
            self.token = self.dep.override.set(
                _ReusableAwaitable(self.value) if self.dep.is_async else self.value
            )

    def __exit__(self, *_: object) -> None:
        with self.dep.lock:
            self.dep.override.reset(self.token)
            self.dep.override_count -= 1
            if self.dep.override_count == 0:
                self.dep.provide = self.dep.dependency


class _Overrides:
    __slots__ = ("_items",)

    def __init__(self, mapping: _OverrideMap) -> None:
        self._items: list[_Override] = [
            _Override(Provider.find(func), value) for func, value in mapping.items()
        ]

    def __enter__(self) -> None:
        entered: list[_Override] = []
        try:
            for o in self._items:
                o.__enter__()
                entered.append(o)
        except BaseException:
            for o in reversed(entered):
                o.__exit__()
            raise

    def __exit__(self, *_: object) -> None:
        for o in self._items:
            o.__exit__()


def override(func: Callable[..., Any], value: Any) -> _Override:
    """
    Temporarily replace a single dependency's value within a scope.

    Args:
        func: The provider callable that was passed to `Depends`.
        value: Value injected in place of calling the provider. Used
            as-is; it is not called, even if the provider is async.

    Returns:
        A context manager holding the override between entry and exit.

    Raises:
        DependencyNotFoundError: If `func` was never passed to `Depends`.

    """
    return _Override(Provider.find(func), value)


def overrides(mapping: _OverrideMap) -> _Overrides:
    """
    Temporarily replace several dependencies' values within one scope.

    Args:
        mapping: Map of provider callables, as passed to `Depends`, to
            their replacement values.

    Returns:
        A context manager applying every override on entry and restoring
        the previous state on exit. If applying one override fails, those
        already applied are unwound before the error propagates.

    Raises:
        ValueError: If `mapping` is empty.
        DependencyNotFoundError: If any key was never passed to `Depends`.

    """
    if not mapping:
        raise ValueError("overrides() requires at least one entry.")
    return _Overrides(mapping)
