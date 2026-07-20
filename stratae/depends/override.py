"""Override tool for replacing injected values for functions."""

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
    """Override a single dependency value."""
    return _Override(Provider.find(func), value)


def overrides(mapping: _OverrideMap) -> _Overrides:
    """Override multiple dependency values simultaneously."""
    if not mapping:
        raise ValueError("overrides() requires at least one entry.")
    return _Overrides(mapping)
