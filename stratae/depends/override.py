"""Override tool for replacing injected values for functions."""

from contextvars import Token
from typing import Any, Callable

from stratae.depends import DependsWrapper


class _Override:
    def __init__(self, dep: DependsWrapper, value: Any):
        self.dep = dep
        self.value = value
        self.token: Token[Any]

    def __enter__(self):
        with self.dep.lock:
            if self.dep.override_count == 0:
                self.dep.provide = self.dep.provide_override
            self.dep.override_count += 1
            self.token = self.dep.override.set(self.value)

    def __exit__(self, *_):
        with self.dep.lock:
            self.dep.override.reset(self.token)
            self.dep.override_count -= 1
            if self.dep.override_count == 0:
                self.dep.provide = self.dep.dependency


def override(func: Callable[..., Any], value: Any):
    """Override a value for a dependency."""
    dep = DependsWrapper.find(func)
    return _Override(dep, value)
