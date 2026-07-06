"""Parameter parsing for function signature creation."""

from inspect import signature
from typing import Annotated, Any, Callable


def foo(func: Callable[..., Any]):
    """Test signature parsing."""
    sig = signature(func).parameters
    print(sig)


def _bar(i: Annotated[int, "something"], /, j: int, k: int = 1):
    print(i + j + k)


foo(_bar)
