"""
Snapshot tests for the code that `inject` wrappers generate.

Each case injects one kind of function, and checks the wrapper's generated source against
`snapshots/<case>.txt`. Together the cases run every branch of the generator, so any change
to the generated code changes a snapshot. Record changed snapshots with
`STRATAE_UPDATE_SNAPSHOTS=1`, and review them like code.

This test suite verifies the following behaviors:

Generated source:
- Each case's wrapper source matches its snapshot. The cases cover sync functions with every
  kind of parameter, generator functions, async functions with and without async
  dependencies, async generator functions, and classes.

Tracebacks:
- A traceback through an `inject` wrapper shows the wrapper's source line.
"""

import asyncio
import inspect
import traceback
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Annotated, Any

import pytest

from stratae.depends import Depends, inject


def _get_value() -> int:
    return 1


async def _get_async_value() -> int:
    await asyncio.sleep(0)
    return 2


type _Value = Annotated[int, Depends(_get_value)]
type _AsyncValue = Annotated[int, Depends(_get_async_value)]


def _every_kind(a: int, /, b: int, value: _Value, *args: int, c: int, d: _Value, **kw: int) -> int:
    return a


def _generator(value: _Value) -> Generator[int]:
    yield value


async def _async_with_async_dep(x: int, value: _AsyncValue) -> int:
    await asyncio.sleep(0)
    return x


async def _async_with_sync_dep(value: _Value) -> int:
    await asyncio.sleep(0)
    return value


async def _async_generator(value: _AsyncValue) -> AsyncGenerator[int]:
    await asyncio.sleep(0)
    yield value


class _Service:
    def __init__(self, value: _Value, name: str = "service") -> None:
        self.value = value


_CASES: list[tuple[str, Callable[..., Any]]] = [
    ("sync-every-parameter-kind", _every_kind),
    ("generator", _generator),
    ("async-with-async-dependency", _async_with_async_dep),
    ("async-with-sync-dependency", _async_with_sync_dep),
    ("async-generator", _async_generator),
    ("class", _Service),
]


@pytest.mark.parametrize(("name", "fn"), _CASES, ids=[case[0] for case in _CASES])
def test_generated_source_matches_snapshot(
    snapshot: Callable[[str, str], None], name: str, fn: Callable[..., Any]
):
    """
    An `inject` wrapper's generated source should match its snapshot.

    Given: The case's function, with injected parameters
    When: It's decorated with `inject`
    Then: The wrapper's generated source matches the case's snapshot
    """
    # Act
    wrapper = inject(fn)

    # Assert
    snapshot(name, inspect.getsource(wrapper.__code__))


def test_traceback_shows_inject_wrapper_source():
    """
    A traceback through an `inject` wrapper should show the wrapper's source line.

    Given: An injected function that raises
    When: It's called and the traceback formatted
    Then: The traceback names the function and shows the wrapper's call to it
    """

    # Arrange
    @inject
    def failing(value: _Value) -> int:
        raise ValueError("failed")

    # Act
    with pytest.raises(ValueError, match="failed") as raised:
        failing()

    # Assert
    formatted = "".join(traceback.format_exception(raised.value))
    assert "failing#" in formatted
    assert "return __func__(__dep_value__.provide())" in formatted
