"""
Tests for call_result: what calling something gives.

This test suite verifies the following behaviors:

- Functions, partials of them, and objects whose class's `__call__` is one give what that
  function gives: a value, a coroutine, a generator or an async generator.
- Functions marked with `inspect.markcoroutinefunction` give a coroutine, even a generator
  function.
- A class gives a value, since calling it builds an instance.
- A `functools.wraps` decorator over a function giving something else is hidden, with what
  the hidden function gives. One over a function giving a value isn't hidden.
- A decorator that's itself async isn't hidden: it gives a coroutine.
- Only coroutines and async generators are awaited.
"""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from functools import partial, wraps
from inspect import markcoroutinefunction
from typing import Any

import pytest

from stratae.callables import CallResult, Gives, call_result


def _value() -> int:
    return 1


async def _coroutine() -> int:
    await asyncio.sleep(0)
    return 1


def _generator() -> Generator[int]:
    yield 1


async def _async_generator() -> AsyncGenerator[int]:
    await asyncio.sleep(0)
    yield 1


def _marked() -> Awaitable[int]:
    return _coroutine()


markcoroutinefunction(_marked)


def _marked_generator() -> Generator[int]:
    yield 1


markcoroutinefunction(_marked_generator)


class _Calls:
    def __init__(self, fn: Callable[..., Any]) -> None:
        self.fn = fn


class _ValueCall:
    def __call__(self) -> int:
        return 1


class _CoroutineCall:
    async def __call__(self) -> int:
        return 1


class _GeneratorCall:
    def __call__(self) -> Generator[int]:
        yield 1


class _AsyncGeneratorCall:
    async def __call__(self) -> AsyncGenerator[int]:
        yield 1


def _passthrough(fn: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    return wrapper


def _async_passthrough(fn: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(0)
        return fn(*args, **kwargs)

    return wrapper


_KNOWN = [
    (_value, Gives.VALUE),
    (_coroutine, Gives.COROUTINE),
    (_generator, Gives.GENERATOR),
    (_async_generator, Gives.ASYNC_GENERATOR),
    (partial(partial(_generator)), Gives.GENERATOR),
    (partial(_async_generator), Gives.ASYNC_GENERATOR),
    (_marked, Gives.COROUTINE),
    (_marked_generator, Gives.COROUTINE),
    (_ValueCall(), Gives.VALUE),
    (_CoroutineCall(), Gives.COROUTINE),
    (_GeneratorCall(), Gives.GENERATOR),
    (_AsyncGeneratorCall(), Gives.ASYNC_GENERATOR),
    (_Calls, Gives.VALUE),
    (_passthrough(_value), Gives.VALUE),
    (_async_passthrough(_generator), Gives.COROUTINE),
]


@pytest.mark.parametrize(("fn", "gives"), _KNOWN, ids=lambda x: getattr(x, "__qualname__", ""))
def test_call_result_known(fn: Callable[..., Any], gives: Gives):
    """
    What calling a callable gives should be read from it, its partials, or its class's call.

    Given: A callable that isn't hiding anything behind a decorator
    When: call_result inspects it
    Then: It gives what its function gives, and isn't hidden
    """
    assert call_result(fn) == CallResult(gives, hidden=False)


@pytest.mark.parametrize(
    ("inner", "gives"),
    [
        (_coroutine, Gives.COROUTINE),
        (_generator, Gives.GENERATOR),
        (_async_generator, Gives.ASYNC_GENERATOR),
    ],
    ids=["coroutine", "generator", "async-generator"],
)
def test_call_result_hidden(inner: Callable[..., Any], gives: Gives):
    """
    A decorator hiding a function that gives something else should be marked hidden.

    Given: A sync `functools.wraps` decorator over a function giving something other than a
        value, and a partial of it
    When: call_result inspects them
    Then: Each gives what the hidden function gives, marked hidden
    """
    decorated = _passthrough(inner)

    assert call_result(decorated) == CallResult(gives, hidden=True)
    assert call_result(partial(decorated)) == CallResult(gives, hidden=True)


def test_only_coroutines_and_async_generators_are_awaited():
    """
    Only what needs an event loop should be awaited.

    Given: Each kind of thing calling can give
    When: Asked whether it's awaited
    Then: Coroutines and async generators are, values and generators aren't
    """
    assert {gives for gives in Gives if gives.awaits} == {Gives.COROUTINE, Gives.ASYNC_GENERATOR}
