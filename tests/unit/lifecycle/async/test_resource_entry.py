"""
Test suite for how an async scope cache enters and exits generator functions.

A cached async generator function has its generator driven by the scope, as
`contextlib.asynccontextmanager` would drive it. A decorator that hides a generator function
is seen through when it records what it wraps, as `functools.wraps` does. These tests check
the driving matches contextlib, and the decorated path matches the plain one.
"""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, cast

import pytest

from stratae.lifecycle._stack import enter_async_generator, exit_async_generator
from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.scope import AsyncScope

type Body = Callable[[list[str]], AsyncGenerator[Any, None]]


def _clean() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        yield "value"
        events.append("after")

    return body


def _suppressing() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        try:
            yield "value"
        except Exception as exc:
            events.append(f"caught {type(exc).__name__}")

    return body


def _replacing() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        try:
            yield "value"
        except Exception:
            raise KeyError("replaced") from None

    return body


def _wrapping() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        try:
            yield "value"
        except Exception as exc:
            raise RuntimeError("wrapped") from exc

    return body


def _finally() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        try:
            yield "value"
        finally:
            events.append("finally")

    return body


def _extra_yield() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        yield "value"
        yield "extra"

    return body


def _yield_after_throw() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        try:
            yield "value"
        except BaseException:
            yield "extra"

    return body


def _no_yield() -> Body:
    async def body(events: list[str]):
        await asyncio.sleep(0)
        return
        yield

    return body


BODIES = {
    "clean": _clean,
    "suppressing": _suppressing,
    "replacing": _replacing,
    "wrapping": _wrapping,
    "finally": _finally,
    "extra_yield": _extra_yield,
    "yield_after_throw": _yield_after_throw,
    "no_yield": _no_yield,
}

EXCEPTIONS: dict[str, Callable[[], BaseException] | None] = {
    "none": None,
    "value_error": lambda: ValueError("boom"),
    "runtime_error": lambda: RuntimeError("boom"),
    "stop_iteration": lambda: StopIteration("stop"),
    "stop_async_iteration": lambda: StopAsyncIteration("stop"),
}


def _passthrough[F: Callable[..., Any]](func: F, events: list[str] | None = None) -> F:
    """Wrap `func` in a `functools.wraps` decorator, recording each call in `events`."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if events is not None:
            events.append("decorator")
        return func(*args, **kwargs)

    return wrapper  # pyright: ignore[reportReturnType]


def _kind(exc: BaseException) -> type[BaseException]:
    """Return an exception's nearest builtin class, as contextlib would have raised it."""
    return next(kind for kind in type(exc).__mro__ if kind.__module__ == "builtins")


def _describe(exc: BaseException) -> tuple[Any, ...]:
    """Describe an exception by kind and message, recursing into a group's exceptions."""
    if isinstance(exc, BaseExceptionGroup):
        group = cast(BaseExceptionGroup[BaseException], exc)
        return (_kind(group), str(group), tuple(_describe(inner) for inner in group.exceptions))
    return (_kind(exc), str(exc))


async def _outcome(run: Callable[[list[str]], Awaitable[Any]]) -> tuple[Any, ...]:
    """Run a scenario, returning what it returned or raised, and the events it recorded."""
    events: list[str] = []
    try:
        result = await run(events)
    except BaseException as exc:
        return ("raised", _describe(exc), events)
    return ("returned", result, events)


@pytest.mark.parametrize("exc_name", EXCEPTIONS)
@pytest.mark.parametrize("body_name", BODIES)
async def test_driven_generator_matches_asynccontextmanager(body_name: str, exc_name: str):
    """
    Test that a scope drives an async generator as `contextlib.asynccontextmanager` would.

    Given: An async generator body, and an exception (or none) raised while its value is used,
    When: It's entered and exited the way a scope does, and separately with
        asynccontextmanager,
    Then: Both should return or raise the same thing and record the same events.
    """
    # Arrange
    body, make_exc = BODIES[body_name](), EXCEPTIONS[exc_name]

    async def driven(events: list[str]) -> str:
        generator = body(events)
        events.append(await enter_async_generator([[]], generator))
        error = None if make_exc is None else make_exc()
        suppressed = await exit_async_generator(generator, error)
        if error is None or suppressed:
            return "done"
        raise error

    async def reference(events: list[str]) -> str:
        async with asynccontextmanager(body)(events) as value:
            events.append(value)
            if make_exc is not None:
                raise make_exc()
        return "done"

    # Act & Assert
    assert await _outcome(driven) == await _outcome(reference)


@pytest.mark.parametrize("exc_name", EXCEPTIONS)
@pytest.mark.parametrize("body_name", BODIES)
async def test_decorated_async_generator_matches_plain(body_name: str, exc_name: str):
    """
    Test that an async generator behind a decorator is cached as it would be without one.

    Given: An async generator body, and an exception (or none) raised in the block using it,
    When: It's cached plainly, and separately behind a sync `functools.wraps` decorator,
    Then: Both should return or raise the same thing and record the same events.
    """
    # Arrange
    make_exc = EXCEPTIONS[exc_name]
    plain, decorated = AsyncScope("plain"), AsyncScope("decorated")
    via_plain = plain.cache(ignore_params=True)(BODIES[body_name]())
    via_decorator = decorated.cache(ignore_params=True)(_passthrough(BODIES[body_name]()))

    def run(
        scope: AsyncScope, cached: Callable[[list[str]], Awaitable[Any]]
    ) -> Callable[[list[str]], Awaitable[str]]:
        async def scenario(events: list[str]) -> str:
            async with scope:
                events.append(await cached(events))
                if make_exc is not None:
                    raise make_exc()
            return "done"

        return scenario

    # Act & Assert
    plain_outcome = await _outcome(run(plain, via_plain))
    assert plain_outcome == await _outcome(run(decorated, via_decorator))


async def test_decorated_async_generator_still_runs_its_decorator():
    """
    Test that an async generator wrapped by another decorator is entered through it.

    Given: An async generator function wrapped by a `functools.wraps` decorator, then cached,
    When: It's called in an active scope,
    Then: The decorator should run, and the resource should still be exited on deactivation.
    """
    # Arrange
    events: list[str] = []
    scope = AsyncScope("request")

    async def generator():
        await asyncio.sleep(0)
        yield "value"
        events.append("exit")

    get_value = scope.cache()(_passthrough(generator, events))

    # Act
    async with scope:
        value = await get_value()

    # Assert
    assert value == "value"
    assert events == ["decorator", "exit"]


@pytest.mark.parametrize("decorate", [False, True], ids=["driven", "decorated"])
async def test_async_entry_finishing_after_deactivation_is_exited(decorate: bool):
    """
    Test that an async resource whose entry finishes after its activation ended is exited.

    Given: An async resource that ends its own scope activation before yielding,
    When: It's called through the scope's cache,
    Then: It should be exited immediately and ScopeInactiveError raised.
    """
    # Arrange
    events: list[str] = []
    scope = AsyncScope("request")
    activation = await scope.activate()

    async def generator():
        await scope.deactivate(activation)
        yield "value"
        events.append("exit")

    cached = scope.cache()(_passthrough(generator) if decorate else generator)

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="activation has ended"):
        await cached()
    assert events == ["exit"]


async def test_async_scope_exits_decorated_sync_generator():
    """
    Test that an async scope enters and exits a sync generator behind a decorator.

    Given: An AsyncScope and a sync generator function wrapped by another decorator,
    When: It's cached and called, then the scope deactivates,
    Then: Its value should be returned without awaiting, and the generator closed.
    """
    # Arrange
    events: list[str] = []
    scope = AsyncScope("request")

    def generator():
        yield "value"
        events.append("exit")

    get_value = scope.cache()(_passthrough(generator))

    # Act
    async with scope:
        value = get_value()

    # Assert
    assert value == "value"
    assert events == ["exit"]


async def test_decorator_hiding_async_generator_returning_another_value_has_it_cached():
    """
    Test that a decorator turning a hidden async generator into another value has it cached.

    Given: An async generator function behind a sync `functools.wraps` decorator that returns
        something else,
    When: It's cached in an AsyncScope and awaited twice,
    Then: The returned value should be cached as it is.
    """

    # Arrange
    async def generator():
        await asyncio.sleep(0)
        yield "value"

    @wraps(generator)
    def summary() -> Any:
        return "summary"

    scope = AsyncScope("request")
    cached = scope.cache()(summary)

    # Act
    async with scope:
        first, second = await cached(), await cached()

    # Assert
    assert first == second == "summary"


async def test_decorator_hiding_async_generator_returning_sync_generator_enters_it():
    """
    Test that a sync generator returned by a decorator hiding an async one is entered.

    Given: An async generator function behind a sync `functools.wraps` decorator that returns
        a sync generator instead,
    When: It's cached in an AsyncScope, awaited, and the scope deactivates,
    Then: The sync generator should be entered, and closed.
    """
    # Arrange
    events: list[str] = []

    async def generator():
        await asyncio.sleep(0)
        yield "value"

    def other():
        yield "other"
        events.append("exit")

    @wraps(generator)
    def swap() -> Any:
        return other()

    scope = AsyncScope("request")
    cached = scope.cache()(swap)

    # Act
    async with scope:
        value = await cached()

    # Assert
    assert value == "other"
    assert events == ["exit"]
