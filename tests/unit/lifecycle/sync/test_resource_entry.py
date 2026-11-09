"""
Test suite for how a scope cache enters and exits sync generator functions.

A cached generator function has its generator driven by the scope, as
`contextlib.contextmanager` would drive it. A decorator that hides a generator function is
seen through when it records what it wraps, as `functools.wraps` does. These tests check
the driving matches contextlib, and the decorated path matches the plain one.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import contextmanager
from functools import wraps
from typing import Any, cast

import pytest

from stratae.lifecycle._stack import enter_generator, exit_generator
from stratae.lifecycle.exceptions import AsyncGeneratorInSyncScopeError, ScopeInactiveError
from stratae.lifecycle.scope import AsyncScope, Scope

type Body = Callable[[list[str]], Generator[Any, None, None]]


def _clean() -> Body:
    def body(events: list[str]):
        yield "value"
        events.append("after")

    return body


def _suppressing() -> Body:
    def body(events: list[str]):
        try:
            yield "value"
        except Exception as exc:
            events.append(f"caught {type(exc).__name__}")

    return body


def _replacing() -> Body:
    def body(events: list[str]):
        try:
            yield "value"
        except Exception:
            raise KeyError("replaced") from None

    return body


def _wrapping() -> Body:
    def body(events: list[str]):
        try:
            yield "value"
        except Exception as exc:
            raise RuntimeError("wrapped") from exc

    return body


def _finally() -> Body:
    def body(events: list[str]):
        try:
            yield "value"
        finally:
            events.append("finally")

    return body


def _extra_yield() -> Body:
    def body(events: list[str]):
        yield "value"
        yield "extra"

    return body


def _yield_after_throw() -> Body:
    def body(events: list[str]):
        try:
            yield "value"
        except BaseException:
            yield "extra"

    return body


def _no_yield() -> Body:
    def body(events: list[str]):
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


def _outcome(run: Callable[[list[str]], Any]) -> tuple[Any, ...]:
    """Run a scenario, returning what it returned or raised, and the events it recorded."""
    events: list[str] = []
    try:
        result = run(events)
    except BaseException as exc:
        return ("raised", _describe(exc), events)
    return ("returned", result, events)


@pytest.mark.parametrize("exc_name", EXCEPTIONS)
@pytest.mark.parametrize("body_name", BODIES)
def test_driven_generator_matches_contextmanager(body_name: str, exc_name: str):
    """
    Test that a scope drives a generator as `contextlib.contextmanager` would.

    Given: A generator body, and an exception (or none) raised while its value is in use,
    When: It's entered and exited the way a scope does, and separately with contextmanager,
    Then: Both should return or raise the same thing and record the same events.
    """
    # Arrange
    body, make_exc = BODIES[body_name](), EXCEPTIONS[exc_name]

    def driven(events: list[str]) -> str:
        generator = body(events)
        events.append(enter_generator([[]], generator))
        error = None if make_exc is None else make_exc()
        suppressed = exit_generator(generator, error)
        if error is None or suppressed:
            return "done"
        raise error

    def reference(events: list[str]) -> str:
        with contextmanager(body)(events) as value:
            events.append(value)
            if make_exc is not None:
                raise make_exc()
        return "done"

    # Act & Assert
    assert _outcome(driven) == _outcome(reference)


@pytest.mark.parametrize("exc_name", EXCEPTIONS)
@pytest.mark.parametrize("body_name", BODIES)
def test_decorated_generator_matches_plain(body_name: str, exc_name: str):
    """
    Test that a generator behind a decorator is cached as it would be without one.

    Given: A generator body, and an exception (or none) raised in the block using it,
    When: It's cached plainly, and separately behind a `functools.wraps` decorator,
    Then: Both should return or raise the same thing and record the same events.
    """
    # Arrange
    make_exc = EXCEPTIONS[exc_name]
    plain, decorated = Scope("plain"), Scope("decorated")
    via_plain = plain.cache(ignore_params=True)(BODIES[body_name]())
    via_decorator = decorated.cache(ignore_params=True)(_passthrough(BODIES[body_name]()))

    def run(scope: Scope, cached: Callable[[list[str]], Any]) -> Callable[[list[str]], str]:
        def scenario(events: list[str]) -> str:
            with scope:
                events.append(cached(events))
                if make_exc is not None:
                    raise make_exc()
            return "done"

        return scenario

    # Act & Assert
    assert _outcome(run(plain, via_plain)) == _outcome(run(decorated, via_decorator))


def test_decorated_generator_still_runs_its_decorator():
    """
    Test that a generator wrapped by another decorator is entered through that decorator.

    Given: A generator function wrapped by a `functools.wraps` decorator, then cached,
    When: It's called in an active scope,
    Then: The decorator should run, and the resource should still be exited on deactivation.
    """
    # Arrange
    events: list[str] = []
    scope = Scope("request")

    def generator():
        yield "value"
        events.append("exit")

    get_value = scope.cache()(_passthrough(generator, events))

    # Act
    with scope:
        value = get_value()

    # Assert
    assert value == "value"
    assert events == ["decorator", "exit"]


@pytest.mark.parametrize("decorate", [False, True], ids=["driven", "decorated"])
def test_entry_finishing_after_deactivation_is_exited(decorate: bool):
    """
    Test that a resource whose entry finishes after its activation ended is exited at once.

    Given: A resource that ends its own scope activation before yielding,
    When: It's called through the scope's cache,
    Then: It should be exited immediately and ScopeInactiveError raised.
    """
    # Arrange
    events: list[str] = []
    scope = Scope("request")
    activation = scope.activate()

    def generator():
        scope.deactivate(activation)
        yield "value"
        events.append("exit")

    cached = scope.cache()(_passthrough(generator) if decorate else generator)

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="activation has ended"):
        cached()
    assert events == ["exit"]


def test_decorator_consuming_the_generator_has_its_value_cached():
    """
    Test that a decorator turning a generator into another value has that value cached.

    Given: A generator function behind a `functools.wraps` decorator that collects it,
    When: It's cached and called twice in an active scope,
    Then: The collected list should be cached, not entered.
    """
    # Arrange
    scope = Scope("request")
    runs: list[int] = []

    def generator():
        runs.append(1)
        yield 1
        yield 2

    @wraps(generator)
    def collected() -> list[int]:
        return list(generator())

    cached = scope.cache()(collected)

    # Act
    with scope:
        first, second = cached(), cached()

    # Assert
    assert first == second == [1, 2]
    assert first is second
    assert runs == [1]


def test_decorator_without_wraps_is_cached_as_its_return_value():
    """
    Test that a decorator not recording what it wraps hides its generator from the cache.

    Given: A generator function behind a decorator that doesn't use `functools.wraps`,
    When: It's cached and called in an active scope,
    Then: The generator object should be cached as it is, not entered.
    """

    # Arrange
    def generator():
        yield "value"

    def hide() -> Generator[str, None, None]:
        return generator()

    scope = Scope("request")
    cached = scope.cache()(hide)

    # Act
    with scope:
        value = cached()

    # Assert
    assert isinstance(value, Generator)


@pytest.mark.parametrize("decorate", [False, True], ids=["plain", "decorated"])
def test_sync_scope_refuses_async_generator_function(decorate: bool):
    """
    Test that a sync scope refuses an async generator function when it's decorated.

    Given: An async generator function, plain or behind a `functools.wraps` decorator,
    When: It's cached in a sync scope,
    Then: AsyncGeneratorInSyncScopeError should be raised, and an async scope can still
        cache it.
    """

    # Arrange
    async def generator() -> AsyncGenerator[str, None]:
        await asyncio.sleep(0)
        yield "value"

    func = _passthrough(generator) if decorate else generator

    # Act & Assert
    with pytest.raises(AsyncGeneratorInSyncScopeError, match="Cache it in an AsyncScope"):
        Scope("request").cache()(func)
    AsyncScope("request").cache()(func)


def test_sync_scope_refuses_returned_async_generator():
    """
    Test that a sync scope refuses an async generator returned behind a decorator.

    Given: A decorator over a generator function that returns an async generator instead,
    When: It's cached in a sync scope and called,
    Then: AsyncGeneratorInSyncScopeError should be raised.
    """

    # Arrange
    def generator():
        yield "value"

    async def other() -> AsyncGenerator[str, None]:
        await asyncio.sleep(0)
        yield "other"

    @wraps(generator)
    def swap() -> Any:
        return other()

    scope = Scope("request")
    cached = scope.cache()(swap)

    # Act & Assert
    with scope, pytest.raises(AsyncGeneratorInSyncScopeError):
        cached()
