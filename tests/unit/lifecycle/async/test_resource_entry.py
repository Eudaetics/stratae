"""
Test suite for how an async scope cache enters and exits async resources.

A function returned directly by `async_resource` has its generator driven by the scope,
skipping `contextlib.asynccontextmanager`. A resource wrapped by another decorator is still
entered through its context manager. These tests check the two behave the same.
"""

from functools import wraps
from typing import Any, AsyncGenerator, Awaitable, Callable, cast

import pytest

from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.resource import async_resource, resource
from stratae.lifecycle.scope import AsyncScope

type Body = Callable[[list[str]], AsyncGenerator[Any, None]]


def _clean() -> Body:
    async def body(events: list[str]):
        yield "value"
        events.append("after")

    return body


def _suppressing() -> Body:
    async def body(events: list[str]):
        try:
            yield "value"
        except Exception as exc:
            events.append(f"caught {type(exc).__name__}")

    return body


def _replacing() -> Body:
    async def body(events: list[str]):
        try:
            yield "value"
        except Exception:
            raise KeyError("replaced") from None

    return body


def _wrapping() -> Body:
    async def body(events: list[str]):
        try:
            yield "value"
        except Exception as exc:
            raise RuntimeError("wrapped") from exc

    return body


def _finally() -> Body:
    async def body(events: list[str]):
        try:
            yield "value"
        finally:
            events.append("finally")

    return body


def _extra_yield() -> Body:
    async def body(events: list[str]):
        yield "value"
        yield "extra"

    return body


def _yield_after_throw() -> Body:
    async def body(events: list[str]):
        try:
            yield "value"
        except BaseException:
            yield "extra"

    return body


def _no_yield() -> Body:
    async def body(events: list[str]):
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


def _describe(exc: BaseException) -> tuple[Any, ...]:
    """Describe an exception by type and message, recursing into a group's exceptions."""
    if isinstance(exc, BaseExceptionGroup):
        group = cast(BaseExceptionGroup[BaseException], exc)
        return (type(group), str(group), tuple(_describe(inner) for inner in group.exceptions))
    return (type(exc), str(exc))


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
async def test_driven_resource_matches_asynccontextmanager(body_name: str, exc_name: str):
    """
    Test that a driven async resource generator behaves as its context manager would.

    Given: An async generator body, and an exception (or none) raised in the block using it,
    When: It's cached as a resource, once driven directly and once through a decorator that
        makes the scope enter its context manager,
    Then: Both should return or raise the same thing and record the same events.
    """
    # Arrange
    make_exc = EXCEPTIONS[exc_name]
    driven, decorated = AsyncScope("driven"), AsyncScope("decorated")
    via_generator = driven.cache(ignore_params=True)(async_resource(BODIES[body_name]()))
    via_contextmanager = decorated.cache(ignore_params=True)(
        _passthrough(async_resource(BODIES[body_name]()))
    )

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
    driven_outcome = await _outcome(run(driven, via_generator))
    assert driven_outcome == await _outcome(run(decorated, via_contextmanager))


async def test_decorated_async_resource_still_runs_its_decorator():
    """
    Test that an async resource wrapped by another decorator is entered through it.

    Given: An async resource wrapped by a `functools.wraps` decorator, then cached,
    When: It's called in an active scope,
    Then: The decorator should run, and the resource should still be exited on deactivation.
    """
    # Arrange
    events: list[str] = []
    scope = AsyncScope("request")

    async def generator():
        yield "value"
        events.append("exit")

    get_value = scope.cache()(_passthrough(async_resource(generator), events))

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
    activation = scope.activate()

    async def generator():
        await scope.deactivate(activation)
        yield "value"
        events.append("exit")

    func = async_resource(generator)
    cached = scope.cache()(_passthrough(func) if decorate else func)

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="activation has ended"):
        await cached()
    assert events == ["exit"]


async def test_async_scope_exits_decorated_sync_resource():
    """
    Test that an async scope exits a sync resource entered through its context manager.

    Given: An AsyncScope and a sync resource wrapped by another decorator,
    When: It's cached and called, then the scope deactivates,
    Then: Its context manager should be exited.
    """
    # Arrange
    events: list[str] = []
    scope = AsyncScope("request")

    def generator():
        yield "value"
        events.append("exit")

    get_value = scope.cache()(_passthrough(resource(generator)))

    # Act
    async with scope:
        value = get_value()

    # Assert
    assert value == "value"
    assert events == ["exit"]
