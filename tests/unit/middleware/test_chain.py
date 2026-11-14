"""
Tests for composing middleware around a call.

This test suite verifies the following behaviors:

chain:
- Runs middleware outermost first, each passing the call on to the next, and returns what the
  outermost returns.
- The innermost call gets the call value the last middleware passed on, and the context the
  chain was run with, which no middleware sees.
- A middleware can refuse the call by raising before next, skip it by returning without next,
  retry it by calling next again, and replace its reply or catch its error.
- An error a middleware raises after next replaces the call's reply.
- With async middleware, each awaits next, and asyncio.timeout turns a slow call into
  TimeoutError.
- With no middleware, the chain is the innermost call.

refuse_kind:
- A sync owner refuses an async def middleware, and an async owner anything but one, naming
  the owner, the middleware and the kind it takes.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from stratae.middleware import MiddlewareKindError, chain, refuse_kind

type _Middleware = Callable[[Any, Callable[[Any], Any]], Any]


def _finish(call: Any, context: Any) -> tuple[Any, Any]:
    return call, context


def _tagging(tag: str, log: list[str]) -> _Middleware:
    def middleware(call: Any, next: Callable[[Any], Any]) -> Any:
        log.append(f"{tag} in")
        reply = next(f"{call}+{tag}")
        log.append(f"{tag} out")
        return reply

    return middleware


def test_runs_outermost_first_passing_the_call_on():
    """
    A chain should run its middleware outermost first, each passing the call on.

    Given: Two middleware, each adding a tag to the call before passing it on
    When: The chain runs a call with a context
    Then: They run outer, inner, inner out, outer out, and the innermost call gets the call
        with both tags and the context
    """
    log: list[str] = []
    chained = chain((_tagging("a", log), _tagging("b", log)), _finish)

    reply = chained("call", "context")

    assert log == ["a in", "b in", "b out", "a out"]
    assert reply == ("call+a+b", "context")


def test_no_middleware_is_the_innermost_call():
    """
    A chain of no middleware should be the innermost call itself.

    Given: No middleware
    When: The chain is built
    Then: It's the innermost call
    """
    assert chain((), _finish) is _finish


def test_middleware_decides_the_call():
    """
    A middleware should be able to refuse, skip, retry, or replace the call's reply.

    Given: Middleware that refuses, skips, retries and replaces the reply
    When: Each wraps a call that fails once, then succeeds
    Then: Refusing raises its error, skipping returns its own reply without the call,
        retrying returns the second attempt, and replacing returns the new reply
    """
    attempts: list[int] = []

    def flaky(call: Any, context: Any) -> str:
        attempts.append(1)
        if len(attempts) % 2:
            raise ConnectionError("blip")
        return "ok"

    def refuse(call: Any, next: Callable[[Any], Any]) -> Any:
        raise PermissionError("no")

    def skip(call: Any, next: Callable[[Any], Any]) -> Any:
        return "cached"

    def retry(call: Any, next: Callable[[Any], Any]) -> Any:
        try:
            return next(call)
        except ConnectionError:
            return next(call)

    def shout(call: Any, next: Callable[[Any], Any]) -> Any:
        return next(call).upper()

    with pytest.raises(PermissionError):
        chain((refuse,), flaky)("call", None)
    assert chain((skip,), flaky)("call", None) == "cached"
    assert attempts == []
    assert chain((shout, retry), flaky)("call", None) == "OK"
    assert len(attempts) == 2


def test_error_after_next_replaces_the_reply():
    """
    An error a middleware raises after next should replace the call's reply.

    Given: A middleware that raises after next returns
    When: The chain runs
    Then: Its error is raised, and the outer middleware sees it
    """
    seen: list[BaseException] = []

    def outer(call: Any, next: Callable[[Any], Any]) -> Any:
        try:
            return next(call)
        except LookupError as error:
            seen.append(error)
            raise

    def failing(call: Any, next: Callable[[Any], Any]) -> Any:
        next(call)
        raise LookupError("after")

    with pytest.raises(LookupError, match="after"):
        chain((outer, failing), _finish)("call", None)
    assert len(seen) == 1


async def test_async_middleware_awaits_next():
    """
    Async middleware should await next, and see the awaited reply.

    Given: An async middleware recording the reply, around an async call
    When: The chain is awaited
    Then: It returns the call's reply, which the middleware saw
    """
    seen: list[Any] = []

    async def finish(call: Any, context: Any) -> str:
        await asyncio.sleep(0)
        return f"{call} done"

    async def record(call: Any, next: Callable[[Any], Awaitable[Any]]) -> Any:
        reply = await next(call)
        seen.append(reply)
        return reply

    assert await chain((record,), finish)("call", None) == "call done"
    assert seen == ["call done"]


async def test_timeout_middleware_turns_a_slow_call_into_timeout_error():
    """
    asyncio.timeout should work as a middleware.

    Given: A middleware giving each call a short timeout
    When: A slow call and a fast one run through it
    Then: The slow one raises TimeoutError, and the fast one replies
    """

    async def timeout(call: Any, next: Callable[[Any], Awaitable[Any]]) -> Any:
        async with asyncio.timeout(0.01):
            return await next(call)

    async def finish(call: float, context: Any) -> str:
        await asyncio.sleep(call)
        return "done"

    chained = chain((timeout,), finish)

    with pytest.raises(TimeoutError):
        await chained(1.0, None)
    assert await chained(0, None) == "done"


def _sync(call: Any, next: Callable[[Any], Any]) -> Any:
    return next(call)


async def _async(call: Any, next: Callable[[Any], Awaitable[Any]]) -> Any:
    return await next(call)


@pytest.mark.parametrize(
    ("asynchronous", "middleware", "wanted"),
    [(False, _async, "a sync function"), (True, _sync, "an async def")],
    ids=["sync-owner", "async-owner"],
)
def test_refuse_kind(asynchronous: bool, middleware: Any, wanted: str):
    """
    An owner should refuse middleware of the wrong kind, naming it and the kind it takes.

    Given: A sync owner and an async def middleware, or an async owner and a sync one
    When: The middleware is checked
    Then: MiddlewareKindError names the owner, the middleware and the kind wanted, and the
        right kind passes
    """
    right = _async if asynchronous else _sync
    refuse_kind((right,), "owner", asynchronous)

    with pytest.raises(MiddlewareKindError, match=rf"owner middleware must be {wanted}"):
        refuse_kind((right, middleware), "owner", asynchronous)
