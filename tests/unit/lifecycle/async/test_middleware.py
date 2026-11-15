"""
Unit tests for middleware on an AsyncScope.

This test suite verifies the following behaviors:

AsyncScope middleware:
- Each activation runs async and sync generator middleware in order, awaiting the async
  ones, and resumes them in reverse after the activation's resources have closed.
- Awaiting activate() runs the middleware to its yield, and awaiting deactivate() resumes it.
- A middleware sees the activation's error raised at its yield.
- An async middleware raising before its yield refuses the activation: the scope isn't left
  active, and the middleware already running sees the error.
- middleware= refuses an async def that isn't a generator, and a plain function, when the
  scope is built.
- A middleware, async or sync, can't swallow the activation's error: trying raises
  MiddlewareError from it in its place.
- Trying to swallow the task's own cancellation raises the cancellation again.
- An async middleware that never yields refuses the activation.
- An async middleware that ends its own activation before its yield is exited at once, and
  the activation's entry fails with EndedDuringEntryError.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator

import pytest

from stratae.lifecycle.exceptions import (
    EndedDuringEntryError,
    MiddlewareError,
    NothingToDeactivateError,
    ResourceDidNotYieldError,
)
from stratae.lifecycle.scope import AsyncScope
from stratae.middleware import MiddlewareKindError


class _Refused(RuntimeError):
    """A deliberate refusal, raised by a middleware on entry."""

    def __init__(self) -> None:
        super().__init__("refused")


class _Failed(LookupError):
    """A deliberate failure, raised in an activation's block."""

    def __init__(self) -> None:
        super().__init__("failed")


def _async_recording(log: list[str], name: str):
    """Return an async middleware recording its entry and exit, awaiting in each."""

    async def middleware(scope: AsyncScope) -> AsyncGenerator[None]:
        await asyncio.sleep(0)
        log.append(f"enter {name} active={scope.is_active()}")
        try:
            yield
        except BaseException as error:
            log.append(f"exit {name} {type(error).__name__}")
            raise
        await asyncio.sleep(0)
        log.append(f"exit {name} None active={scope.is_active()}")

    return middleware


def _sync_recording(log: list[str], name: str):
    """Return a sync middleware recording its entry and exit."""

    def middleware(scope: AsyncScope) -> Generator[None, None, None]:
        log.append(f"enter {name} active={scope.is_active()}")
        yield
        log.append(f"exit {name} None active={scope.is_active()}")

    return middleware


async def test_async_and_sync_middleware_wrap_each_activation():
    """
    Async and sync middleware should be entered in order and exited in reverse.

    Given: An async scope with an async middleware, a sync one, and an async resource
    When: The scope is activated with `async with`, the resource used, and the block ends
    Then: Both enter in order with the scope active, the resource closes first, and both exit
        in reverse with the scope no longer active
    """
    # Arrange
    log: list[str] = []
    scope = AsyncScope(
        "work", middleware=(_async_recording(log, "outer"), _sync_recording(log, "inner"))
    )

    @scope.cache()
    async def resource() -> AsyncGenerator[str]:
        await asyncio.sleep(0)
        log.append("resource entered")
        yield "resource"
        log.append("resource closed")

    # Act
    async with scope:
        await resource()

    # Assert
    assert log == [
        "enter outer active=True",
        "enter inner active=True",
        "resource entered",
        "resource closed",
        "exit inner None active=False",
        "exit outer None active=False",
    ]


async def test_activate_and_deactivate_run_middleware():
    """
    Awaiting activate() and deactivate() should enter and exit the middleware.

    Given: An async scope with an async recording middleware
    When: It's activated with activate() and deactivated with the token
    Then: The middleware is entered by activate() and exited by deactivate()
    """
    # Arrange
    log: list[str] = []
    scope = AsyncScope("work", middleware=(_async_recording(log, "middleware"),))

    # Act
    activation = await scope.activate()
    entered = list(log)
    await scope.deactivate(activation)

    # Assert
    assert entered == ["enter middleware active=True"]
    assert log == ["enter middleware active=True", "exit middleware None active=False"]


async def test_exit_sees_activation_error():
    """
    An async middleware's exit should see the error the activation ended with.

    Given: An async scope with an async recording middleware
    When: Its block raises
    Then: The middleware's exit sees the error, which propagates
    """
    # Arrange
    log: list[str] = []
    scope = AsyncScope("work", middleware=(_async_recording(log, "middleware"),))

    # Act
    with pytest.raises(_Failed):
        async with scope:
            raise _Failed

    # Assert
    assert log[-1] == "exit middleware _Failed"


async def test_refusing_async_middleware_ends_activation():
    """
    An async middleware raising on entry should refuse the activation.

    Given: An async scope whose first middleware records and second refuses after awaiting
    When: The scope is activated
    Then: The error propagates, the scope isn't active, and the first is exited with it
    """
    # Arrange
    log: list[str] = []

    async def refuse(scope: AsyncScope) -> AsyncGenerator[None]:
        await asyncio.sleep(0)
        raise _Refused
        yield  # unreachable, but makes this a generator

    scope = AsyncScope("work", middleware=(_async_recording(log, "first"), refuse))

    # Act
    with pytest.raises(_Refused):
        await scope.activate()

    # Assert
    assert not scope.is_active()
    assert log == ["enter first active=True", "exit first _Refused"]


async def _not_a_generator(scope: AsyncScope) -> None:
    await asyncio.sleep(0)


def _plain(scope: AsyncScope) -> None:
    return None


@pytest.mark.parametrize(
    "middleware", [_not_a_generator, _plain], ids=["async-def", "plain-function"]
)
def test_middleware_of_the_wrong_kind_refused(middleware: object):
    """
    An async scope should refuse a middleware that isn't a generator function when it's built.

    Given: An async def that isn't a generator, or a plain function
    When: An async scope is built with it as middleware
    Then: MiddlewareKindError names the scope and the middleware, before anything is called
    """
    # Act / Assert
    with pytest.raises(MiddlewareKindError, match=r"AsyncScope 'work'.*generator function"):
        AsyncScope("work", middleware=(middleware,))  # pyright: ignore[reportArgumentType]


async def _async_swallow(scope: AsyncScope) -> AsyncGenerator[None, None]:
    try:
        yield
    except BaseException:
        await asyncio.sleep(0)
        return


def _sync_swallow(scope: AsyncScope) -> Generator[None, None, None]:
    try:
        yield
    except BaseException:
        return


@pytest.mark.parametrize("middleware", [_async_swallow, _sync_swallow], ids=["async", "sync"])
async def test_middleware_cannot_swallow_activation_error(middleware: object):
    """
    A middleware, async or sync, shouldn't be able to swallow the activation's error.

    Given: An async scope whose middleware returns whatever its yield raises
    When: The block raises
    Then: MiddlewareError is raised from the block's error in its place, and the scope isn't
        active
    """
    # Arrange
    scope = AsyncScope("work", middleware=(middleware,))  # pyright: ignore[reportArgumentType]

    # Act
    with pytest.raises(MiddlewareError) as raised:
        async with scope:
            raise _Failed

    # Assert
    assert isinstance(raised.value.__cause__, _Failed)
    assert not scope.is_active()


@pytest.mark.parametrize("middleware", [_async_swallow, _sync_swallow], ids=["async", "sync"])
async def test_swallowing_task_cancellation_raises_it_again(middleware: object):
    """
    A middleware trying to swallow the task's own cancellation should raise it again.

    Given: An async scope whose middleware returns whatever its yield raises, active in a task
    When: The task is cancelled while its block waits
    Then: The task ends cancelled, rather than with MiddlewareError
    """
    # Arrange
    scope = AsyncScope("work", middleware=(middleware,))  # pyright: ignore[reportArgumentType]
    started = asyncio.Event()

    async def work() -> None:
        async with scope:
            started.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(work())
    await started.wait()

    # Act
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert
    assert task.cancelled()


async def _never_yields(scope: AsyncScope) -> AsyncGenerator[None, None]:
    await asyncio.sleep(0)
    return
    yield


async def test_async_middleware_that_never_yields_refuses():
    """
    An async middleware that returns without yielding should refuse the activation.

    Given: An async scope whose async generator middleware returns before its yield
    When: The scope is activated
    Then: ResourceDidNotYieldError propagates, and the scope isn't active
    """
    # Arrange
    scope = AsyncScope("work", middleware=(_never_yields,))

    # Act / Assert
    with pytest.raises(ResourceDidNotYieldError):
        async with scope:
            pass
    assert not scope.is_active()


async def test_async_middleware_ending_its_activation_before_yielding_is_exited():
    """
    Test that an async middleware whose activation ends while it's entering is exited at once.

    Given: An async middleware that deactivates its scope before yielding,
    When: The scope is entered,
    Then: The middleware is exited, the entry fails with EndedDuringEntryError, and the scope
          isn't left active.
    """
    # Arrange
    events: list[str] = []

    async def ending(scope: AsyncScope) -> AsyncGenerator[None]:
        await scope.deactivate()
        yield
        events.append("exit")

    scope = AsyncScope("request", middleware=(ending,))

    # Act
    with pytest.raises(NothingToDeactivateError) as raised:
        async with scope:
            events.append("body")

    # Assert
    assert isinstance(raised.value.__context__, EndedDuringEntryError)
    assert events == ["exit"]
    assert not scope.is_active()
