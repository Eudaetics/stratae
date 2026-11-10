"""
Unit tests for middleware on an AsyncScope.

This test suite verifies the following behaviors:

AsyncScope middleware:
- Each activation enters async and sync middleware in order, awaiting the async ones, and
  exits them in reverse after the activation's resources have closed.
- Awaiting activate() enters the middleware, and awaiting deactivate() exits it.
- An exit sees the activation's error.
- An async middleware raising on entry refuses the activation: the scope isn't left active,
  and the middleware already entered is exited with the error.
- An async context manager class is accepted as middleware.
- middleware= refuses a class that isn't a context manager.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import ClassVar

import pytest

from stratae.lifecycle.exceptions import MiddlewareNotEnterableError
from stratae.lifecycle.scope import AsyncScope


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

    @asynccontextmanager
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

    @contextmanager
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

    @asynccontextmanager
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


class _Timed:
    """An async context manager class, taking the scope, used as middleware."""

    entered: ClassVar[list[str]] = []

    def __init__(self, scope: AsyncScope) -> None:
        self.scope = scope

    async def __aenter__(self) -> None:
        await asyncio.sleep(0)
        self.entered.append(self.scope.name)

    async def __aexit__(self, *_: object) -> None:
        await asyncio.sleep(0)


async def test_async_context_manager_class_is_middleware():
    """
    An async context manager class should be usable as an async scope's middleware.

    Given: An async context manager class taking the scope
    When: An async scope built with it is activated
    Then: The class is instantiated with the scope and entered
    """
    # Arrange
    _Timed.entered = []
    scope = AsyncScope("work", middleware=(_Timed,))

    # Act
    async with scope:
        pass

    # Assert
    assert _Timed.entered == ["work"]


class _NotAManager:
    """A class that isn't a context manager."""

    def __init__(self, scope: AsyncScope) -> None:
        self.scope = scope


def test_unenterable_middleware_class_refused():
    """
    An async scope should refuse a middleware class that isn't a context manager.

    Given: A class that isn't a context manager
    When: An async scope is built with it as middleware
    Then: MiddlewareNotEnterableError names the class
    """
    # Act / Assert
    with pytest.raises(MiddlewareNotEnterableError, match="_NotAManager"):
        AsyncScope("work", middleware=(_NotAManager,))  # pyright: ignore[reportArgumentType]
