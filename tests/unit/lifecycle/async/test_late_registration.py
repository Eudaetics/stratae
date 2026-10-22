"""Unit tests for cached functions registered while an AsyncScope activation is already live."""

import asyncio
from collections.abc import Awaitable, Callable

from stratae.lifecycle.resource import async_resource
from stratae.lifecycle.scope import AsyncScope


async def _call_after_late_registration(
    scope: AsyncScope, register: Callable[[], None], call: Callable[[], Awaitable[object]]
) -> tuple[object, object]:
    """
    Call `call` twice from a live activation, after another task ran `register`.

    The registering task never sees this activation, so the new slot can't be added to it.
    """
    registered = asyncio.Event()
    started = asyncio.Event()

    async def handle_request() -> tuple[object, object]:
        async with scope:
            started.set()
            await registered.wait()
            return await call(), await call()

    async def register_elsewhere() -> None:
        await started.wait()
        register()
        registered.set()

    request, _ = await asyncio.gather(
        asyncio.create_task(handle_request()), asyncio.create_task(register_elsewhere())
    )
    return request


async def test_function_registered_during_live_activation_is_cached(
    async_context_scope: AsyncScope,
):
    """
    A cached function registered after an activation started works within that activation.

    Given: A context scope activation already live in one task,
    When: another task registers a new cached coroutine function, and the first task calls it,
    Then: the call should compute its value, and a second call should return the cached one.
    """
    # Arrange
    calls = 0
    registry: dict[str, Callable[[], Awaitable[object]]] = {}

    def register() -> None:
        @async_context_scope.cache()
        async def get_late() -> object:
            nonlocal calls
            calls += 1
            return object()

        registry["late"] = get_late

    # Act
    first, second = await _call_after_late_registration(
        async_context_scope, register, lambda: registry["late"]()
    )

    # Assert
    assert first is second
    assert calls == 1


async def test_keyed_function_registered_during_live_activation_is_cached(
    async_context_scope: AsyncScope,
):
    """
    A keyed cached function registered after an activation started works within it.

    Given: A context scope activation already live in one task,
    When: another task registers a new keyed cached coroutine function, and the first task
        calls it with the same key twice,
    Then: the first call should compute its value, and the second should return it cached.
    """
    # Arrange
    calls = 0
    registry: dict[str, Callable[[str], Awaitable[object]]] = {}

    def register() -> None:
        @async_context_scope.cache()
        async def get_item(key: str) -> object:
            nonlocal calls
            calls += 1
            return object()

        registry["late"] = get_item

    # Act
    first, second = await _call_after_late_registration(
        async_context_scope, register, lambda: registry["late"]("a")
    )

    # Assert
    assert first is second
    assert calls == 1


async def test_resource_registered_during_live_activation_is_entered_and_cleaned_up(
    async_context_scope: AsyncScope,
):
    """
    A cached resource registered after an activation started is entered once and cleaned up.

    Given: A context scope activation already live in one task,
    When: another task registers a new cached async resource, and the first task uses it,
    Then: it should be entered once, and exited when that activation ends.
    """
    # Arrange
    events: list[str] = []
    registry: dict[str, Callable[[], Awaitable[object]]] = {}

    def register() -> None:
        @async_context_scope.cache()
        @async_resource
        async def get_connection():
            events.append("open")
            try:
                yield object()
            finally:
                events.append("close")

        registry["late"] = get_connection

    # Act
    first, second = await _call_after_late_registration(
        async_context_scope, register, lambda: registry["late"]()
    )

    # Assert
    assert first is second
    assert events == ["open", "close"]
