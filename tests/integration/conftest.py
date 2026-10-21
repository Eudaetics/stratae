"""Pytest fixtures shared by the ASGI framework integration tests."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

import pytest

type ASGIApp = Callable[[dict[str, Any], Any, Any], Awaitable[None]]


@pytest.fixture
def cancel_mid_request() -> Callable[[ASGIApp, str, Callable[[], bool]], Awaitable[None]]:
    """
    Provide a driver that cancels an in-flight request, as a client disconnect or timeout would.

    The driver calls the app over raw ASGI - no test client, which would wait the request
    out - waits until `suspended()` reports the handler is parked mid-request, then cancels
    the request task and waits for it to unwind.
    """

    async def drive(app: ASGIApp, path: str, suspended: Callable[[], bool]) -> None:
        scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "root_path": "",
            "query_string": b"",
            "headers": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        }

        async def receive() -> dict[str, Any]:
            await asyncio.Event().wait()  # the client never sends anything further
            raise AssertionError("unreachable")

        async def send(message: dict[str, Any]) -> None:
            pass

        task = asyncio.ensure_future(app(scope, receive, send))
        async with asyncio.timeout(2):
            while not suspended():
                await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    return drive
