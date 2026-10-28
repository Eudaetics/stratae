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
            await asyncio.Event().wait()
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


@pytest.fixture
def cancel_mid_websocket() -> Callable[[ASGIApp, str, Callable[[], bool]], Awaitable[None]]:
    """
    Provide a driver that cancels an open WebSocket connection, as a server shutdown would.

    The driver connects over raw ASGI, waits until `suspended()` reports the endpoint is
    parked mid-connection, then cancels the connection task and waits for it to unwind.
    """

    async def drive(app: ASGIApp, path: str, suspended: Callable[[], bool]) -> None:
        scope: dict[str, Any] = {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "scheme": "ws",
            "path": path,
            "raw_path": path.encode(),
            "root_path": "",
            "query_string": b"",
            "headers": [],
            "subprotocols": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        }
        incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        incoming.put_nowait({"type": "websocket.connect"})

        async def send(message: dict[str, Any]) -> None:
            pass

        task = asyncio.ensure_future(app(scope, incoming.get, send))
        async with asyncio.timeout(2):
            while not suspended():
                await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    return drive


@pytest.fixture
def record_response_start() -> Callable[[ASGIApp, list[str]], ASGIApp]:
    """
    Provide a wrapper that logs when an app starts its response, into a given event list.

    Lets a test place the response's start - the point after which the status can no longer
    change - among the events its resources record.
    """

    def wrap(app: ASGIApp, events: list[str]) -> ASGIApp:
        async def recording_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
            async def recording_send(message: dict[str, Any]) -> None:
                if message["type"] == "http.response.start":
                    events.append(f"response-start: {message['status']}")
                await send(message)

            await app(scope, receive, recording_send)

        return recording_app

    return wrap
