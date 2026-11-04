"""
Smoke tests for the ASGI envelope middleware, using a real Starlette app.

This test suite verifies the following behaviors:

- An async endpoint, and a sync one run in Starlette's threadpool, see the request's envelope.
- A response from an exception handler, and a 404, carry the correlation id.
- A WebSocket endpoint sees the connection's envelope, and the accept carries its id.
- The envelope is active inside a scoped_route's scope.
- Lifespan startup and shutdown run as they would without the middleware.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route, WebSocketRoute
from starlette.testclient import TestClient
from starlette.websockets import WebSocket

from stratae.events.envelope import Envelope
from stratae.integrations.asgi import EnvelopeMiddleware
from stratae.integrations.starlette import scoped_route
from stratae.lifecycle import AsyncScope

pytestmark = pytest.mark.starlette

_CALLER = "0b9d7a52-6f1e-4c3a-9d2b-5e8f1a7c4d60"
_HEADERS = {"x-correlation-id": _CALLER}


def _chain() -> str:
    """Return the active envelope's correlation id."""
    envelope = Envelope.current()
    assert envelope is not None
    return str(envelope.correlation_id)


async def _async_endpoint(request: Request) -> PlainTextResponse:
    await asyncio.sleep(0)
    return PlainTextResponse(_chain())


def _sync_endpoint(request: Request) -> PlainTextResponse:
    return PlainTextResponse(_chain())


async def _failing(request: Request) -> PlainTextResponse:
    await asyncio.sleep(0)
    raise LookupError("missing")


async def _on_lookup_error(request: Request, exc: Exception) -> JSONResponse:
    await asyncio.sleep(0)
    return JSONResponse({"chain": _chain()}, status_code=404)


async def _socket(websocket: WebSocket) -> None:
    await websocket.accept()
    await websocket.send_text(_chain())
    await websocket.close()


type Lifespan = Callable[[Starlette], AbstractAsyncContextManager[None]]


def _app(lifespan: Lifespan | None = None) -> Starlette:
    """Build an app with the middleware outermost, and the shared routes."""
    return Starlette(
        routes=[
            Route("/async", _async_endpoint),
            Route("/sync", _sync_endpoint),
            Route("/failing", _failing),
            WebSocketRoute("/socket", _socket),
        ],
        exception_handlers={LookupError: _on_lookup_error},
        middleware=[Middleware(EnvelopeMiddleware)],
        lifespan=lifespan,
    )


@pytest.mark.parametrize("path", ["/async", "/sync"])
def test_endpoints_see_request_envelope(path: str):
    """
    Async and sync endpoints should both see the request's envelope.

    Given: An app with the middleware, and an async and a sync endpoint
    When: Each is requested with a correlation id header
    Then: The endpoint sees the caller's correlation id, and the response echoes it
    """
    # Arrange
    with TestClient(_app()) as client:
        # Act
        response = client.get(path, headers=_HEADERS)

    # Assert
    assert response.text == _CALLER
    assert response.headers["x-correlation-id"] == _CALLER


def test_handled_errors_and_404_carry_correlation_id():
    """
    Responses from an exception handler, and a 404, should carry the correlation id.

    Given: An app with the middleware, and a handler for LookupError
    When: A failing route and a missing path are requested
    Then: The handler sees the request's chain, and both responses echo it
    """
    # Arrange
    with TestClient(_app()) as client:
        # Act
        handled = client.get("/failing", headers=_HEADERS)
        missing = client.get("/missing", headers=_HEADERS)

    # Assert
    assert handled.json() == {"chain": _CALLER}
    assert missing.status_code == 404
    assert handled.headers["x-correlation-id"] == missing.headers["x-correlation-id"] == _CALLER


def test_websocket_sees_connection_envelope():
    """
    A WebSocket endpoint should see the connection's envelope.

    Given: An app with the middleware and a WebSocket route
    When: A client connects with a correlation id header
    Then: The endpoint sees the caller's chain, and the accept carries its id
    """
    # Arrange
    with TestClient(_app()) as client, client.websocket_connect("/socket", headers=_HEADERS) as ws:
        # Act
        chain = ws.receive_text()
        accepted = ws.extra_headers

    # Assert
    assert chain == _CALLER
    assert accepted == [(b"x-correlation-id", _CALLER.encode())]


def test_envelope_active_inside_scoped_route(async_request_scope: AsyncScope):
    """
    The envelope should be active inside a scoped_route's scope.

    Given: A scope-cached resource that records the chain it was created in
    When: A scoped route using it is requested with a correlation id header
    Then: The resource was created in the caller's chain
    """
    # Arrange
    chains: list[str] = []

    @async_request_scope.cache()
    async def get_chain() -> str:
        await asyncio.sleep(0)
        chains.append(_chain())
        return chains[-1]

    async def endpoint(request: Request) -> PlainTextResponse:
        return PlainTextResponse(await get_chain())

    app = Starlette(
        routes=[scoped_route(async_request_scope)("/", endpoint)],
        middleware=[Middleware(EnvelopeMiddleware)],
    )

    # Act
    with TestClient(app) as client:
        response = client.get("/", headers=_HEADERS)

    # Assert
    assert chains == [_CALLER]
    assert response.text == _CALLER


def test_lifespan_runs():
    """
    Lifespan startup and shutdown should run as they would without the middleware.

    Given: An app with the middleware and a lifespan
    When: The test client starts and stops it
    Then: Both run, outside any envelope
    """
    # Arrange
    seen: list[tuple[str, Envelope | None]] = []

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncGenerator[None]:
        seen.append(("startup", Envelope.current()))
        yield
        seen.append(("shutdown", Envelope.current()))

    # Act
    with TestClient(_app(lifespan=lifespan)) as client:
        client.get("/async")

    # Assert
    assert seen == [("startup", None), ("shutdown", None)]
