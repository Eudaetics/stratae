"""
Smoke tests for the FastAPI WebSocket lifecycle integration, using a real FastAPI app.

This test suite verifies the following behaviors:

scoped_websocket:
- A connection that the server closes ends its scope cleanly.
- A scope-cached resource is created once per connection.
- Concurrent connections get separate scopes.
- FastAPI dependencies and path parameters resolve inside the scope.
- A client disconnect the endpoint doesn't catch reaches cleanup as WebSocketDisconnect.
- A WebSocketException, which FastAPI handles inside the route, still reaches cleanup.
- An unhandled exception reaches cleanup and propagates.
- A cancelled connection reaches cleanup as CancelledError.
- The scope applies to routes on an included router, whether set per route or on the
  router.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Annotated

import pytest

pytest.importorskip("fastapi")

import fastapi
from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect, WebSocketException
from fastapi.testclient import TestClient

from stratae.integrations.fastapi import scoped_websocket
from stratae.lifecycle import AsyncScope, async_resource

pytestmark = pytest.mark.fastapi


@pytest.fixture
def events() -> list[str]:
    """Provide a fresh event log for the app fixture to record how each connection ended."""
    return []


@pytest.fixture
def get_session(
    async_request_scope: AsyncScope, events: list[str]
) -> Callable[..., Awaitable[object]]:
    """Provide a connection-scoped resource that records how its connection ended."""

    @async_request_scope.cache()
    @async_resource
    async def get_session():
        events.append("open")
        try:
            yield object()
        except BaseException as exc:
            events.append(f"ended with {type(exc).__name__}")
            raise
        else:
            events.append("ended cleanly")

    return get_session


@pytest.fixture
def app(
    async_request_scope: AsyncScope,
    events: list[str],
    get_session: Callable[..., Awaitable[object]],
) -> FastAPI:
    """Build a FastAPI app whose included WebSocket routes activate a scope per connection."""
    router = APIRouter(dependencies=[scoped_websocket(async_request_scope)])

    async def current_session() -> object:
        return await get_session()

    @router.websocket("/server-close")
    async def server_close(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        await websocket.close()

    @router.websocket("/twice")
    async def twice(websocket: WebSocket) -> None:
        await websocket.accept()
        first = await get_session()
        second = await get_session()
        await websocket.send_json({"same_session": first is second})
        await websocket.close()

    @router.websocket("/session-id")
    async def session_id(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json({"session": id(await get_session())})
        await websocket.receive_text()
        await websocket.close()

    @router.websocket("/orders/{order_id}")
    async def order_updates(
        websocket: WebSocket,
        order_id: str,
        session: Annotated[object, fastapi.Depends(current_session)],
    ) -> None:
        await websocket.accept()
        await websocket.send_json(
            {"order_id": order_id, "same_session": session is await get_session()}
        )
        await websocket.close()

    @router.websocket("/until-disconnect")
    async def until_disconnect(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        await websocket.receive_text()

    @router.websocket("/reject")
    async def reject(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        raise WebSocketException(code=4401, reason="deliberate rejection")

    @router.websocket("/unhandled")
    async def unhandled(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        raise RuntimeError("deliberate unhandled failure")

    @router.websocket("/suspend")
    async def suspend(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        events.append("suspended")
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    app = FastAPI()
    app.include_router(router, prefix="/ws")
    return app


def test_server_close_ends_scope_cleanly(app: FastAPI, events: list[str]):
    """
    A connection that the server closes should end its scope without an exception.

    Given: A FastAPI app whose WebSocket routes activate a scope per connection
    When: The endpoint closes the connection itself
    Then: The connection-scoped resource sees a clean end
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/ws/server-close") as websocket:
        with pytest.raises(WebSocketDisconnect):
            websocket.receive_text()

    # Assert
    assert events == ["open", "ended cleanly"]


def test_resource_cached_once_per_connection(app: FastAPI, events: list[str]):
    """
    A scope-cached resource should be created once for the whole connection.

    Given: A FastAPI app whose WebSocket routes activate a scope per connection
    When: The endpoint calls the connection-scoped resource twice
    Then: Both calls return the same instance, opened and ended once
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/ws/twice") as websocket:
        reply = websocket.receive_json()

    # Assert
    assert reply == {"same_session": True}
    assert events == ["open", "ended cleanly"]


def test_concurrent_connections_get_separate_scopes(app: FastAPI, events: list[str]):
    """
    Connections open at the same time should each get their own scope.

    Given: A FastAPI app whose WebSocket routes activate a scope per connection
    When: Two connections are open at once and each reports its session
    Then: The sessions differ, and each connection opens and ends its own
    """
    # Arrange
    client = TestClient(app)

    # Act
    with (
        client.websocket_connect("/ws/session-id") as first,
        client.websocket_connect("/ws/session-id") as second,
    ):
        first_session = first.receive_json()["session"]
        second_session = second.receive_json()["session"]
        first.send_text("done")
        second.send_text("done")

    # Assert
    assert first_session != second_session
    assert events == ["open", "open", "ended cleanly", "ended cleanly"]


def test_fastapi_dependencies_resolve_inside_scope(app: FastAPI, events: list[str]):
    """
    FastAPI dependencies and path parameters should resolve with the scope already active.

    Given: A FastAPI WebSocket endpoint taking a path parameter and a FastAPI dependency
        that reads the connection-scoped resource
    When: A client connects
    Then: The dependency gets the same instance as the endpoint body, and the path
        parameter is bound
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/ws/orders/42") as websocket:
        reply = websocket.receive_json()

    # Assert
    assert reply == {"order_id": "42", "same_session": True}
    assert events == ["open", "ended cleanly"]


def test_client_disconnect_reaches_cleanup(app: FastAPI, events: list[str]):
    """
    A client disconnect the endpoint doesn't catch should reach cleanup as an exception.

    Given: A FastAPI WebSocket endpoint that waits for a message without catching
        WebSocketDisconnect
    When: The client closes the connection
    Then: The connection-scoped resource sees WebSocketDisconnect, which propagates
    """
    # Arrange
    client = TestClient(app)

    # Act
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/until-disconnect") as websocket:
            websocket.close()

    # Assert
    assert events == ["open", "ended with WebSocketDisconnect"]


def test_handled_websocket_exception_reaches_cleanup(app: FastAPI, events: list[str]):
    """
    A WebSocketException should reach cleanup even though FastAPI handles it.

    Given: A FastAPI WebSocket endpoint that raises WebSocketException
    When: A client connects
    Then: The client is closed with the exception's code, and the connection-scoped
        resource sees the WebSocketException
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/ws/reject") as websocket:
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_text()

    # Assert
    assert closed.value.code == 4401
    assert events == ["open", "ended with WebSocketException"]


def test_unhandled_exception_reaches_cleanup(app: FastAPI, events: list[str]):
    """
    An unhandled exception should reach cleanup and still propagate.

    Given: A FastAPI WebSocket endpoint that raises an exception with no handler
    When: A client connects
    Then: The exception propagates, and the connection-scoped resource sees it
    """
    # Arrange
    client = TestClient(app)

    # Act
    with pytest.raises(RuntimeError, match="deliberate unhandled failure"):
        with client.websocket_connect("/ws/unhandled") as websocket:
            websocket.receive_text()

    # Assert
    assert events == ["open", "ended with RuntimeError"]


async def test_cancelled_connection_reaches_cleanup(
    app: FastAPI, events: list[str], cancel_mid_websocket: Callable[..., Awaitable[None]]
):
    """
    A connection cancelled mid-endpoint should reach cleanup as CancelledError.

    Given: A FastAPI WebSocket endpoint suspended mid-connection
    When: The connection is cancelled, as a server shutdown would
    Then: The connection-scoped resource sees CancelledError
    """
    # Act
    await cancel_mid_websocket(app, "/ws/suspend", lambda: "suspended" in events)

    # Assert
    assert events == ["open", "suspended", "ended with CancelledError"]


def test_route_dependency_scopes_included_route(
    async_request_scope: AsyncScope,
    events: list[str],
    get_session: Callable[..., Awaitable[object]],
):
    """
    A scope set on one route should still apply once its router is included.

    Given: A router whose WebSocket route lists scoped_websocket in its own dependencies
    When: The router is included in an app and a client connects
    Then: The connection-scoped resource opens and ends with the connection
    """
    # Arrange
    router = APIRouter()

    @router.websocket("/server-close", dependencies=[scoped_websocket(async_request_scope)])
    async def server_close(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        await websocket.close()

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    # Act
    with client.websocket_connect("/server-close") as websocket:
        with pytest.raises(WebSocketDisconnect):
            websocket.receive_text()

    # Assert
    assert events == ["open", "ended cleanly"]
