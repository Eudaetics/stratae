"""
Smoke tests for the Starlette WebSocket lifecycle integration, using a real Starlette app.

This test suite verifies the following behaviors:

scoped_websocket_route:
- A connection that the server closes ends its scope cleanly.
- A scope-cached resource is created once per connection.
- Concurrent connections get separate scopes.
- A client disconnect the endpoint doesn't catch reaches cleanup as WebSocketDisconnect.
- A WebSocketException, which Starlette handles inside the route, still reaches cleanup.
- An unhandled exception reaches cleanup and propagates.
- A cancelled connection reaches cleanup as CancelledError.
- A WebSocketEndpoint class ends its scope cleanly on a client disconnect, and its
  exceptions reach cleanup.
- A sync function endpoint is rejected when the route is built.
- The route keeps its endpoint's name for URL lookups.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.endpoints import WebSocketEndpoint
from starlette.exceptions import WebSocketException
from starlette.testclient import TestClient
from starlette.websockets import WebSocket, WebSocketDisconnect

from stratae.integrations.starlette import scoped_websocket_route
from stratae.lifecycle import AsyncScope, async_resource

pytestmark = pytest.mark.starlette


@pytest.fixture
def events() -> list[str]:
    """Provide a fresh event log for the app fixture to record how each connection ended."""
    return []


@pytest.fixture
def app(async_request_scope: AsyncScope, events: list[str]) -> Starlette:
    """Build a Starlette app whose WebSocket routes activate a scope per connection."""

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

    async def server_close(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        await websocket.close()

    async def twice(websocket: WebSocket) -> None:
        await websocket.accept()
        first = await get_session()
        second = await get_session()
        await websocket.send_json({"same_session": first is second})
        await websocket.close()

    async def session_id(websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json({"session": id(await get_session())})
        await websocket.receive_text()
        await websocket.close()

    async def until_disconnect(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        await websocket.receive_text()

    async def reject(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        raise WebSocketException(code=4401, reason="deliberate rejection")

    async def unhandled(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        raise RuntimeError("deliberate unhandled failure")

    async def suspend(websocket: WebSocket) -> None:
        await websocket.accept()
        await get_session()
        events.append("suspended")
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    class Session(WebSocketEndpoint):
        async def on_connect(self, websocket: WebSocket) -> None:
            await websocket.accept()
            await get_session()

        async def on_receive(self, websocket: WebSocket, data: Any) -> None:
            raise WebSocketException(code=4400, reason="deliberate rejection")

    WebSocketRoute = scoped_websocket_route(async_request_scope)
    return Starlette(
        routes=[
            WebSocketRoute("/server-close", server_close),
            WebSocketRoute("/twice", twice),
            WebSocketRoute("/session-id", session_id),
            WebSocketRoute("/until-disconnect", until_disconnect),
            WebSocketRoute("/reject", reject),
            WebSocketRoute("/unhandled", unhandled),
            WebSocketRoute("/suspend", suspend),
            WebSocketRoute("/class", Session),
        ],
    )


def test_server_close_ends_scope_cleanly(app: Starlette, events: list[str]):
    """
    A connection that the server closes should end its scope without an exception.

    Given: A Starlette app whose WebSocket routes activate a scope per connection
    When: The endpoint closes the connection itself
    Then: The connection-scoped resource sees a clean end
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/server-close") as websocket:
        with pytest.raises(WebSocketDisconnect):
            websocket.receive_text()

    # Assert
    assert events == ["open", "ended cleanly"]


def test_resource_cached_once_per_connection(app: Starlette, events: list[str]):
    """
    A scope-cached resource should be created once for the whole connection.

    Given: A Starlette app whose WebSocket routes activate a scope per connection
    When: The endpoint calls the connection-scoped resource twice
    Then: Both calls return the same instance, opened and ended once
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/twice") as websocket:
        reply = websocket.receive_json()

    # Assert
    assert reply == {"same_session": True}
    assert events == ["open", "ended cleanly"]


def test_concurrent_connections_get_separate_scopes(app: Starlette, events: list[str]):
    """
    Connections open at the same time should each get their own scope.

    Given: A Starlette app whose WebSocket routes activate a scope per connection
    When: Two connections are open at once and each reports its session
    Then: The sessions differ, and each connection opens and ends its own
    """
    # Arrange
    client = TestClient(app)

    # Act
    with (
        client.websocket_connect("/session-id") as first,
        client.websocket_connect("/session-id") as second,
    ):
        first_session = first.receive_json()["session"]
        second_session = second.receive_json()["session"]
        first.send_text("done")
        second.send_text("done")

    # Assert
    assert first_session != second_session
    assert events == ["open", "open", "ended cleanly", "ended cleanly"]


def test_client_disconnect_reaches_cleanup(app: Starlette, events: list[str]):
    """
    A client disconnect the endpoint doesn't catch should reach cleanup as an exception.

    Given: A Starlette app whose WebSocket endpoint waits for a message without catching
        WebSocketDisconnect
    When: The client closes the connection
    Then: The connection-scoped resource sees WebSocketDisconnect, which propagates
    """
    # Arrange
    client = TestClient(app)

    # Act
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/until-disconnect") as websocket:
            websocket.close()

    # Assert
    assert events == ["open", "ended with WebSocketDisconnect"]


def test_handled_websocket_exception_reaches_cleanup(app: Starlette, events: list[str]):
    """
    A WebSocketException should reach cleanup even though Starlette handles it.

    Given: A Starlette app whose WebSocket endpoint raises WebSocketException
    When: A client connects
    Then: The client is closed with the exception's code, and the connection-scoped
        resource sees the WebSocketException
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/reject") as websocket:
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_text()

    # Assert
    assert closed.value.code == 4401
    assert events == ["open", "ended with WebSocketException"]


def test_unhandled_exception_reaches_cleanup(app: Starlette, events: list[str]):
    """
    An unhandled exception should reach cleanup and still propagate.

    Given: A Starlette app whose WebSocket endpoint raises an exception with no handler
    When: A client connects
    Then: The exception propagates, and the connection-scoped resource sees it
    """
    # Arrange
    client = TestClient(app)

    # Act
    with pytest.raises(RuntimeError, match="deliberate unhandled failure"):
        with client.websocket_connect("/unhandled") as websocket:
            websocket.receive_text()

    # Assert
    assert events == ["open", "ended with RuntimeError"]


async def test_cancelled_connection_reaches_cleanup(
    app: Starlette, events: list[str], cancel_mid_websocket: Callable[..., Awaitable[None]]
):
    """
    A connection cancelled mid-endpoint should reach cleanup as CancelledError.

    Given: A Starlette app whose WebSocket endpoint is suspended mid-connection
    When: The connection is cancelled, as a server shutdown would
    Then: The connection-scoped resource sees CancelledError
    """
    # Act
    await cancel_mid_websocket(app, "/suspend", lambda: "suspended" in events)

    # Assert
    assert events == ["open", "suspended", "ended with CancelledError"]


def test_class_endpoint_client_disconnect_ends_scope_cleanly(app: Starlette, events: list[str]):
    """
    A WebSocketEndpoint class that handles the disconnect should end its scope cleanly.

    Given: A Starlette app whose scoped WebSocket route takes a WebSocketEndpoint class
    When: The client closes the connection
    Then: The connection-scoped resource sees a clean end
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/class") as websocket:
        websocket.close()

    # Assert
    assert events == ["open", "ended cleanly"]


def test_class_endpoint_exception_reaches_cleanup(app: Starlette, events: list[str]):
    """
    An exception from a WebSocketEndpoint class should reach cleanup.

    Given: A Starlette app whose WebSocketEndpoint raises WebSocketException on a message
    When: The client sends a message
    Then: The connection-scoped resource sees the WebSocketException
    """
    # Arrange
    client = TestClient(app)

    # Act
    with client.websocket_connect("/class") as websocket:
        websocket.send_text("hello")
        with pytest.raises(WebSocketDisconnect):
            websocket.receive_text()

    # Assert
    assert events == ["open", "ended with WebSocketException"]


def test_sync_endpoint_is_rejected_when_route_is_built(async_request_scope: AsyncScope):
    """
    A sync function endpoint fails clearly when its route is built, not on first connection.

    Given: A sync function, which Starlette can't run as a WebSocket endpoint
    When: A scoped WebSocket route is built for it
    Then: A TypeError explains that the route needs an async endpoint
    """

    # Arrange
    def echo(websocket: WebSocket) -> None:
        pass

    WebSocketRoute = scoped_websocket_route(async_request_scope)

    # Act & Assert
    with pytest.raises(TypeError, match="needs an async endpoint"):
        WebSocketRoute("/echo", echo)


def test_route_keeps_endpoint_name_for_url_lookup(async_request_scope: AsyncScope):
    """
    A scoped WebSocket route is named after its endpoint, so URL lookups by name still work.

    Given: A Starlette app with a scoped WebSocket route for an endpoint named order_updates
    When: A URL is looked up by that endpoint's name
    Then: The route's path is returned
    """

    # Arrange
    async def order_updates(websocket: WebSocket) -> None:
        pass

    WebSocketRoute = scoped_websocket_route(async_request_scope)
    named_app = Starlette(routes=[WebSocketRoute("/orders/{order_id}", order_updates)])

    # Act
    path = named_app.url_path_for("order_updates", order_id="1")

    # Assert
    assert path == "/orders/1"
