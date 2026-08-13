"""Smoke tests for the Starlette lifecycle integration, using a real Starlette app."""

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.testclient import TestClient

from stratae.integrations.starlette import scoped_route
from stratae.lifecycle import AsyncScope, async_resource

pytestmark = pytest.mark.starlette


@pytest.fixture
def events() -> list[str]:
    """Provide a fresh event log for the app fixture to record open/commit/rollback/close into."""
    return []


@pytest.fixture
def app(async_request_scope: AsyncScope, events: list[str]) -> Starlette:
    """Build a Starlette app whose routes activate the request scope via scoped_route."""

    @async_request_scope.cache()
    @async_resource
    async def get_transaction():
        events.append("open")
        try:
            yield "connection"
            events.append("commit")
        except Exception:
            events.append("rollback")
            raise
        finally:
            events.append("close")

    async def ok(request: Request) -> JSONResponse:
        await get_transaction()
        return JSONResponse({"status": "ok"})

    async def http_exception_route(request: Request) -> JSONResponse:
        await get_transaction()
        raise HTTPException(status_code=400, detail="deliberate 400")

    async def unhandled(request: Request) -> JSONResponse:
        await get_transaction()
        raise RuntimeError("deliberate unhandled failure")

    async def twice(request: Request) -> JSONResponse:
        first = await get_transaction()
        second = await get_transaction()
        return JSONResponse({"same_connection": first is second})

    async def twice_then_fail(request: Request) -> None:
        first = await get_transaction()
        second = await get_transaction()
        if first is not second:
            raise HTTPException(status_code=409, detail="expected the same cached connection")
        raise RuntimeError("deliberate failure after calling resource twice")

    async def stream(request: Request) -> StreamingResponse:
        connection = await get_transaction()

        async def body():
            events.append(f"stream:{connection}")
            yield b"chunk1"
            events.append("stream-done")
            yield b"chunk2"

        return StreamingResponse(body())

    async def stream_twice(request: Request) -> StreamingResponse:
        first = await get_transaction()

        async def body():
            second = await get_transaction()
            events.append(f"same_connection:{first is second}")
            yield b"chunk1"

        return StreamingResponse(body())

    async def stream_then_fail(request: Request) -> StreamingResponse:
        await get_transaction()

        async def body():
            yield b"chunk1"
            raise RuntimeError("deliberate failure mid-stream")

        return StreamingResponse(body())

    async def fail_before_resource(request: Request) -> None:
        raise RuntimeError("deliberate failure before touching any resource")

    async def stream_fail_before_resource(request: Request) -> StreamingResponse:
        async def body():
            yield b"chunk1"
            raise RuntimeError("deliberate mid-stream failure before touching any resource")

        return StreamingResponse(body())

    @async_request_scope.cache()
    @async_resource
    async def get_swallowing_transaction():
        events.append("open")
        try:
            yield "connection"
            events.append("commit")
        except ValueError:
            events.append("swallowed")
        finally:
            events.append("close")

    async def swallowed_exception_route(request: Request) -> JSONResponse:
        await get_swallowing_transaction()
        raise ValueError("deliberate failure swallowed by resource cleanup")

    async def stream_swallowed_exception(request: Request) -> StreamingResponse:
        await get_swallowing_transaction()

        async def body():
            yield b"chunk1"
            raise ValueError("deliberate mid-stream failure swallowed by resource cleanup")

        return StreamingResponse(body())

    Route = scoped_route(async_request_scope)
    return Starlette(
        routes=[
            Route("/ok", ok),
            Route("/http-exception", http_exception_route),
            Route("/unhandled", unhandled),
            Route("/twice", twice),
            Route("/twice-then-fail", twice_then_fail),
            Route("/stream", stream),
            Route("/stream-twice", stream_twice),
            Route("/fail-before-resource", fail_before_resource),
            Route("/stream-fail-before-resource", stream_fail_before_resource),
            Route("/stream-then-fail", stream_then_fail),
            Route("/swallowed-exception", swallowed_exception_route),
            Route("/stream-swallowed-exception", stream_swallowed_exception),
        ],
    )


def test_successful_request_commits(app: Starlette, events: list[str]):
    """
    A request scope activated via scoped_route should commit on success.

    Given: A Starlette app whose routes activate the request scope
    When: A route completes without raising
    Then: The request-scoped resource sees a clean close - open, commit, close
    """
    # Act
    client = TestClient(app)
    response = client.get("/ok")

    # Assert
    assert response.status_code == 200
    assert events == ["open", "commit", "close"]


def test_handled_exception_rolls_back(app: Starlette, events: list[str]):
    """
    HTTPException should still roll back the request-scoped resource.

    Given: A Starlette app whose routes activate the request scope
    When: A route raises HTTPException
    Then: The request-scoped resource still sees the exception and rolls back
    """
    # Act
    client = TestClient(app)
    response = client.get("/http-exception")

    # Assert
    assert response.status_code == 400
    assert events == ["open", "rollback", "close"]


def test_unhandled_exception_rolls_back(app: Starlette, events: list[str]):
    """
    An unhandled exception should still roll back the request-scoped resource.

    Given: A Starlette app whose routes activate the request scope
    When: A route raises an exception with no matching handler
    Then: The request-scoped resource sees the exception and rolls back
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/unhandled")

    # Assert
    assert response.status_code == 500
    assert events == ["open", "rollback", "close"]


def test_resource_called_twice_commits_once(app: Starlette, events: list[str]):
    """
    Calling the resource multiple times in one request shouldn't double-commit.

    Given: A Starlette app whose routes activate the request scope
    When: A route calls the request-scoped resource twice
    Then: Both calls return the same cached connection, and only a single
        open/commit/close cycle happens for the whole request
    """
    # Act
    client = TestClient(app)
    response = client.get("/twice")

    # Assert
    assert response.status_code == 200
    assert response.json() == {"same_connection": True}
    assert events == ["open", "commit", "close"]


def test_resource_called_twice_then_fails_rolls_back_once(app: Starlette, events: list[str]):
    """
    Calling the resource twice before failing should still only roll back once.

    Given: A Starlette app whose routes activate the request scope
    When: A route calls the request-scoped resource twice and then raises
    Then: Only a single open/rollback/close cycle happens for the whole request
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/twice-then-fail")

    # Assert
    assert response.status_code == 500
    assert events == ["open", "rollback", "close"]


def test_streaming_response_keeps_scope_open_until_body_completes(
    app: Starlette, events: list[str]
):
    """
    A StreamingResponse should keep the request scope open for its whole body.

    Given: A Starlette app whose routes activate the request scope
    When: A route returns a StreamingResponse whose body runs after the handler returns
    Then: The request-scoped resource stays open through the entire streamed body,
        and only commits/closes once the last chunk has been sent
    """
    # Act
    client = TestClient(app)
    response = client.get("/stream")

    # Assert
    assert response.status_code == 200
    assert response.text == "chunk1chunk2"
    assert events == ["open", "stream:connection", "stream-done", "commit", "close"]


def test_streaming_response_resource_still_valid_mid_stream(app: Starlette, events: list[str]):
    """
    The request-scoped resource should still be usable from inside a streamed body.

    Given: A Starlette app whose routes activate the request scope
    When: A route calls the request-scoped resource once, then again from inside the streamed body
    Then: The second call returns the same cached connection as the first
    """
    # Act
    client = TestClient(app)
    response = client.get("/stream-twice")

    # Assert
    assert response.status_code == 200
    assert response.text == "chunk1"
    assert events == ["open", "same_connection:True", "commit", "close"]


def test_streaming_response_rolls_back_on_body_failure(app: Starlette, events: list[str]):
    """
    A failure partway through a streamed body should still roll back the resource.

    Given: A Starlette app whose routes activate the request scope
    When: A route returns a StreamingResponse whose body raises after sending its first chunk
    Then: The request-scoped resource rolls back rather than committing
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/stream-then-fail")

    # Assert
    assert events == ["open", "rollback", "close"]


def test_exception_before_resource_use_still_propagates(app: Starlette, events: list[str]):
    """
    An exception raised before touching the resource should still propagate.

    Given: A Starlette app whose routes activate the request scope
    When: A route raises without ever calling the request-scoped resource
    Then: The exception still propagates as a 500, and no resource lifecycle events happen
        at all
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/fail-before-resource")

    # Assert
    assert response.status_code == 500
    assert events == []


def test_streaming_response_failure_before_resource_use_still_propagates(
    app: Starlette, events: list[str]
):
    """
    A mid-stream failure should still propagate even when the resource was never touched.

    Given: A Starlette app whose routes activate the request scope
    When: A route returns a StreamingResponse whose body raises without ever calling
        the request-scoped resource
    Then: The exception still propagates rather than being silently swallowed, and no
        resource lifecycle events happen at all
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/stream-fail-before-resource")

    # Assert
    assert events == []


def test_route_exception_still_propagates_when_resource_swallows_it(
    app: Starlette, events: list[str]
):
    """
    A route exception should still reach the client even when a resource swallows it.

    Given: A Starlette app whose request-scoped resource catches and suppresses the
        exception type the route raises
    When: The route raises that exception
    Then: The response is still a 500
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/swallowed-exception")

    # Assert
    assert response.status_code == 500
    assert events == ["open", "swallowed", "close"]


def test_streaming_response_exception_still_propagates_when_resource_swallows_it(
    app: Starlette, events: list[str]
):
    """
    Mid-stream exceptions should still surface even when a resource swallows it.

    Given: A Starlette app whose request-scoped resource catches and suppresses the
        exception type a streamed body raises
    When: The StreamingResponse body raises that exception after sending its first chunk
    Then: The exception still reaches the ASGI layer
    """
    # Act
    client = TestClient(app)
    with pytest.raises(ValueError, match="deliberate mid-stream failure"):
        client.get("/stream-swallowed-exception")

    # Assert
    assert events == ["open", "swallowed", "close"]
