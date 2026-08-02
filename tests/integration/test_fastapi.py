"""Smoke tests for the FastAPI lifecycle integration, using a real FastAPI app."""

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from stratae.integrations.fastapi import scoped_route
from stratae.lifecycle import AsyncScope, async_resource

pytestmark = pytest.mark.fastapi


@pytest.fixture
def events() -> list[str]:
    """Provide a fresh event log for the app fixture to record open/commit/rollback/close into."""
    return []


@pytest.fixture
def app(async_request_scope: AsyncScope, events: list[str]) -> FastAPI:
    """Build a FastAPI app whose request scope is activated via scoped_route."""

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

    fastapi_app = FastAPI()
    fastapi_app.router.route_class = scoped_route(async_request_scope)

    @fastapi_app.get("/ok")
    async def ok() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()
        return {"status": "ok"}

    @fastapi_app.get("/http-exception")
    async def http_exception_route() -> None:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()
        raise HTTPException(status_code=400, detail="deliberate 400")

    @fastapi_app.get("/unhandled")
    async def unhandled() -> None:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()
        raise RuntimeError("deliberate unhandled failure")

    @fastapi_app.get("/twice")
    async def twice() -> dict[str, bool]:  # pyright: ignore[reportUnusedFunction]
        first = await get_transaction()
        second = await get_transaction()
        return {"same_connection": first is second}

    @fastapi_app.get("/twice-then-fail")
    async def twice_then_fail() -> None:  # pyright: ignore[reportUnusedFunction]
        first = await get_transaction()
        second = await get_transaction()
        if first is not second:
            raise HTTPException(status_code=409, detail="expected the same cached connection")
        raise RuntimeError("deliberate failure after calling resource twice")

    @fastapi_app.get("/stream")
    async def stream() -> StreamingResponse:  # pyright: ignore[reportUnusedFunction]
        connection = await get_transaction()

        async def body():
            events.append(f"stream:{connection}")
            yield b"chunk1"
            events.append("stream-done")
            yield b"chunk2"

        return StreamingResponse(body())

    @fastapi_app.get("/stream-twice")
    async def stream_twice() -> StreamingResponse:  # pyright: ignore[reportUnusedFunction]
        first = await get_transaction()

        async def body():
            second = await get_transaction()
            events.append(f"same_connection:{first is second}")
            yield b"chunk1"

        return StreamingResponse(body())

    @fastapi_app.get("/stream-then-fail")
    async def stream_then_fail() -> StreamingResponse:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()

        async def body():
            yield b"chunk1"
            raise RuntimeError("deliberate failure mid-stream")

        return StreamingResponse(body())

    @fastapi_app.get("/fail-before-resource")
    async def fail_before_resource() -> None:  # pyright: ignore[reportUnusedFunction]
        raise RuntimeError("deliberate failure before touching any resource")

    @fastapi_app.get("/stream-fail-before-resource")
    async def stream_fail_untouched() -> StreamingResponse:  # pyright: ignore[reportUnusedFunction]
        async def body():
            yield b"chunk1"
            raise RuntimeError("deliberate mid-stream failure before touching any resource")

        return StreamingResponse(body())

    return fastapi_app


def test_successful_request_commits(app: FastAPI, events: list[str]):
    """
    Test that a request scope activated via scoped_route commits on success.

    Given: A FastAPI app whose route class activates the request scope
    When: A route completes without raising
    Then: The request-scoped resource sees a clean close - open, commit, close
    """
    # Act
    client = TestClient(app)
    response = client.get("/ok")

    # Assert
    assert response.status_code == 200
    assert events == ["open", "commit", "close"]


def test_handled_exception_rolls_back(app: FastAPI, events: list[str]):
    """
    Test that an HTTPException reaches the request-scoped resource, unlike ASGI middleware.

    Given: A FastAPI app whose route class activates the request scope
    When: A route raises HTTPException, which FastAPI's ExceptionMiddleware handles
    Then: The request-scoped resource still sees the exception and rolls back, since
        scoped_route is entered inside FastAPI's own dependency-resolution exit stack
    """
    # Act
    client = TestClient(app)
    response = client.get("/http-exception")

    # Assert
    assert response.status_code == 400
    assert events == ["open", "rollback", "close"]


def test_unhandled_exception_rolls_back(app: FastAPI, events: list[str]):
    """
    Test that a genuinely unhandled exception reaches the request-scoped resource.

    Given: A FastAPI app whose route class activates the request scope
    When: A route raises an exception with no matching handler
    Then: The request-scoped resource sees the exception and rolls back
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/unhandled")

    # Assert
    assert response.status_code == 500
    assert events == ["open", "rollback", "close"]


def test_resource_called_twice_commits_once(app: FastAPI, events: list[str]):
    """
    Test that calling the resource multiple times in one request doesn't double-commit.

    Given: A FastAPI app whose route class activates the request scope
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


def test_resource_called_twice_then_fails_rolls_back_once(app: FastAPI, events: list[str]):
    """
    Test that calling the resource twice before failing still only rolls back once.

    Given: A FastAPI app whose route class activates the request scope
    When: A route calls the request-scoped resource twice (getting the same cached
        connection both times) and then raises
    Then: Only a single open/rollback/close cycle happens for the whole request -
        the resource is not entered or torn down twice just because it was called twice
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/twice-then-fail")

    # Assert
    assert response.status_code == 500
    assert events == ["open", "rollback", "close"]


def test_streaming_response_keeps_scope_open_until_body_completes(app: FastAPI, events: list[str]):
    """
    Test that a StreamingResponse keeps the request scope open for its whole body.

    Given: A FastAPI app whose route class activates the request scope
    When: A route returns a StreamingResponse whose body is produced after the route
        handler itself has already returned
    Then: The request-scoped resource stays open through the entire streamed body,
        and only commits/closes once the last chunk has been sent - not as soon as
        the handler returns the response object
    """
    # Act
    client = TestClient(app)
    response = client.get("/stream")

    # Assert
    assert response.status_code == 200
    assert response.text == "chunk1chunk2"
    assert events == ["open", "stream:connection", "stream-done", "commit", "close"]


def test_streaming_response_resource_still_valid_mid_stream(app: FastAPI, events: list[str]):
    """
    Test that the request-scoped resource is still usable from inside a streamed body.

    Given: A FastAPI app whose route class activates the request scope
    When: A route calls the request-scoped resource once before returning a StreamingResponse,
        then calls it again from inside the body, after the route handler has already returned
    Then: The second call returns the same cached connection as the first - the scope is
        still genuinely active for the whole body, not just deferred at the edges
    """
    # Act
    client = TestClient(app)
    response = client.get("/stream-twice")

    # Assert
    assert response.status_code == 200
    assert response.text == "chunk1"
    assert events == ["open", "same_connection:True", "commit", "close"]


def test_streaming_response_rolls_back_on_body_failure(app: FastAPI, events: list[str]):
    """
    Test that a failure partway through a streamed body still rolls back the resource.

    Given: A FastAPI app whose route class activates the request scope
    When: A route returns a StreamingResponse whose body raises after sending its first chunk
    Then: The request-scoped resource rolls back rather than committing, even though the
        failure happens after the route handler itself already returned successfully
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/stream-then-fail")

    # Assert
    assert events == ["open", "rollback", "close"]


def test_exception_before_resource_use_still_propagates(app: FastAPI, events: list[str]):
    """
    Test that an exception raised before touching the resource still propagates.

    Given: A FastAPI app whose route class activates the request scope
    When: A route raises without ever calling the request-scoped resource, so the scope's
        activation never entered any resource and has nothing to roll back
    Then: The exception still propagates as a 500, and no resource lifecycle events happen
        at all, since the resource was never touched
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/fail-before-resource")

    # Assert
    assert response.status_code == 500
    assert events == []


def test_streaming_response_failure_before_resource_use_still_propagates(
    app: FastAPI, events: list[str]
):
    """
    Test that a mid-stream failure still propagates even when the resource was never touched.

    Given: A FastAPI app whose route class activates the request scope
    When: A route returns a StreamingResponse whose body raises without the handler or the
        body ever calling the request-scoped resource, so the scope's activation has
        nothing to roll back
    Then: The exception still propagates rather than being silently swallowed, and no
        resource lifecycle events happen at all
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/stream-fail-before-resource")

    # Assert
    assert events == []
