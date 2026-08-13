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

    @fastapi_app.get("/swallowed-exception")
    async def swallowed_exception_route() -> None:  # pyright: ignore[reportUnusedFunction]
        await get_swallowing_transaction()
        raise ValueError("deliberate failure swallowed by resource cleanup")

    @fastapi_app.get("/stream-swallowed-exception")
    async def stream_swallowed_exception() -> StreamingResponse:  # pyright: ignore[reportUnusedFunction]
        await get_swallowing_transaction()

        async def body():
            yield b"chunk1"
            raise ValueError("deliberate mid-stream failure swallowed by resource cleanup")

        return StreamingResponse(body())

    return fastapi_app


def test_successful_request_commits(app: FastAPI, events: list[str]):
    """
    A request scope activated via scoped_route should commit on success.

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
    HTTPException should still roll back the request-scoped resource.

    Given: A FastAPI app whose route class activates the request scope
    When: A route raises HTTPException
    Then: The request-scoped resource still sees the exception and rolls back
    """
    # Act
    client = TestClient(app)
    response = client.get("/http-exception")

    # Assert
    assert response.status_code == 400
    assert events == ["open", "rollback", "close"]


def test_unhandled_exception_rolls_back(app: FastAPI, events: list[str]):
    """
    An unhandled exception should still roll back the request-scoped resource.

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
    Calling the resource multiple times in one request shouldn't double-commit.

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
    Calling the resource twice before failing should still only roll back once.

    Given: A FastAPI app whose route class activates the request scope
    When: A route calls the request-scoped resource twice and then raises
    Then: Only a single open/rollback/close cycle happens for the whole request
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/twice-then-fail")

    # Assert
    assert response.status_code == 500
    assert events == ["open", "rollback", "close"]


def test_streaming_response_keeps_scope_open_until_body_completes(app: FastAPI, events: list[str]):
    """
    A StreamingResponse should keep the request scope open for its whole body.

    Given: A FastAPI app whose route class activates the request scope
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


def test_streaming_response_resource_still_valid_mid_stream(app: FastAPI, events: list[str]):
    """
    The request-scoped resource should still be usable from inside a streamed body.

    Given: A FastAPI app whose route class activates the request scope
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


def test_streaming_response_rolls_back_on_body_failure(app: FastAPI, events: list[str]):
    """
    A failure partway through a streamed body should still roll back the resource.

    Given: A FastAPI app whose route class activates the request scope
    When: A route returns a StreamingResponse whose body raises after sending its first chunk
    Then: The request-scoped resource rolls back rather than committing
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/stream-then-fail")

    # Assert
    assert events == ["open", "rollback", "close"]


def test_exception_before_resource_use_still_propagates(app: FastAPI, events: list[str]):
    """
    An exception raised before touching the resource should still propagate.

    Given: A FastAPI app whose route class activates the request scope
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
    app: FastAPI, events: list[str]
):
    """
    A mid-stream failure should still propagate even when the resource was never touched.

    Given: A FastAPI app whose route class activates the request scope
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
    app: FastAPI, events: list[str]
):
    """
    A route exception should still reach the client even when a resource swallows it.

    Given: A FastAPI app whose request-scoped resource catches and suppresses the
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
    app: FastAPI, events: list[str]
):
    """
    Mid-stream exceptions should still surface even when a resource swallows it.

    Given: A FastAPI app whose request-scoped resource catches and suppresses the
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
