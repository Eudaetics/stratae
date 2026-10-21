"""Smoke tests for the FastAPI lifecycle integration, using a real FastAPI app."""

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from stratae.integrations.fastapi import scoped_route
from stratae.lifecycle import AsyncScope, async_resource
from stratae.lifecycle.exceptions import ScopeInactiveError

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

    @fastapi_app.get("/http-exception-while-handling")
    async def http_exception_while_handling() -> None:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()
        try:
            raise KeyError("missing")
        except KeyError:
            raise HTTPException(status_code=404, detail="deliberate 404 while handling") from None

    @fastapi_app.get("/suspend")
    async def suspend() -> None:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()
        events.append("suspended")
        await asyncio.Event().wait()

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


def test_http_exception_raised_while_handling_another_keeps_its_status(
    app: FastAPI, events: list[str]
):
    """
    An HTTPException raised inside an except block should still produce its own status.

    Given: A FastAPI app whose route class activates the request scope
    When: A route raises HTTPException(404) while handling a KeyError
    Then: The client gets the 404, and the request-scoped resource rolls back
    """
    # Act
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/http-exception-while-handling")

    # Assert
    assert response.status_code == 404
    assert events == ["open", "rollback", "close"]


async def test_cancelled_request_still_closes_resource(
    app: FastAPI, events: list[str], cancel_mid_request: Callable[..., Awaitable[None]]
):
    """
    A request cancelled mid-handler should still close its request-scoped resource.

    Given: A FastAPI app whose route class activates the request scope
    When: The request is cancelled (a client disconnect or timeout) while the handler is suspended
    Then: The request-scoped resource is closed without committing
    """
    # Act
    await cancel_mid_request(app, "/suspend", lambda: "suspended" in events)

    # Assert
    assert "commit" not in events
    assert events[-1] == "close"


def test_route_subclass_can_post_process_the_response(async_request_scope: AsyncScope):
    """
    A route class built on scoped_route can still modify the response its handler returns.

    Given: A route class subclassing scoped_route's, setting a header on every response
    When: A request is handled
    Then: The request succeeds, and the response carries the header
    """

    # Arrange
    class TimedRoute(scoped_route(async_request_scope)):
        def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
            handler = super().get_route_handler()

            async def timed(request: Request) -> Response:
                response = await handler(request)
                response.headers["X-Response-Time"] = "1ms"
                return response

            return timed

    timed_app = FastAPI()
    timed_app.router.route_class = TimedRoute

    @timed_app.get("/ok")
    async def ok() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        return {"status": "ok"}

    # Act
    client = TestClient(timed_app, raise_server_exceptions=False)
    response = client.get("/ok")

    # Assert
    assert response.status_code == 200
    assert response.headers["X-Response-Time"] == "1ms"


@pytest.fixture
def commit_fails() -> dict[str, bool]:
    """Provide a switch the transaction app's commit checks, to make it fail on demand."""
    return {"commit": False}


@pytest.fixture
def transaction_app(
    events: list[str],
    commit_fails: dict[str, bool],
    record_response_start: Callable[..., Any],
) -> Any:
    """
    Build a FastAPI app with a request scope and a transaction scope nested in it.

    The connection is request-scoped, released once the response is fully sent; the
    transaction is scoped to the handler, committing or rolling back when it returns.
    """
    request_scope = AsyncScope("request")
    transaction_scope = AsyncScope("transaction", requires=request_scope)

    @request_scope.cache()
    @async_resource
    async def get_connection():
        events.append("connection-open")
        try:
            yield "connection"
        finally:
            events.append("connection-close")

    @transaction_scope.cache()
    @async_resource
    async def get_transaction():
        await get_connection()
        events.append("begin")
        try:
            yield "transaction"
            if commit_fails["commit"]:
                raise RuntimeError("deliberate commit failure")
            events.append("commit")
        except BaseException:
            events.append("rollback")
            raise

    fastapi_app = FastAPI()
    fastapi_app.router.route_class = scoped_route(request_scope, handler=transaction_scope)

    @fastapi_app.post("/write")
    async def write() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()
        return {"status": "written"}

    @fastapi_app.post("/write-conflict")
    async def write_conflict() -> None:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()
        raise HTTPException(status_code=409, detail="deliberate conflict")

    @fastapi_app.post("/write-then-stream")
    async def write_then_stream() -> StreamingResponse:  # pyright: ignore[reportUnusedFunction]
        await get_transaction()

        async def body():
            events.append(f"stream: {await get_connection()}")
            try:
                await get_transaction()
            except ScopeInactiveError:
                events.append("stream: transaction ended")
            yield b"chunk"

        return StreamingResponse(body())

    @fastapi_app.post("/write-then-background")
    async def write_then_background(  # pyright: ignore[reportUnusedFunction]
        tasks: BackgroundTasks,
    ) -> dict[str, str]:
        await get_transaction()

        async def notify() -> None:
            events.append(f"background: {await get_connection()}")

        tasks.add_task(notify)
        return {"status": "written"}

    return record_response_start(fastapi_app, events)


def test_transaction_commits_before_response_starts(transaction_app: Any, events: list[str]):
    """
    A handler-scoped transaction commits before the response starts.

    Given: A FastAPI app whose route class activates a request scope, with a transaction
        scope nested around the handler
    When: A route using the transaction returns normally
    Then: The transaction commits before the response starts, and the request-scoped
        connection is released only after the response
    """
    # Act
    response = TestClient(transaction_app).post("/write")

    # Assert
    assert response.status_code == 200
    assert events == [
        "connection-open",
        "begin",
        "commit",
        "response-start: 200",
        "connection-close",
    ]


def test_failed_commit_returns_server_error(
    transaction_app: Any, events: list[str], commit_fails: dict[str, bool]
):
    """
    A commit that fails turns the response into a 500, since it runs before the response.

    Given: A FastAPI app with a handler-scoped transaction whose commit fails
    When: A route using the transaction returns normally
    Then: The transaction rolls back, and the client gets a 500 instead of a success. The
        unhandled error leaves the route, so the request scope also ends before the 500,
        which the server-error middleware outside the route sends.
    """
    # Arrange
    commit_fails["commit"] = True

    # Act
    response = TestClient(transaction_app, raise_server_exceptions=False).post("/write")

    # Assert
    assert response.status_code == 500
    assert events == [
        "connection-open",
        "begin",
        "rollback",
        "connection-close",
        "response-start: 500",
    ]


def test_http_exception_rolls_back_transaction_before_response(
    transaction_app: Any, events: list[str]
):
    """
    An HTTPException rolls back the handler-scoped transaction before its response is sent.

    Given: A FastAPI app with a handler-scoped transaction
    When: A route using the transaction raises HTTPException(409)
    Then: The transaction rolls back before the 409 starts, and the connection is released after
    """
    # Act
    response = TestClient(transaction_app).post("/write-conflict")

    # Assert
    assert response.status_code == 409
    assert events == [
        "connection-open",
        "begin",
        "rollback",
        "response-start: 409",
        "connection-close",
    ]


def test_streamed_body_uses_request_resources_after_commit(transaction_app: Any, events: list[str]):
    """
    A streamed body still has the request's resources, but not the ended transaction.

    Given: A FastAPI app with a handler-scoped transaction
    When: A route commits and returns a StreamingResponse whose body uses the connection,
        then reaches for the transaction
    Then: The body gets the connection after the commit, the transaction raises
        ScopeInactiveError there, and the connection is released only after the body
    """
    # Act
    response = TestClient(transaction_app).post("/write-then-stream")

    # Assert
    assert response.status_code == 200
    assert events == [
        "connection-open",
        "begin",
        "commit",
        "response-start: 200",
        "stream: connection",
        "stream: transaction ended",
        "connection-close",
    ]


def test_background_task_uses_request_resources_after_commit(
    transaction_app: Any, events: list[str]
):
    """
    A background task runs after the commit, with the request's resources still open.

    Given: A FastAPI app with a handler-scoped transaction
    When: A route commits and schedules a background task that uses the connection
    Then: The task runs after the commit and the response, and the connection is released
        only after the task
    """
    # Act
    response = TestClient(transaction_app).post("/write-then-background")

    # Assert
    assert response.status_code == 200
    assert events == [
        "connection-open",
        "begin",
        "commit",
        "response-start: 200",
        "background: connection",
        "connection-close",
    ]
