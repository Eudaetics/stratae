"""Smoke tests for the Starlette lifecycle integration, using a real Starlette app."""

import asyncio
import threading
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.endpoints import HTTPEndpoint
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.testclient import TestClient

from stratae.integrations.starlette import scoped_route
from stratae.lifecycle import AsyncScope, async_resource, resource
from stratae.lifecycle.exceptions import ScopeInactiveError

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

    async def http_exception_while_handling(request: Request) -> JSONResponse:
        await get_transaction()
        try:
            raise KeyError("missing")
        except KeyError:
            raise HTTPException(status_code=404, detail="deliberate 404 while handling") from None

    async def suspend(request: Request) -> JSONResponse:
        await get_transaction()
        events.append("suspended")
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

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
            Route("/http-exception-while-handling", http_exception_while_handling),
            Route("/suspend", suspend),
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


def test_http_exception_raised_while_handling_another_keeps_its_status(
    app: Starlette, events: list[str]
):
    """
    An HTTPException raised inside an except block should still produce its own status.

    Given: A Starlette app whose routes activate the request scope via scoped_route
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
    app: Starlette, events: list[str], cancel_mid_request: Callable[..., Awaitable[None]]
):
    """
    A request cancelled mid-handler should still close its request-scoped resource.

    Given: A Starlette app whose routes activate the request scope via scoped_route
    When: The request is cancelled (a client disconnect or timeout) while the handler is suspended
    Then: The request-scoped resource is closed without committing
    """
    # Act
    await cancel_mid_request(app, "/suspend", lambda: "suspended" in events)

    # Assert
    assert "commit" not in events
    assert events[-1] == "close"


def test_route_keeps_endpoint_name_for_url_lookup(async_request_scope: AsyncScope):
    """
    A scoped route is named after its endpoint, so URL lookups by name still work.

    Given: A Starlette app with a scoped route for an endpoint named read_product
    When: A URL is looked up by that endpoint's name
    Then: The route's path is returned
    """

    # Arrange
    async def read_product(request: Request) -> JSONResponse:
        return JSONResponse({})

    Route = scoped_route(async_request_scope)
    named_app = Starlette(routes=[Route("/products/{product_id}", read_product)])

    # Act
    path = named_app.url_path_for("read_product", product_id="1")

    # Assert
    assert path == "/products/1"


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
    Build a Starlette app with a request scope and a transaction scope nested in it.

    The connection is request-scoped, released once the response is fully sent; the
    transaction is scoped to the endpoint, committing or rolling back when it returns.
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

    async def write(request: Request) -> JSONResponse:
        await get_transaction()
        return JSONResponse({"status": "written"})

    async def write_conflict(request: Request) -> JSONResponse:
        await get_transaction()
        raise HTTPException(status_code=409, detail="deliberate conflict")

    async def write_then_stream(request: Request) -> StreamingResponse:
        await get_transaction()

        async def body():
            events.append(f"stream: {await get_connection()}")
            try:
                await get_transaction()
            except ScopeInactiveError:
                events.append("stream: transaction ended")
            yield b"chunk"

        return StreamingResponse(body())

    async def write_then_background(request: Request) -> JSONResponse:
        await get_transaction()

        async def notify() -> None:
            events.append(f"background: {await get_connection()}")

        return JSONResponse({"status": "written"}, background=BackgroundTask(notify))

    Route = scoped_route(request_scope, handler=transaction_scope)
    starlette_app = Starlette(
        routes=[
            Route("/write", write, methods=["POST"]),
            Route("/write-conflict", write_conflict, methods=["POST"]),
            Route("/write-then-stream", write_then_stream, methods=["POST"]),
            Route("/write-then-background", write_then_background, methods=["POST"]),
        ]
    )
    return record_response_start(starlette_app, events)


def test_transaction_commits_before_response_starts(transaction_app: Any, events: list[str]):
    """
    An endpoint-scoped transaction commits before the response starts.

    Given: A Starlette app whose routes activate a request scope, with a transaction scope
        nested around the endpoint
    When: An endpoint using the transaction returns normally
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

    Given: A Starlette app with an endpoint-scoped transaction whose commit fails
    When: An endpoint using the transaction returns normally
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
    An HTTPException rolls back the endpoint-scoped transaction before its response is sent.

    Given: A Starlette app with an endpoint-scoped transaction
    When: An endpoint using the transaction raises HTTPException(409)
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

    Given: A Starlette app with an endpoint-scoped transaction
    When: An endpoint commits and returns a StreamingResponse whose body uses the connection,
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

    Given: A Starlette app with an endpoint-scoped transaction
    When: An endpoint commits and attaches a background task that uses the connection
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


@pytest.fixture
def sync_app(events: list[str], record_response_start: Callable[..., Any]) -> Any:
    """
    Build a Starlette app of sync endpoints, with a request scope and a nested transaction scope.

    Both resources are sync, since a sync endpoint can't await. The connection is
    request-scoped; the transaction is scoped to the endpoint on the routes built with
    `handler=`.
    """
    request_scope = AsyncScope("request")
    transaction_scope = AsyncScope("transaction", requires=request_scope)

    @request_scope.cache()
    @resource
    def get_connection():
        events.append("connection-open")
        try:
            yield "connection"
        except Exception:
            events.append("connection-rollback")
            raise
        finally:
            events.append("connection-close")

    @transaction_scope.cache()
    @resource
    def get_transaction():
        get_connection()
        events.append("begin")
        try:
            yield "transaction"
            events.append("commit")
        except BaseException:
            events.append("rollback")
            raise

    def read(request: Request) -> JSONResponse:
        connection = get_connection()
        on_worker = threading.current_thread() is not threading.main_thread()
        return JSONResponse({"connection": connection, "on_worker_thread": on_worker})

    def conflict(request: Request) -> JSONResponse:
        get_connection()
        raise HTTPException(status_code=409, detail="deliberate conflict")

    def write(request: Request) -> JSONResponse:
        get_transaction()
        return JSONResponse({"status": "written"})

    Route = scoped_route(request_scope)
    TransactionRoute = scoped_route(request_scope, handler=transaction_scope)
    starlette_app = Starlette(
        routes=[
            Route("/read", read),
            Route("/conflict", conflict),
            TransactionRoute("/write", write, methods=["POST"]),
        ]
    )
    return record_response_start(starlette_app, events)


def test_sync_endpoint_runs_on_worker_thread_with_request_scope(sync_app: Any, events: list[str]):
    """
    A sync endpoint runs in Starlette's threadpool, with the request scope still visible.

    Given: A Starlette app whose scoped route wraps a sync endpoint using a request-scoped
        sync resource
    When: The endpoint is requested
    Then: It runs on a worker thread, as Starlette runs sync endpoints, gets the resource,
        and the resource is released after the response
    """
    # Act
    response = TestClient(sync_app).get("/read")

    # Assert
    assert response.status_code == 200
    assert response.json() == {"connection": "connection", "on_worker_thread": True}
    assert events == ["connection-open", "response-start: 200", "connection-close"]


def test_sync_endpoint_http_exception_rolls_back_request_scope(sync_app: Any, events: list[str]):
    """
    An HTTPException from a sync endpoint still rolls back the request-scoped resource.

    Given: A Starlette app whose scoped route wraps a sync endpoint using a request-scoped
        sync resource
    When: The endpoint raises HTTPException(409)
    Then: The client gets the 409, and the resource sees the exception before the response
    """
    # Act
    response = TestClient(sync_app).get("/conflict")

    # Assert
    assert response.status_code == 409
    assert events == [
        "connection-open",
        "connection-rollback",
        "connection-close",
        "response-start: 409",
    ]


def test_sync_endpoint_transaction_commits_before_response(sync_app: Any, events: list[str]):
    """
    A sync endpoint's handler-scoped transaction commits before the response starts.

    Given: A Starlette app whose scoped route, built with a handler scope, wraps a sync
        endpoint using a handler-scoped sync transaction
    When: The endpoint returns normally
    Then: The transaction commits before the response starts, and the request-scoped
        connection is released after it
    """
    # Act
    response = TestClient(sync_app).post("/write")

    # Assert
    assert response.status_code == 200
    assert events == [
        "connection-open",
        "begin",
        "commit",
        "response-start: 200",
        "connection-close",
    ]


def test_sync_route_keeps_endpoint_name_for_url_lookup(async_request_scope: AsyncScope):
    """
    A scoped route for a sync endpoint is named after it, so URL lookups by name work.

    Given: A Starlette app with a scoped route for a sync endpoint named read_product
    When: A URL is looked up by that endpoint's name
    Then: The route's path is returned
    """

    # Arrange
    def read_product(request: Request) -> JSONResponse:
        return JSONResponse({})

    Route = scoped_route(async_request_scope)
    named_app = Starlette(routes=[Route("/products/{product_id}", read_product)])

    # Act
    path = named_app.url_path_for("read_product", product_id="1")

    # Assert
    assert path == "/products/1"


def test_partial_sync_endpoint_runs_on_worker_thread(
    async_request_scope: AsyncScope, events: list[str]
):
    """
    A sync endpoint wrapped in functools.partial is still recognised as sync.

    Given: A Starlette app whose scoped route wraps a partial of a sync endpoint function
    When: The endpoint is requested
    Then: It runs on a worker thread, with the partial's bound argument applied
    """

    # Arrange
    def greet(greeting: str, request: Request) -> JSONResponse:
        on_worker = threading.current_thread() is not threading.main_thread()
        return JSONResponse({"greeting": greeting, "on_worker_thread": on_worker})

    Route = scoped_route(async_request_scope)
    partial_app = Starlette(routes=[Route("/greet", partial(greet, "hello"))])

    # Act
    response = TestClient(partial_app).get("/greet")

    # Assert
    assert response.status_code == 200
    assert response.json() == {"greeting": "hello", "on_worker_thread": True}


def test_class_based_endpoint_is_rejected_when_route_is_built(async_request_scope: AsyncScope):
    """
    A class-based endpoint fails clearly when its scoped route is built, not on first request.

    Given: An HTTPEndpoint subclass, which Starlette runs as an ASGI app rather than as a
        request handler
    When: A scoped route is built for it
    Then: A TypeError explains that scoped routes take function or method endpoints
    """

    # Arrange
    class ProductEndpoint(HTTPEndpoint):
        async def get(self, request: Request) -> JSONResponse:
            return JSONResponse({})

    Route = scoped_route(async_request_scope)

    # Act & Assert
    with pytest.raises(TypeError, match="function or method endpoint"):
        Route("/products", ProductEndpoint)


def test_asgi_app_endpoint_is_rejected_when_route_is_built(async_request_scope: AsyncScope):
    """
    A raw ASGI app endpoint fails clearly when its scoped route is built.

    Given: A callable object implementing the ASGI interface
    When: A scoped route is built for it
    Then: A TypeError explains that scoped routes take function or method endpoints
    """

    # Arrange
    class App:
        async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
            pass

    Route = scoped_route(async_request_scope)

    # Act & Assert
    with pytest.raises(TypeError, match="function or method endpoint"):
        Route("/app", App())


def test_method_endpoint_is_accepted(async_request_scope: AsyncScope):
    """
    A bound method endpoint works like a function endpoint, as it does in Starlette.

    Given: A Starlette app whose scoped route wraps a bound async method
    When: The endpoint is requested
    Then: The method handles the request
    """

    # Arrange
    class Products:
        async def read(self, request: Request) -> JSONResponse:
            return JSONResponse({"handled_by": type(self).__name__})

    Route = scoped_route(async_request_scope)
    method_app = Starlette(routes=[Route("/products", Products().read)])

    # Act
    response = TestClient(method_app).get("/products")

    # Assert
    assert response.status_code == 200
    assert response.json() == {"handled_by": "Products"}
