"""Smoke tests for the FastAPI lifecycle integration, using a real FastAPI app."""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from stratae.integrations.lifecycle.fastapi import scoped_route
from stratae.lifecycle import async_resource
from stratae.lifecycle.lifecycle import AsyncLifecycle

pytestmark = pytest.mark.fastapi


@pytest.fixture
def events() -> list[str]:
    """Provide a fresh event log for the app fixture to record open/commit/rollback/close into."""
    return []


@pytest.fixture
def app(async_lifecycle: AsyncLifecycle, events: list[str]) -> FastAPI:
    """Build a FastAPI app whose request scope is activated via scoped_route."""

    @async_lifecycle.cache("request")
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
    fastapi_app.router.route_class = scoped_route(async_lifecycle, "request")

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
