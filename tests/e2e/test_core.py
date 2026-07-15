"""End-to-end tests for core dependency injection with lifecycle management."""

import inspect
from random import randint
from typing import Any

import pytest

from stratae.context import Context
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import Lifecycle, Scope


@pytest.fixture
def lifecycle():
    """Provide a Lifecycle instance for testing."""
    scopes = ["application", "session", "request"]
    yield Lifecycle([Scope(name, "shared") for name in scopes])


def test_api_request_processing_with_dependency_injection(lifecycle: Lifecycle):
    """
    End-to-end test simulating a web API processing user requests.

    - Application scope: Database pool, app config (shared across all requests)
    - Request scope: User session, request handler (unique per request)
    - Context: User preferences injected into the request pipeline
    """
    # Arrange: Context for user-specific settings
    user_id = Context[int]("user_id")

    @inject
    def get_user_preferences(uid: Injected[int, Depends(user_id)]) -> dict[str, Any]:
        """Fetch user preferences from a mock database."""
        return {"theme": "dark", "language": "en", "notifications_enabled": True, "user_id": uid}

    def create_database_connection() -> str:
        """Simulate creating a database connection (expensive operation)."""
        connection_id = f"db_conn_{randint(1000, 9999)}"
        return connection_id

    @lifecycle.cache("application")
    @inject
    def initialize_connection_pool(
        db_conn: Injected[str, Depends(create_database_connection)],
    ) -> dict[str, Any]:
        """Initialize connection pool at application startup (shared across requests)."""
        pool_size = randint(5, 20)
        return {"connection": db_conn, "pool_size": pool_size, "max_connections": pool_size * 10}

    @lifecycle.cache("request")
    @inject
    def authenticate_user(
        pool: Injected[dict[str, Any], Depends(initialize_connection_pool)],
        uid: Injected[int, Depends(user_id)],
    ) -> dict[str, Any]:
        """Authenticate user for this specific request."""
        return {
            "user_id": uid,
            "token": f"token_{uid}_{hash(uid) % 100000}",
            "authenticated": True,
            "pool_connection": pool["connection"],
        }

    @lifecycle.cache("request")
    @inject
    def process_api_request(
        pool: Injected[dict[str, Any], Depends(initialize_connection_pool)],
        auth: Injected[dict[str, Any], Depends(authenticate_user)],
        prefs: Injected[dict[str, Any], Depends(get_user_preferences)],
    ) -> dict[str, Any]:
        """Process the API request with all dependencies injected."""
        response = {
            "status": "success",
            "user_id": auth["user_id"],
            "preferences": prefs,
            "pool_size": pool["pool_size"],
            "data": f"Response for {auth['token']}",
        }
        return response

    # Act & Assert: Simulate multiple requests in an application lifecycle
    with lifecycle.start("application"):
        user_id.set(42)
        pool = initialize_connection_pool()
        with lifecycle.start("request"):
            print(inspect.signature(process_api_request))
            response1 = process_api_request()
            assert response1["status"] == "success"
            assert response1["user_id"] == 42
            assert response1["preferences"]["user_id"] == 42

            assert process_api_request() is response1
            assert initialize_connection_pool() is pool

        user_id.set(99)
        with lifecycle.start("request"):
            response2 = process_api_request()
            assert response2["status"] == "success"
            assert response2["user_id"] == 99
            assert response2["preferences"]["user_id"] == 99

            assert response2["pool_size"] is response1["pool_size"]
            assert process_api_request() is response2
            assert initialize_connection_pool() is pool

        assert response2 is not response1
        assert initialize_connection_pool() is pool
