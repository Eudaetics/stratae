"""End-to-end tests for core dependency injection with lifecycle management."""

import inspect
from random import randint
from typing import Annotated, Any

from stratae.context import Context
from stratae.depends import Depends, inject
from stratae.lifecycle import Scope


def test_api_request_processing_with_dependency_injection(
    application_scope: Scope, request_scope: Scope
):
    """
    End-to-end test simulating a web API processing user requests.

    - Application scope: Database pool, app config (shared across all requests)
    - Request scope: User session, request handler (unique per request)
    - Context: User preferences injected into the request pipeline
    """
    # Arrange: Context for user-specific settings
    user_id = Context[int]("user_id")

    @inject
    def get_user_preferences(uid: Annotated[int, Depends(user_id)]) -> dict[str, Any]:
        """Fetch user preferences from a mock database."""
        return {"theme": "dark", "language": "en", "notifications_enabled": True, "user_id": uid}

    def create_database_connection() -> str:
        """Simulate creating a database connection (expensive operation)."""
        connection_id = f"db_conn_{randint(1000, 9999)}"
        return connection_id

    @application_scope.cache()
    @inject
    def initialize_connection_pool(
        db_conn: Annotated[str, Depends(create_database_connection)],
    ) -> dict[str, Any]:
        """Initialize connection pool at application startup (shared across requests)."""
        pool_size = randint(5, 20)
        return {"connection": db_conn, "pool_size": pool_size, "max_connections": pool_size * 10}

    @request_scope.cache()
    @inject
    def authenticate_user(
        pool: Annotated[dict[str, Any], Depends(initialize_connection_pool)],
        uid: Annotated[int, Depends(user_id)],
    ) -> dict[str, Any]:
        """Authenticate user for this specific request."""
        return {
            "user_id": uid,
            "token": f"token_{uid}_{hash(uid) % 100000}",
            "authenticated": True,
            "pool_connection": pool["connection"],
        }

    @request_scope.cache()
    @inject
    def process_api_request(
        pool: Annotated[dict[str, Any], Depends(initialize_connection_pool)],
        auth: Annotated[dict[str, Any], Depends(authenticate_user)],
        prefs: Annotated[dict[str, Any], Depends(get_user_preferences)],
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
    with application_scope.activate():
        user_id.set(42)
        pool = initialize_connection_pool()
        with request_scope.activate():
            print(inspect.signature(process_api_request))
            response1 = process_api_request()
            assert response1["status"] == "success"
            assert response1["user_id"] == 42
            assert response1["preferences"]["user_id"] == 42

            assert process_api_request() is response1
            assert initialize_connection_pool() is pool

        user_id.set(99)
        with request_scope.activate():
            response2 = process_api_request()
            assert response2["status"] == "success"
            assert response2["user_id"] == 99
            assert response2["preferences"]["user_id"] == 99

            assert response2["pool_size"] is response1["pool_size"]
            assert process_api_request() is response2
            assert initialize_connection_pool() is pool

        assert response2 is not response1
        assert initialize_connection_pool() is pool
