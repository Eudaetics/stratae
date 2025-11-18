# pyright: reportMissingImports=false
"""ASGI Integration Tests for Stratae Lifecycle Management."""

from unittest.mock import Mock

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

from fastapi import FastAPI
from fastapi.testclient import TestClient

from stratae.depends import inject
from stratae.integrations.asgi import RequestLifecycleMiddleware
from stratae.lifecycle import AsyncLifecycle


@pytest.fixture
def fastapi_app(async_lifecycle: AsyncLifecycle):
    """Create a FastAPI app with RequestLifecycleMiddleware for testing."""
    app = FastAPI()
    app.add_middleware(RequestLifecycleMiddleware, lifecycle=async_lifecycle, scope="request")

    counter = Mock()

    class SampleObject:
        def __init__(self):
            counter()
            self.value = counter.call_count

    @inject
    def get_object() -> SampleObject:
        return SampleObject()

    @async_lifecycle.cache("request")
    @inject
    def get_request_object() -> SampleObject:
        return SampleObject()

    @app.get("/")
    def get_endpoint():  # pyright: ignore[reportUnusedFunction]
        no_lifecycle_1 = get_object()
        no_lifecycle_2 = get_object()
        lifecycle_1 = get_request_object()
        lifecycle_2 = get_request_object()

        return {
            "status": "ok",
            "no_lifecycle_first_value": no_lifecycle_1.value,
            "no_lifecycle_second_value": no_lifecycle_2.value,
            "first_value": lifecycle_1.value,
            "second_value": lifecycle_2.value,
        }

    return app


@pytest.mark.asgi
def test_asgi_stratae_integration(fastapi_app: FastAPI):
    """
    Test that the ASGI integration with Stratae lifecycle works correctly.

    Given: An ASGI application with RequestLifecycleMiddleware
    When: A request is made to the application
    Then: The request is processed within a REQUEST scope lifecycle
    """
    # Act
    with TestClient(fastapi_app) as app_client:
        response = app_client.get("/")

    # Assert
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "ok"
    assert data["no_lifecycle_first_value"] != data["no_lifecycle_second_value"], (
        "Non-lifecycle managed objects should be different"
    )
    assert data["first_value"] == data["second_value"], (
        "Lifecycle managed objects should be the same within REQUEST scope"
    )
