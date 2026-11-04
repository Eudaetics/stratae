"""Pytest fixtures providing Scope/AsyncScope objects for integration and e2e tests."""

import pytest

from stratae.lifecycle import AsyncScope, Scope


@pytest.fixture
def application_scope():
    """Provide a shared-isolation, dense-storage application Scope."""
    return Scope("application", "shared")


@pytest.fixture
def request_scope():
    """Provide a context-isolation, dense-storage request Scope."""
    return Scope("request", "context")


@pytest.fixture
def async_application_scope():
    """Provide a shared-isolation, dense-storage application AsyncScope."""
    return AsyncScope("application", "shared")


@pytest.fixture
def async_request_scope():
    """Provide a context-isolation, dense-storage request AsyncScope."""
    return AsyncScope("request", "context")
