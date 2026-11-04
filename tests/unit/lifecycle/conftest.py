"""Shared fixtures for the stratae.lifecycle unit tests."""

import pytest

from stratae.lifecycle import AsyncScope, Scope


@pytest.fixture
def scope():
    """Provide a single shared-isolation, dense-storage Scope."""
    return Scope("application", "shared")


@pytest.fixture
def context_scope():
    """Provide a single context-isolation, dense-storage Scope."""
    return Scope("request", "context")


@pytest.fixture
def async_scope():
    """Provide a single shared-isolation, dense-storage AsyncScope."""
    return AsyncScope("application", "shared")


@pytest.fixture
def async_context_scope():
    """Provide a single context-isolation, dense-storage AsyncScope."""
    return AsyncScope("request", "context")


@pytest.fixture
def threaded_scope():
    """Provide a single context-isolation, dense-storage Scope with threads=True."""
    return Scope("request", "context", threads=True)


@pytest.fixture
def async_threaded_scope():
    """Provide a single context-isolation, dense-storage AsyncScope with threads=True."""
    return AsyncScope("request", "context", threads=True)


@pytest.fixture
def scope_chain():
    """Provide three Scopes chained by requires: application <- session <- request."""
    application = Scope("application", "shared")
    session = Scope("session", "context", requires=application)
    request = Scope("request", "context", requires=session)
    return application, session, request


@pytest.fixture
def async_scope_chain():
    """Provide three AsyncScopes chained by requires: application <- session <- request."""
    application = AsyncScope("application", "shared")
    session = AsyncScope("session", "context", requires=application)
    request = AsyncScope("request", "context", requires=session)
    return application, session, request
