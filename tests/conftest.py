"""Pytest configuration for tests using a lifecycle manager."""

from typing import Sequence

import pytest

from stratae.lifecycle import AsyncLifecycle, Lifecycle, Scope


@pytest.fixture
def scopes():
    """Provide a list of lifecycle scope names for testing."""
    yield ["application", "session", "request"]


@pytest.fixture
def lifecycle(scopes: Sequence[str]):
    """Provide a Lifecycle instance with shared-isolation scopes for testing."""
    yield Lifecycle([Scope(name, "shared") for name in scopes])


@pytest.fixture
async def async_lifecycle(scopes: Sequence[str]):
    """Provide an AsyncLifecycle instance with context-isolation scopes for testing."""
    yield AsyncLifecycle([Scope(name, "context") for name in scopes])
