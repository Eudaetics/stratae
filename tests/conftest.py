"""Pytest configuration for tests using a lifecycle manager."""

from typing import Sequence

import pytest

from stratae.lifecycle import AsyncLifecycle, Lifecycle


@pytest.fixture
def scopes():
    """Provide a list of lifecycle scopes for testing."""
    yield ["application", "session", "request"]


@pytest.fixture
def lifecycle(scopes: Sequence[str]):
    """Provide a Lifecycle instance for testing."""
    yield Lifecycle(scopes)


@pytest.fixture
async def async_lifecycle(scopes: Sequence[str]):
    """Provide an AsyncLifecycle instance for testing."""
    yield AsyncLifecycle(scopes)
