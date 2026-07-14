"""Pytest configuration for tests using a lifecycle manager."""

from typing import Sequence

import pytest

from stratae.lifecycle import Lifecycle, Scope
from stratae.lifecycle.lifecycle import AsyncLifecycle


@pytest.fixture
def scopes():
    """Provide a list of lifecycle scope names for testing."""
    yield ["application", "session", "request"]


@pytest.fixture
def scope_objs():
    """Provide a list of scope objects."""
    yield [Scope("application", "shared"), Scope("session", "context"), Scope("request", "context")]


@pytest.fixture
def lifecycle(scope_objs: Sequence[Scope]):
    """Provide a Lifecycle instance with shared-isolation scopes for testing."""
    yield Lifecycle(scope_objs)


@pytest.fixture
async def async_lifecycle(scope_objs: Sequence[Scope]):
    """Provide an AsyncLifecycle instance with context-isolation scopes for testing."""
    yield AsyncLifecycle(scope_objs)
