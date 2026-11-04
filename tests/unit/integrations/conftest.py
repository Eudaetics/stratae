"""Pytest fixtures shared by the integration adapters' unit tests."""

import asyncio
from functools import cached_property
from typing import Any
from unittest.mock import AsyncMock

import pytest


class _Channel(AsyncMock):
    """A mock AMQP channel, whose closing future is made on first use, in the running loop."""

    @cached_property
    def closing(self) -> asyncio.Future[Any]:
        """The future that fails when the broker drops the channel."""
        return asyncio.get_running_loop().create_future()


@pytest.fixture
def channel() -> AsyncMock:
    """Return a mock AMQP channel, whose closing future fails when the broker drops it."""
    return _Channel()
