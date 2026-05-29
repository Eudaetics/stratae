"""Fixtures for running event tests."""

import pytest

from stratae.events.channel import Channel


@pytest.fixture(autouse=True)
def clear_channel_registry():
    """Clear the Channel registry before and after every test."""
    Channel._registry.clear()  # pyright: ignore[reportPrivateUsage]
    yield
    Channel._registry.clear()  # pyright: ignore[reportPrivateUsage]
