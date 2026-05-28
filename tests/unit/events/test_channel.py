"""
Unit tests for the Channel class.

This test suite verifies the following behaviours:
- name is stored on initialisation.
- metadata is an empty dict when no kwargs are supplied.
- metadata contains all kwargs when supplied.
- Creating a second channel with the same name raises ValueError.
- A Channel instance is hashable for use as an adapter registry key.
"""

import pytest

from stratae.events.channel import Channel


@pytest.fixture(autouse=True)
def clear_channel_registry():
    """Clear the Channel registry before and after every test."""
    Channel._registry.clear()  # pyright: ignore[reportPrivateUsage]
    yield
    Channel._registry.clear()  # pyright: ignore[reportPrivateUsage]


def test_init_stores_name():
    """
    Name should be stored on the channel at initialisation.

    Given: A routing name
    When: A Channel is created
    Then: channel.name should equal the supplied name string
    """
    # Arrange & Act
    channel = Channel("orders")

    # Assert
    assert channel.name == "orders"


def test_metadata_is_empty_when_no_kwargs():
    """
    Metadata should be an empty dict when no keyword arguments are supplied.

    Given: A routing name with no extra kwargs
    When: A Channel is created
    Then: channel.metadata should be an empty dict
    """
    # Arrange & Act
    channel = Channel("orders")

    # Assert
    assert channel.metadata == {}


def test_metadata_stores_all_kwargs():
    """
    Metadata should contain all keyword arguments supplied at initialisation.

    Given: A routing name and several metadata kwargs
    When: A Channel is created
    Then: channel.metadata should contain exactly those kwargs
    """
    # Arrange & Act
    channel = Channel("orders", version=1, priority="low")

    # Assert
    assert channel.metadata == {"version": 1, "priority": "low"}


def test_duplicate_name_raises():
    """
    Creating a second channel with the same name should raise ValueError.

    Given: A Channel already registered under a given name
    When: A second Channel is created with the same name
    Then: A ValueError should be raised
    """
    # Arrange
    Channel("orders")

    # Act & Assert
    with pytest.raises(ValueError, match="orders"):
        Channel("orders")


def test_channel_is_hashable():
    """
    Channel must be hashable for use as an adapter handler registry key.

    Given: A Channel instance
    When: It is added to a set and looked up
    Then: It should be found — confirming it is hashable and comparable by identity
    """
    # Arrange & Act
    channel = Channel("orders")

    # Assert
    assert channel in {channel}
