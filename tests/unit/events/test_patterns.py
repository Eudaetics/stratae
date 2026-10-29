"""
Unit tests for the event pattern classes.

This test suite verifies the following behaviors:

Request:
- Stores its schema, and its reply type as `reply`, `emit_returns` and `handler_returns`.
- Derives its name from the schema, or takes an explicit one.
- Replies None when no reply type is given, as a command.
- A payload-less request takes its name, carries NoPayload, and is a signal.
- A payload-less request without a name raises TypeError.

PubSub:
- Stores its schema and derives its name, or takes an explicit one.
- Emits None, and its handlers may return anything.
- A payload-less event takes its name, carries NoPayload, and is a signal.
- A payload-less event without a name raises TypeError.

Immutability:
- Assigning to or deleting an event's attribute raises AttributeError.
- A custom class using Immutable sets its fields with _assign and refuses assignment.
"""

from typing import Any, cast

import pytest

from stratae.events import Immutable, NoPayload, PubSub, Request


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


class _BookFound:
    def __init__(self, title: str) -> None:
        self.title = title


class _FindBook:
    def __init__(self, query: str) -> None:
        self.query = query


# region: Request


def test_request_stores_schema_and_reply() -> None:
    """
    A request event should keep the schema and reply type it was given.

    Given: A payload class and a reply class
    When: A Request is created
    Then: Its schema is the payload class, its reply is what it emits and what its handler
        returns, and it isn't a signal
    """
    # Act
    find_book = Request(_FindBook, reply=_BookFound)

    # Assert
    assert find_book.schema is _FindBook
    assert find_book.reply is _BookFound
    assert find_book.emit_returns is _BookFound
    assert find_book.handler_returns is _BookFound
    assert find_book.is_signal is False


def test_request_derives_name_from_schema() -> None:
    """
    A request event's name should default to its schema's name.

    Given: A payload class
    When: A Request is created without a name
    Then: Its name is the schema's __name__
    """
    # Act
    find_book = Request(_FindBook, reply=_BookFound)

    # Assert
    assert find_book.name == "_FindBook"


def test_request_accepts_explicit_name() -> None:
    """
    A request event should take an explicit name over its schema's.

    Given: A payload class and a name
    When: A Request is created with the name
    Then: Its name is the given one
    """
    # Act
    find_book = Request(_FindBook, reply=_BookFound, name="find_book")

    # Assert
    assert find_book.name == "find_book"


def test_command_replies_none() -> None:
    """
    A request event without a reply type should reply None, as a command.

    Given: A payload class and no reply type
    When: A Request is created
    Then: Its reply type is NoneType
    """
    # Act
    place_order = Request(_OrderPlaced)

    # Assert
    assert place_order.reply is type(None)


def test_payloadless_request_is_signal() -> None:
    """
    A request event without a schema should carry no payload.

    Given: A name and a reply type, but no schema
    When: A Request is created
    Then: It takes the name, carries NoPayload, and is a signal
    """
    # Act
    ping = Request(reply=str, name="ping")

    # Assert
    assert ping.name == "ping"
    assert ping.schema is NoPayload
    assert ping.is_signal is True


def test_payloadless_request_requires_name() -> None:
    """
    A payload-less request event should need a name.

    Given: No schema to derive a name from
    When: A Request is created without a name
    Then: A TypeError is raised, rather than every such event being named "NoPayload"
    """
    # Arrange
    untyped_request = cast(Any, Request)

    # Act / Assert
    with pytest.raises(TypeError, match="needs a name"):
        untyped_request(reply=str)


# endregion
# region: PubSub


def test_pubsub_stores_schema_and_derives_name() -> None:
    """
    A pub/sub event should keep its schema and default its name to the schema's.

    Given: A payload class
    When: A PubSub is created without a name
    Then: Its schema references the class, its name is the class's, and it isn't a signal
    """
    # Act
    order_placed = PubSub(_OrderPlaced)

    # Assert
    assert order_placed.schema is _OrderPlaced
    assert order_placed.name == "_OrderPlaced"
    assert order_placed.is_signal is False


def test_pubsub_emits_none_and_handlers_return_anything() -> None:
    """
    A pub/sub event should emit None and accept any handler return.

    Given: A payload class
    When: A PubSub is created
    Then: It emits NoneType, and its handlers return object
    """
    # Act
    order_placed = PubSub(_OrderPlaced)

    # Assert
    assert order_placed.emit_returns is type(None)
    assert order_placed.handler_returns is object


def test_pubsub_accepts_explicit_name() -> None:
    """
    A pub/sub event should take an explicit name over its schema's.

    Given: A payload class and a name
    When: A PubSub is created with the name
    Then: Its name is the given one
    """
    # Act
    order_placed = PubSub(_OrderPlaced, name="order_placed")

    # Assert
    assert order_placed.name == "order_placed"


def test_payloadless_pubsub_is_signal() -> None:
    """
    A pub/sub event without a schema should carry no payload.

    Given: A name, but no schema
    When: A PubSub is created
    Then: It takes the name, carries NoPayload, and is a signal
    """
    # Act
    heartbeat = PubSub(name="heartbeat")

    # Assert
    assert heartbeat.name == "heartbeat"
    assert heartbeat.schema is NoPayload
    assert heartbeat.is_signal is True


def test_payloadless_pubsub_requires_name() -> None:
    """
    A payload-less pub/sub event should need a name.

    Given: No schema to derive a name from
    When: A PubSub is created without a name
    Then: A TypeError is raised
    """
    # Arrange
    untyped_pubsub = cast(Any, PubSub)

    # Act / Assert
    with pytest.raises(TypeError, match="needs a name"):
        untyped_pubsub()


# endregion
# region: Immutability


@pytest.mark.parametrize(
    "event",
    [Request(_FindBook, reply=_BookFound), PubSub(_OrderPlaced)],
    ids=["request", "pubsub"],
)
def test_assigning_attribute_raises(event: Any) -> None:
    """
    An event's attributes should not be assignable once it's constructed.

    Given: A constructed event
    When: One of its attributes is assigned
    Then: An AttributeError is raised, and the attribute keeps its value
    """
    # Arrange
    name = event.name

    # Act / Assert
    with pytest.raises(AttributeError, match="immutable"):
        event.name = "renamed"
    assert event.name == name


@pytest.mark.parametrize(
    "event",
    [Request(_FindBook, reply=_BookFound), PubSub(_OrderPlaced)],
    ids=["request", "pubsub"],
)
def test_deleting_attribute_raises(event: Any) -> None:
    """
    An event's attributes should not be deletable.

    Given: A constructed event
    When: One of its attributes is deleted
    Then: An AttributeError is raised, and the attribute is still set
    """
    # Act / Assert
    with pytest.raises(AttributeError, match="immutable"):
        del event.schema
    assert event.schema is not None


def test_immutable_custom_class() -> None:
    """
    A custom class using Immutable should set its fields once, then refuse changes.

    Given: A slotted class using Immutable that sets a field with _assign in __init__
    When: It's constructed, then the field is assigned
    Then: The field holds its initial value, and assignment raises AttributeError naming the
        class
    """

    # Arrange
    class _Point(Immutable):
        __slots__ = ("x",)
        x: int

        def __init__(self, x: int) -> None:
            self._assign(x=x)

    point = _Point(3)

    # Act / Assert
    assert point.x == 3
    with pytest.raises(AttributeError, match="_Point is immutable"):
        point.x = 4


# endregion
