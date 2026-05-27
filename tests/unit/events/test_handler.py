"""
Unit tests for the Handler class.

This test suite verifies the following behaviors:

Handler:
- is_async is False for a synchronous callable.
- is_async is True for an asynchronous callable.
- call stores the wrapped callable.
- Two Handlers wrapping the same callable are equal.
- Two Handlers wrapping different callables are not equal.
- A Handler is not equal to a non-Handler object.
- Two Handlers wrapping the same callable have the same hash.
"""

from stratae.events.event import Event
from stratae.events.handler import Handler


def _sync_handler(event: Event) -> None:
    """Define a simple sync handler for testing."""
    pass


async def _async_handler(event: Event) -> None:
    """Define a simple async handler for testing."""
    pass


def test_is_async_is_false_for_sync_callable():
    """
    is_async should be False for a synchronous callable.

    Given: A synchronous callable
    When: A Handler is constructed with it
    Then: is_async should be False
    """
    handler = Handler(_sync_handler)

    assert handler.is_async is False


def test_is_async_is_true_for_async_callable():
    """
    is_async should be True for an asynchronous callable.

    Given: An async callable
    When: A Handler is constructed with it
    Then: is_async should be True
    """
    handler = Handler(_async_handler)

    assert handler.is_async is True


def test_call_stores_wrapped_callable():
    """
    Call should reference the callable passed at construction.

    Given: A callable
    When: A Handler is constructed with it
    Then: call should be that callable
    """
    handler = Handler(_sync_handler)

    assert handler.call is _sync_handler


def test_two_handlers_wrapping_same_callable_are_equal():
    """
    Two Handlers wrapping the same callable should be equal.

    Given: Two Handler instances constructed with the same callable
    When: They are compared with ==
    Then: They should be equal
    """
    assert Handler(_sync_handler) == Handler(_sync_handler)


def test_two_handlers_wrapping_different_callables_are_not_equal():
    """
    Two Handlers wrapping different callables should not be equal.

    Given: Two Handler instances constructed with different callables
    When: They are compared with ==
    Then: They should not be equal
    """
    assert Handler(_sync_handler) != Handler(_async_handler)


def test_handler_is_not_equal_to_non_handler():
    """
    A Handler should not be equal to a non-Handler object.

    Given: A Handler instance and an arbitrary non-Handler object
    When: They are compared with ==
    Then: They should not be equal
    """
    assert Handler(_sync_handler) != _sync_handler


def test_two_handlers_wrapping_same_callable_have_same_hash():
    """
    Two Handlers wrapping the same callable should have the same hash.

    Given: Two Handler instances constructed with the same callable
    When: Their hashes are compared
    Then: The hashes should be equal
    """
    assert hash(Handler(_sync_handler)) == hash(Handler(_sync_handler))
