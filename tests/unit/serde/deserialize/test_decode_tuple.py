"""Test decode_tuple directly: confirm a tuple, or convert one decoded as a list/set/frozenset."""

import pytest

from stratae.serde import decode_tuple


def test_decode_tuple_confirms_tuple():
    """
    A tuple value is confirmed for a bare tuple target.

    Given: A tuple value.
    When: decode_tuple is called with that value.
    Then: The same tuple is returned.
    """
    assert decode_tuple((1, 2, 3)) == (1, 2, 3)


def test_decode_tuple_rejects_incompatible_value():
    """
    A value with no container form is rejected for a bare tuple target.

    Given: An int value.
    When: decode_tuple is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_tuple(42)


def test_decode_tuple_accepts_list_source():
    """
    A list-sourced value is accepted for a tuple target.

    Given: A list value.
    When: decode_tuple is called with that value.
    Then: A tuple with the same elements is returned.
    """
    assert decode_tuple([1, 2, 3]) == (1, 2, 3)


def test_decode_tuple_accepts_set_source():
    """
    A set-sourced value is accepted for a tuple target.

    Given: A set value.
    When: decode_tuple is called with that value.
    Then: A tuple with the same elements is returned.
    """
    assert sorted(decode_tuple({1, 2, 3})) == [1, 2, 3]  # pyright: ignore[reportUnknownArgumentType]


def test_decode_tuple_accepts_frozenset_source():
    """
    A frozenset-sourced value is accepted for a tuple target.

    Given: A frozenset value.
    When: decode_tuple is called with that value.
    Then: A tuple with the same elements is returned.
    """
    assert sorted(decode_tuple(frozenset({1, 2, 3}))) == [1, 2, 3]  # pyright: ignore[reportUnknownArgumentType]
