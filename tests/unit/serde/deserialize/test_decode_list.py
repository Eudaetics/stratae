"""Test decode_list directly: confirm a list, or convert one decoded as a tuple/set/frozenset."""

import pytest

from stratae.serde import decode_list


def test_decode_list_confirms_list():
    """
    A list value is confirmed for a bare list target.

    Given: A list value.
    When: decode_list is called with that value.
    Then: The same list is returned.
    """
    assert decode_list([1, 2, 3]) == [1, 2, 3]


def test_decode_list_rejects_incompatible_value():
    """
    A value with no container form is rejected for a bare list target.

    Given: An int value.
    When: decode_list is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_list(42)


def test_decode_list_accepts_tuple_source():
    """
    A tuple-sourced value is accepted for a list target.

    Given: A tuple value.
    When: decode_list is called with that value.
    Then: A list with the same elements is returned.
    """
    assert decode_list((1, 2, 3)) == [1, 2, 3]


def test_decode_list_accepts_set_source():
    """
    A set-sourced value is accepted for a list target.

    Given: A set value.
    When: decode_list is called with that value.
    Then: A list with the same elements is returned.
    """
    assert sorted(decode_list({1, 2, 3})) == [1, 2, 3]  # pyright: ignore[reportUnknownArgumentType]


def test_decode_list_accepts_frozenset_source():
    """
    A frozenset-sourced value is accepted for a list target.

    Given: A frozenset value.
    When: decode_list is called with that value.
    Then: A list with the same elements is returned.
    """
    assert sorted(decode_list(frozenset({1, 2, 3}))) == [1, 2, 3]  # pyright: ignore[reportUnknownArgumentType]
