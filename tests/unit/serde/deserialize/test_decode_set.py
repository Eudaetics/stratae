"""Test decode_set directly: confirm a set, or convert one decoded as a list/tuple/frozenset."""

import pytest

from stratae.serde import decode_set


def test_decode_set_confirms_set():
    """
    A set value is confirmed for a bare set target.

    Given: A set value.
    When: decode_set is called with that value.
    Then: The same set is returned.
    """
    assert decode_set({1, 2, 3}) == {1, 2, 3}


def test_decode_set_rejects_incompatible_value():
    """
    A value with no container form is rejected for a bare set target.

    Given: An int value.
    When: decode_set is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_set(42)


def test_decode_set_accepts_tuple_source():
    """
    A tuple-sourced value is accepted for a set target.

    Given: A tuple value.
    When: decode_set is called with that value.
    Then: A set with the same elements is returned.
    """
    assert decode_set((1, 2, 3)) == {1, 2, 3}


def test_decode_set_accepts_frozenset_source():
    """
    A frozenset-sourced value is accepted for a set target.

    Given: A frozenset value.
    When: decode_set is called with that value.
    Then: A set with the same elements is returned.
    """
    assert decode_set(frozenset({1, 2, 3})) == {1, 2, 3}
