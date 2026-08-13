"""Test decode_frozenset directly: confirm a frozenset, or convert a list/tuple/set."""

import pytest

from stratae.serde import decode_frozenset


def test_decode_frozenset_confirms_frozenset():
    """
    A frozenset value is confirmed for a bare frozenset target.

    Given: A frozenset value.
    When: decode_frozenset is called with that value.
    Then: The same frozenset is returned.
    """
    assert decode_frozenset(frozenset({1, 2, 3})) == frozenset({1, 2, 3})


def test_decode_frozenset_rejects_incompatible_value():
    """
    A value with no container form is rejected for a bare frozenset target.

    Given: An int value.
    When: decode_frozenset is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_frozenset(42)


def test_decode_frozenset_accepts_list_source():
    """
    A list-sourced value is accepted for a frozenset target.

    Given: A list value.
    When: decode_frozenset is called with that value.
    Then: A frozenset with the same elements is returned.
    """
    assert decode_frozenset([1, 2, 3]) == frozenset({1, 2, 3})


def test_decode_frozenset_accepts_tuple_source():
    """
    A tuple-sourced value is accepted for a frozenset target.

    Given: A tuple value.
    When: decode_frozenset is called with that value.
    Then: A frozenset with the same elements is returned.
    """
    assert decode_frozenset((1, 2, 3)) == frozenset({1, 2, 3})


def test_decode_frozenset_accepts_set_source():
    """
    A set-sourced value is accepted for a frozenset target.

    Given: A set value.
    When: decode_frozenset is called with that value.
    Then: A frozenset with the same elements is returned.
    """
    assert decode_frozenset({1, 2, 3}) == frozenset({1, 2, 3})
