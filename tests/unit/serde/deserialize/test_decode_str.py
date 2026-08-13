"""Test decode_str directly: confirm-only, no coercion, for a bare str target."""

import pytest

from stratae.serde import decode_str


def test_decode_str_rejects_int():
    """
    An int is rejected for a str target.

    Given: An int value.
    When: decode_str is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_str(42)


def test_decode_str_rejects_dict():
    """
    A dict is rejected for a str target.

    Given: A dict value.
    When: decode_str is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_str({"a": 1})
