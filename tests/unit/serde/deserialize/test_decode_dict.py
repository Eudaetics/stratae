"""Test decode_dict directly: confirm-only, for a bare, unparameterized dict target."""

import pytest

from stratae.serde import decode_dict


def test_decode_dict_confirms_dict():
    """
    A dict value is confirmed for a bare dict target.

    Given: A dict value.
    When: decode_dict is called with that value.
    Then: The same dict is returned.
    """
    value = {"a": 1, "b": 2}

    assert decode_dict(value) == value


def test_decode_dict_rejects_non_dict():
    """
    A non-dict value is rejected for a bare dict target.

    Given: An int value.
    When: decode_dict is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_dict(42)
