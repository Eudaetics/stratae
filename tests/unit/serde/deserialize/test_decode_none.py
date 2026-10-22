"""Test decode_none directly: confirm-only, registered under both NoneType and bare None."""

import pytest

from stratae.serde import decode_none


def test_decode_none_confirms_none():
    """
    None is confirmed for a NoneType/None target.

    Given: The value None.
    When: decode_none is called with that value.
    Then: None is returned.
    """
    assert decode_none(None) is None


def test_decode_none_rejects_non_none():
    """
    A non-null value is rejected for a NoneType/None target.

    Given: An int value.
    When: decode_none is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_none(42)
