"""Test decode_float directly: confirm-only, or coerce a numeric value into a float."""

import pytest

from stratae.serde import decode_float


def test_decode_float_accepts_int_string():
    """
    A string cleanly representing an int is accepted for a float target.

    Given: The string "42".
    When: decode_float is called with that value.
    Then: The value is returned as a float.
    """
    result = decode_float("42")

    assert result == 42.0
    assert isinstance(result, float)


def test_decode_float_accepts_float_string():
    """
    A string representing a float is accepted for a float target.

    Given: The string "31.5".
    When: decode_float is called with that value.
    Then: The value is returned as a float.
    """
    result = decode_float("31.5")

    assert result == 31.5
    assert isinstance(result, float)


def test_decode_float_rejects_bool():
    """
    A bool is rejected for a float target.

    Given: A bool value.
    When: decode_float is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_float(False)


def test_decode_float_rejects_non_numeric_string():
    """
    A non-numeric string is rejected for a float target.

    Given: The string "hello".
    When: decode_float is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_float("hello")


def test_decode_float_rejects_non_compatible_object():
    """
    A non-bool, non-number, non-string value is rejected for a float target.

    Given: A value that is neither a bool, a number, nor a numeric string.
    When: decode_float is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError, match="not a number or a numeric string"):
        decode_float(object())
