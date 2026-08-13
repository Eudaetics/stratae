"""Test decode_int directly: confirm-only, or coerce a whole-numbered value into an int."""

import pytest

from stratae.serde import decode_int


def test_decode_int_accepts_whole_float():
    """
    A whole-numbered float is accepted for an int target.

    Given: A whole-numbered float.
    When: decode_int is called with that value.
    Then: The value is returned as an int.
    """
    result = decode_int(42.0)

    assert result == 42
    assert isinstance(result, int)


def test_decode_int_accepts_int_string():
    """
    A string cleanly representing an int is accepted for an int target.

    Given: The string "42".
    When: decode_int is called with that value.
    Then: The value is returned as an int.
    """
    result = decode_int("42")

    assert result == 42
    assert isinstance(result, int)


def test_decode_int_accepts_whole_float_string():
    """
    A string representing a whole-numbered float is accepted for an int target.

    Given: The string "42.0".
    When: decode_int is called with that value.
    Then: The value is returned as an int.
    """
    result = decode_int("42.0")

    assert result == 42
    assert isinstance(result, int)


def test_decode_int_rejects_fractional_float():
    """
    A fractional float is rejected for an int target.

    Given: A fractional float.
    When: decode_int is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError, match="whole number"):
        decode_int(42.7)


def test_decode_int_rejects_fractional_float_string():
    """
    A string representing a fractional float is rejected for an int target.

    Given: The string "42.7".
    When: decode_int is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError, match="whole number"):
        decode_int("42.7")


def test_decode_int_rejects_bool():
    """
    A bool is rejected for an int target.

    Given: A bool value.
    When: decode_int is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_int(True)


def test_decode_int_rejects_non_numeric_string():
    """
    A non-numeric string is rejected for an int target.

    Given: The string "hello".
    When: decode_int is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_int("hello")


def test_decode_int_rejects_non_compatible_object():
    """
    `decode_int` raises on a non number and non string.

    Given: A value that is neither a number nor a numeric string.
    When: decode_int is called with that value.
    Then: A TypeError is raised.
    """
    with pytest.raises(TypeError, match="not an int or an int-shaped string"):
        decode_int(object())
