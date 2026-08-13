"""Test decode_decimal directly: confirm a native Decimal, or parse its string form."""

from decimal import Decimal

import pytest

from stratae.serde import decode_decimal


def test_decode_decimal_parses_string_form():
    """
    A Decimal's string form is parsed back into a Decimal.

    Given: A Decimal's string form.
    When: decode_decimal is called with that value.
    Then: A Decimal equal to the original is returned.
    """
    value = Decimal("19.99")

    result = decode_decimal(str(value))

    assert result == value


def test_decode_decimal_accepts_native_instance():
    """
    An already-native Decimal instance is accepted for a Decimal target.

    Given: A Decimal instance, not a string.
    When: decode_decimal is called with that value.
    Then: The same Decimal is returned.
    """
    value = Decimal("19.99")

    result = decode_decimal(value)

    assert result == value


def test_decode_decimal_rejects_float():
    """
    A raw float is rejected for a Decimal target.

    Given: A float value.
    When: decode_decimal is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_decimal(19.99)


def test_decode_decimal_rejects_non_string():
    """
    A non-string, non-Decimal value is rejected for a Decimal target.

    Given: A dict value.
    When: decode_decimal is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_decimal({"a": 1})
