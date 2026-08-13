"""Test decode_datetime directly: confirm a native datetime, or parse its ISO 8601 string form."""

from datetime import datetime

import pytest

from stratae.serde import decode_datetime


def test_decode_datetime_parses_iso_string():
    """
    A datetime's ISO 8601 string form is parsed back into a datetime.

    Given: A datetime's ISO 8601 string form.
    When: decode_datetime is called with that value.
    Then: A datetime equal to the original is returned.
    """
    value = datetime(2024, 1, 15, 9, 30)

    result = decode_datetime(value.isoformat())

    assert result == value


def test_decode_datetime_accepts_native_instance():
    """
    An already-native datetime instance is accepted for a datetime target.

    Given: A datetime instance, not a string.
    When: decode_datetime is called with that value.
    Then: The same datetime is returned.
    """
    value = datetime(2024, 1, 15, 9, 30)

    result = decode_datetime(value)

    assert result == value


def test_decode_datetime_rejects_non_string():
    """
    A non-string, non-datetime value is rejected for a datetime target.

    Given: A list value.
    When: decode_datetime is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_datetime([2024, 1, 15])
