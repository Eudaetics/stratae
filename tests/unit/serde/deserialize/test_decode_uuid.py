"""Test decode_uuid directly: confirm a native UUID, or parse its string form."""

from uuid import uuid4

import pytest

from stratae.serde import decode_uuid


def test_decode_uuid_parses_string_form():
    """
    A UUID's string form is parsed back into a UUID.

    Given: A UUID's string form.
    When: decode_uuid is called with that value.
    Then: A UUID equal to the original is returned.
    """
    value = uuid4()

    result = decode_uuid(str(value))

    assert result == value


def test_decode_uuid_accepts_native_instance():
    """
    An already-native UUID instance is accepted for a UUID target.

    Given: A UUID instance, not a string.
    When: decode_uuid is called with that value.
    Then: The same UUID is returned.
    """
    value = uuid4()

    result = decode_uuid(value)

    assert result == value


def test_decode_uuid_rejects_non_string():
    """
    A non-string, non-UUID value is rejected for a UUID target.

    Given: An int value.
    When: decode_uuid is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_uuid(42)
