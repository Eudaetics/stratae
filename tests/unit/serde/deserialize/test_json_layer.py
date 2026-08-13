"""
Test deserialize's top-level JSON decode step, independent of any specific target type.

Covers the `__call__` entry point itself: decoding raw bytes with `json.loads`,
returning that decoded value unchanged when no `type` is given, and round-tripping
a top-level scalar whose shape already matches its target type.
"""

import json
from typing import Any

import pytest

from stratae.serde import deserialize


def test_deserialize_invalid_json_raises():
    """
    Malformed JSON bytes surface as a JSON decode error.

    Given: Bytes that aren't valid JSON.
    When: deserialize is called.
    Then: json.JSONDecodeError propagates.
    """
    with pytest.raises(json.JSONDecodeError):
        deserialize(b"{not valid json", type=dict)


def test_deserialize_empty_bytes_raises():
    """
    Empty input surfaces as a JSON decode error.

    Given: Empty bytes.
    When: deserialize is called.
    Then: json.JSONDecodeError propagates.
    """
    with pytest.raises(json.JSONDecodeError):
        deserialize(b"", type=dict)


def test_deserialize_invalid_utf8_raises():
    """
    Bytes which aren't valid UTF-8 fail to decode.

    Given: Bytes that aren't valid UTF-8.
    When: deserialize is called.
    Then: UnicodeDecodeError propagates.
    """
    with pytest.raises(UnicodeDecodeError):
        deserialize(b"\xff\xfe\x00", type=dict)


@pytest.mark.parametrize(
    ("target_type", "value"),
    [
        (str, "hi"),
        (int, 42),
        (float, 3.14),
        (bool, True),
    ],
    ids=["str", "int", "float", "bool"],
)
def test_deserialize_round_trips_native_shape(target_type: type[Any], value: Any):
    """
    A top-level JSON scalar round-trips when type matches its native shape.

    Given: JSON bytes whose top level is a plain, JSON-native scalar.
    When: deserialize is called with that value's own type as the target.
    Then: The original value is returned unchanged.
    """
    # Arrange
    data = json.dumps(value).encode()

    # Act
    result = deserialize(data, type=target_type)

    # Assert
    assert result == value


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (b'{"a": 1, "b": 2}', {"a": 1, "b": 2}),
        (b"[1, 2, 3]", [1, 2, 3]),
        (b'"hi"', "hi"),
        (b"42", 42),
        (b"3.14", 3.14),
        (b"true", True),
        (b"null", None),
    ],
    ids=["dict", "list", "str", "int", "float", "bool", "null"],
)
def test_deserialize_without_type_returns_decoded_value(data: bytes, expected: Any):
    """
    Omitting type just decodes the JSON value, equivalent to json.loads.

    Given: JSON bytes of any native shape.
    When: deserialize is called with no type argument.
    Then: The decoded value is returned exactly as json.loads would produce it.
    """
    assert deserialize(data) == expected
