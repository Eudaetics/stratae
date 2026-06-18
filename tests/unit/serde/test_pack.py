"""Test the pack function for various payloads."""

import json
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

from stratae.serde import pack
from stratae.serde import packer as pack_module


def test_pack_returns_bytes():
    """
    Test that pack returns bytes.

    Given: A JSON-serializable payload.
    When: The pack function is called with the payload.
    Then: The result should be a bytes object.
    """
    # Arrange
    payload = {"key": "value"}

    # Act
    result = pack(payload)

    # Assert
    assert isinstance(result, bytes)


def test_pack_plain_payload():
    """
    Test that natively JSON-serializable payloads round-trip correctly.

    Given: A payload of plain JSON-serializable types.
    When: The pack function is called with the payload.
    Then: Decoding the result should yield the original payload.
    """
    # Arrange
    payload = {"key": "value", "count": 3, "items": [1, 2, 3], "active": True}

    # Act
    result = pack(payload)

    # Assert
    assert json.loads(result) == payload


def test_pack_delegates_to_encode_for_unknown_types(mocker: MockerFixture):
    """
    Test that pack wires encode in as the json.dumps default hook.

    Given: A payload containing a value json can't natively serialize.
    When: The pack function is called with the payload.
    Then: encode is called with that value, and its return value is what
        ends up in the packed output - per-type encoding behavior itself
        is covered by encode's own tests, not duplicated here.
    """
    # Arrange
    encode_mock = mocker.patch.object(pack_module, "encode", return_value="encoded-value")
    value = uuid4()
    payload = {"id": value}

    # Act
    result = pack(payload)

    # Assert
    encode_mock.assert_called_once_with(value)
    assert json.loads(result) == {"id": "encoded-value"}


def test_pack_raises_type_error_for_non_encodable():
    """
    Test that pack raises TypeError for payloads containing non-encodable values.

    Given: A payload containing an object with no encode registration.
    When: The pack function is called with the payload.
    Then: A TypeError should be raised indicating the object is not encodable.
    """

    # Arrange
    class NonEncodable:
        pass

    payload = {"value": NonEncodable()}

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .* is not encodable"):
        pack(payload)


def test_pack_register_for_custom_type():
    """
    Test that new payload types can register their own pack handler.

    Given: A custom payload type with its own registered pack handler.
    When: The pack function is called with an instance of that type.
    Then: The registered handler should be used instead of the default json path.
    """

    # Arrange
    class RawPayload:
        def __init__(self, data: bytes):
            self.data = data

    @pack.register
    def _(obj: RawPayload) -> bytes:
        """Pack a RawPayload by returning its raw bytes unchanged."""
        return obj.data

    value = RawPayload(b"already packed")

    # Act
    result = pack(value)

    # Assert
    assert result == b"already packed"
