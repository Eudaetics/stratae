"""Test the msgspec integration's pack registration for msgspec.Struct."""

import json
from uuid import UUID, uuid4

import msgspec
import pytest
from pytest_mock import MockerFixture

from stratae.integrations.serde import msgspec as msgspec_integration
from stratae.serde import pack


class Point(msgspec.Struct):
    """A simple msgspec.Struct used to exercise the pack registration."""

    x: int
    y: int


def test_pack_returns_bytes_for_struct():
    """
    Test that pack returns bytes for a msgspec.Struct.

    Given: A msgspec.Struct instance.
    When: The pack function is called with the instance.
    Then: The result should be a bytes object.
    """
    # Arrange
    value = Point(x=1, y=2)

    # Act
    result = pack(value)

    # Assert
    assert isinstance(result, bytes)


def test_pack_struct_round_trips():
    """
    Test that packing a msgspec.Struct produces its JSON representation.

    Given: A msgspec.Struct instance.
    When: The pack function is called with the instance.
    Then: Decoding the result should yield the struct's fields.
    """
    # Arrange
    value = Point(x=1, y=2)

    # Act
    result = pack(value)

    # Assert
    assert json.loads(result) == {"x": 1, "y": 2}


def test_pack_struct_uses_encode_for_unknown_field_types(mocker: MockerFixture):
    """
    Test that pack wires encode in as msgspec's enc_hook for struct fields.

    Given: A msgspec.Struct field containing a value msgspec can't natively
        serialize.
    When: The pack function is called with the struct.
    Then: encode is called with that value, and its return value is what
        ends up in the packed output.
    """

    # Arrange
    class Nested:
        def __init__(self, id: UUID):
            self.id = id

    class WithId(msgspec.Struct):
        child: object

    encode_mock = mocker.patch.object(msgspec_integration, "encode", return_value="encoded-value")
    value = Nested(uuid4())
    struct = WithId(child=value)

    # Act
    result = pack(struct)

    # Assert
    encode_mock.assert_called_once_with(value)
    assert json.loads(result) == {"child": "encoded-value"}


def test_pack_struct_raises_type_error_for_non_encodable_field():
    """
    Test that pack raises TypeError for struct fields with no encode registration.

    Given: A msgspec.Struct field containing an object with no encode
        registration.
    When: The pack function is called with the struct.
    Then: A TypeError should be raised indicating the object is not encodable.
    """

    # Arrange
    class NonEncodable:
        pass

    class WithValue(msgspec.Struct):
        value: object

    struct = WithValue(value=NonEncodable())

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .* is not encodable"):
        pack(struct)
