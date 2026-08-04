"""Test the serialize function for various payloads."""

import json
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

from stratae.serde import serialize


def test_serialize_returns_bytes():
    """
    Test that serialize returns bytes.

    Given: A JSON-serializable payload.
    When: The serialize function is called with the payload.
    Then: The result should be a bytes object.
    """
    # Arrange
    payload = {"key": "value"}

    # Act
    result = serialize(payload)

    # Assert
    assert isinstance(result, bytes)


def test_serialize_plain_payload():
    """
    Test that natively JSON-serializable payloads round-trip correctly.

    Given: A payload of plain JSON-serializable types.
    When: The serialize function is called with the payload.
    Then: Decoding the result should yield the original payload.
    """
    # Arrange
    payload = {"key": "value", "count": 3, "items": [1, 2, 3], "active": True}

    # Act
    result = serialize(payload)

    # Assert
    assert json.loads(result) == payload


def test_serialize_delegates_to_encode_for_unknown_types(mocker: MockerFixture):
    """
    Test that serialize wires encode in as the json.dumps default hook.

    Given: A payload containing a value json can't natively serialize.
    When: The serialize function is called with the payload.
    Then: encode is called with that value, and its return value is what
        ends up in the serialized output - per-type encoding behavior itself
        is covered by encode's own tests, not duplicated here.
    """
    # Arrange
    value = uuid4()
    payload = {"id": value}

    # Act
    result = serialize(payload)

    # Assert
    assert json.loads(result) == {"id": str(value)}


def test_serialize_raises_type_error_for_non_encodable():
    """
    Test that serialize raises TypeError for payloads containing non-encodable values.

    Given: A payload containing an object with no encode registration.
    When: The serialize function is called with the payload.
    Then: A TypeError should be raised indicating the object is not encodable.
    """

    # Arrange
    class NonEncodable:
        pass

    payload = {"value": NonEncodable()}

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .* is not encodable"):
        serialize(payload)


def test_serialize_register_for_custom_type():
    """
    Test that new payload types can register their own serialize handler.

    Given: A custom payload type with its own registered serialize handler.
    When: The serialize function is called with an instance of that type.
    Then: The registered handler should be used instead of the default json path.
    """

    # Arrange
    class RawPayload:
        def __init__(self, data: bytes):
            self.data = data

    @serialize.register
    def _(obj: RawPayload) -> bytes:
        """Serialize a RawPayload by returning its raw bytes unchanged."""
        return obj.data

    value = RawPayload(b"already serialized")

    # Act
    result = serialize(value)

    # Assert
    assert result == b"already serialized"


def test_serialize_raises_for_circular_reference():
    """
    Test that a self-referencing payload is rejected instead of recursing forever.

    Given: A payload that contains a circular reference to itself.
    When: The serialize function is called with the payload.
    Then: A ValueError is raised for the circular reference.
    """
    # Arrange
    payload: dict[str, object] = {"key": "value"}
    payload["self"] = payload

    # Act & Assert
    with pytest.raises(ValueError, match="Circular reference detected"):
        serialize(payload)


def test_serialize_raises_for_non_string_convertible_keys():
    """
    Test that dict keys json can't coerce to a string are rejected.

    Given: A payload whose dict key is a type json doesn't know how to
        represent as an object key (only str, int, float, bool, and None
        are allowed).
    When: The serialize function is called with the payload.
    Then: A TypeError is raised naming keys as the problem.
    """
    # Arrange
    payload = {(1, 2): "value"}

    # Act & Assert
    with pytest.raises(TypeError, match="keys must be"):
        serialize(payload)


def test_serialize_raises_for_set_payload():
    """
    Test that a bare set, with no to_dict/model_dump, isn't encodable.

    Given: A set payload - not natively JSON-serializable and without a
        to_dict or model_dump method for encode's fallback to use.
    When: The serialize function is called with the payload.
    Then: A TypeError is raised indicating the set isn't encodable.
    """
    # Arrange
    payload = {1, 2, 3}

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .* is not encodable"):
        serialize(payload)


def test_serialize_raises_for_raw_bytes_payload():
    """
    Test that raw bytes, serialize's own output type, aren't encodable as input.

    Given: A bytes payload - not natively JSON-serializable and without a
        to_dict or model_dump method.
    When: The serialize function is called with the payload.
    Then: A TypeError is raised indicating the bytes aren't encodable.
    """
    # Arrange
    payload = b"already bytes"

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .* is not encodable"):
        serialize(payload)


def test_serialize_raises_when_to_dict_attribute_is_not_callable():
    """
    Test that a to_dict attribute that isn't callable fails loudly.

    Given: An object whose to_dict attribute exists but isn't a method.
    When: The serialize function is called with the object.
    Then: A TypeError is raised because encode() tries to call it.
    """

    # Arrange
    class BrokenToDict:
        to_dict = "not a method"

    # Act & Assert
    with pytest.raises(TypeError, match="not callable"):
        serialize(BrokenToDict())


def test_serialize_propagates_error_raised_inside_to_dict():
    """
    Test that an exception raised inside a payload's to_dict propagates unchanged.

    Given: An object whose to_dict raises during its own computation.
    When: The serialize function is called with the object.
    Then: The original exception propagates unchanged.
    """

    # Arrange
    class Broken:
        def to_dict(self):
            raise RuntimeError("boom")

    # Act & Assert
    with pytest.raises(RuntimeError, match="boom"):
        serialize(Broken())


def test_serialize_allows_out_of_spec_float_values():
    """
    Test that non-finite floats are encoded as non-spec tokens instead of raising.

    Given: A payload containing NaN and +/-Infinity, which Python's json
        module permits by default even though they aren't valid per the
        JSON spec.
    When: The serialize function is called with the payload.
    Then: No error is raised - the caller gets non-spec-compliant tokens in
        the output rather than a failure, a landmine for strict JSON
        consumers downstream.
    """
    # Arrange
    payload = {"value": float("nan"), "pos_inf": float("inf"), "neg_inf": float("-inf")}

    # Act
    result = serialize(payload)

    # Assert
    assert result == b'{"value": NaN, "pos_inf": Infinity, "neg_inf": -Infinity}'
