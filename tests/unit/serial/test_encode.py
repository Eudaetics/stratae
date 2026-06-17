"""Test the encode function for various data types."""

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from stratae.serial import encode


@contextmanager
def strict_datetime_encoding():
    """Temporarily register a stricter datetime encoder that rejects naive datetimes."""
    original = encode.dispatch(datetime)

    def encoder(obj: datetime) -> str:
        if obj.tzinfo is None:
            raise ValueError(f"naive datetime {obj!r} is not encodable; attach a timezone")
        return obj.isoformat()

    encode.register(datetime, encoder)
    try:
        yield
    finally:
        encode.register(datetime, original)


def test_uuid_encoding():
    """
    Test that UUIDs are encoded as strings.

    Given: A UUID object.
    When: The encode function is called with the UUID.
    Then: The result should be a string representation of the UUID.
    """
    # Arrange
    value = uuid4()

    # Act
    result = encode(value)

    # Assert
    assert isinstance(result, str)
    assert result == str(value)


def test_datetime_encoding():
    """
    Test that datetime objects are encoded as ISO 8601 strings.

    Given: A datetime object.
    When: The encode function is called with the datetime.
    Then: The result should be an ISO 8601 string representation of the datetime.
    """
    # Arrange
    value = datetime(2023, 10, 1, 12, 0, 0)

    # Act
    result = encode(value)

    # Assert
    assert isinstance(result, str)
    assert result == value.isoformat()


def test_decimal_encoding():
    """
    Test that Decimal objects are encoded as strings.

    Given: A Decimal object.
    When: The encode function is called with the Decimal.
    Then: The result should be a string representation of the Decimal.
    """
    # Arrange
    value = Decimal("123.456")

    # Act
    result = encode(value)

    # Assert
    assert isinstance(result, str)
    assert result == str(value)


def test_override_datetime_encoding_for_stricter_tz_awareness():
    """
    Test that consumers can override the default datetime encoding.

    Given: A consumer that wants to reject naive datetimes instead of the
        default's permissive isoformat behavior.
    When: A stricter encoder is registered for datetime.
    Then: Naive datetimes raise, while aware datetimes still encode normally.
    """
    # Arrange
    naive = datetime(2023, 10, 1, 12, 0, 0)
    aware = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Act & Assert
    with strict_datetime_encoding():
        with pytest.raises(ValueError, match="naive datetime"):
            encode(naive)

        assert encode(aware) == aware.isoformat()


def test_custom_object_encoding_to_dict():
    """
    Test that custom objects with to_dict method are encoded correctly.

    Given: A custom object with a to_dict method.
    When: The encode function is called with the custom object.
    Then: The result should be a dictionary representation of the object.
    """

    # Arrange
    class CustomObject:
        def to_dict(self):
            return {"key": "value"}

    value = CustomObject()

    # Act
    result = encode(value)

    # Assert
    assert isinstance(result, dict)
    assert result == {"key": "value"}


def test_custom_object_encoding_model_dump():
    """
    Test that custom objects with model_dump method are encoded correctly.

    Given: A custom object with a model_dump method.
    When: The encode function is called with the custom object.
    Then: The result should be a dictionary representation of the object.
    """

    # Arrange
    class CustomModel:
        def model_dump(self):
            return {"key": "value"}

    value = CustomModel()

    # Act
    result = encode(value)

    # Assert
    assert isinstance(result, dict)
    assert result == {"key": "value"}


def test_custom_object_encoding_dict():
    """
    Test that custom objects with __dict__ attribute do not encode directly.

    Given: A custom object with a __dict__ attribute.
    When: The encode function is called with the custom object.
    Then: A TypeError should be raised indicating the object is not encodable.
    """

    # Arrange
    class CustomDictObject:
        def __init__(self):
            self.key = "value"

    value = CustomDictObject()

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .*CustomDictObject.* is not encodable"):
        encode(value)


def test_encode_raises_type_error_for_non_encodable():
    """
    Test that encode raises TypeError for non-encodable objects.

    Given: An object that does not have to_dict or model_dump.
    When: The encode function is called with the object.
    Then: A TypeError should be raised indicating the object is not encodable.
    """

    # Arrange
    def non_encodable_function():
        """Return a value that cannot be encoded."""
        return "I am not encodable"

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .* is not encodable"):
        encode(non_encodable_function)


def test_encode_exhausts_all_options():
    """Test that encode always returns or raises - no fallthrough."""

    # Arrange
    class ObjectWithNothing:
        def __init__(self):
            self.value = "test_value"

    # Act & Assert
    with pytest.raises(TypeError, match="Object of type .*ObjectWithNothing.* is not encodable"):
        encode(ObjectWithNothing())


def test_new_registration():
    """
    Test that new types can be registered with encode.

    Given: A new type with a custom encoding method.
    When: The encode function is called with the new type.
    Then: The result should be the expected encoded output.
    """

    # Arrange
    class NewType:
        def __init__(self, value: str):
            self.value = value

    @encode.register
    def _(obj: NewType) -> dict[str, str]:
        """Define a custom encoder for NewType."""
        return {"new_type_value": obj.value, "encoded": "test"}

    value = NewType("test_value")

    # Act
    result = encode(value)

    # Assert
    assert isinstance(result, dict)
    assert result == {"new_type_value": "test_value", "encoded": "test"}


def test_encode_priority_to_dict_over_model_dump():
    """
    Test that encode prefers to_dict() over model_dump().

    Given: An object with both to_dict() method and model_dump() method,
    When: encode is called,
    Then: it should use to_dict() and ignore model_dump().
    """

    # Arrange
    class ObjectWithModelDump:
        def __init__(self):
            self.attr = "from_model_dump"

        def to_dict(self):
            return {"source": "to_dict_method"}

        def model_dump(self):
            return {"source": "model_dump_method"}

    obj = ObjectWithModelDump()

    # Act
    result = encode(obj)

    # Assert
    assert result == {"source": "to_dict_method"}
    assert result != {"source": "model_dump_method"}
    assert result != {"attr": "from_model_dump"}
