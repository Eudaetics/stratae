"""
Test decode_enum and decode_enum_by_name.

decode_enum confirms a native member of the target enum, or looks one up by
value. The same handler covers `Enum` subclasses through MRO fallback when
registered on `Enum` itself via `takes_type=True`. decode_enum_by_name
does the same by member name instead.

Covers direct calls to both, deserializing into a plain Enum, an IntEnum,
and a StrEnum (whose mixed-in int/str base must not win over Enum),
invalid values/names being rejected, the shared handler applying inside a
list, a subclass's own handler taking priority over the shared one, and
decode_enum_by_name working end-to-end once registered.
"""

import json
from enum import Enum, IntEnum, StrEnum
from typing import Any

import pytest

from stratae.serde import decode_enum, decode_enum_by_name, deserialize


@pytest.fixture()
def register_enum():
    """Temporarily register decode by enum value."""
    deserialize.register(Enum, decode_enum, takes_type=True)
    yield
    deserialize.deregister(Enum)


@pytest.fixture()
def register_enum_by_name():
    """Temporarily register decode by enum name."""
    deserialize.register(Enum, decode_enum_by_name, takes_type=True)
    yield
    deserialize.deregister(Enum)


def test_decode_enum_looks_up_member_by_value():
    """
    A raw value matching a member's value is looked up on the target enum.

    Given: The raw value of an Enum member.
    When: decode_enum is called with that value and the enum as the type.
    Then: The matching member is returned.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act
    result = decode_enum("red", Color)

    # Assert
    assert result is Color.RED


def test_decode_enum_accepts_native_instance():
    """
    An already-native member of the target enum is accepted as-is.

    Given: An Enum member, not its raw value.
    When: decode_enum is called with that member and the enum as the type.
    Then: The same member is returned.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act
    result = decode_enum(Color.GREEN, Color)

    # Assert
    assert result is Color.GREEN


def test_decode_enum_rejects_invalid_value():
    """
    A value with no matching member on the target enum is rejected.

    Given: A string that isn't any member's value on the target enum.
    When: decode_enum is called with that value and the enum as the type.
    Then: TypeError is raised.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act & Assert
    with pytest.raises(TypeError, match="not a valid member value"):
        decode_enum("purple", Color)


def test_decode_enum_works_for_int_enum():
    """
    The shared registration covers IntEnum too, not just plain Enum.

    Given: The raw int value of an IntEnum member.
    When: decode_enum is called with that value and the IntEnum as the type.
    Then: The matching member is returned.
    """

    # Arrange
    class Priority(IntEnum):
        LOW = 1
        HIGH = 2

    # Act
    result = decode_enum(2, Priority)

    # Assert
    assert result is Priority.HIGH


def test_deserialize_dispatches_to_shared_enum_handler(register_enum: None):
    """
    Deserialize reaches decode_enum for an Enum target with no handler of its own.

    Given: JSON bytes holding an Enum member's raw value, and no handler
        registered for that enum specifically.
    When: deserialize is called with that enum as the target type.
    Then: The matching member is returned, via the handler registered on
        the Enum base class.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    data = json.dumps("green").encode()

    # Act
    result = deserialize(data, type=Color)

    # Assert
    assert result is Color.GREEN


def test_deserialize_shared_enum_handler_applies_inside_list(register_enum: None):
    """
    The shared Enum handler also applies to list[T] elements, not just the top level.

    Given: JSON bytes holding a list of an Enum's raw member values.
    When: deserialize is called with list[that enum] as the target.
    Then: Each element is reconstructed as the matching member.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    data = json.dumps(["red", "green"]).encode()

    # Act
    result = deserialize(data, type=list[Color])

    # Assert
    assert result == [Color.RED, Color.GREEN]


def test_deserialize_rejects_invalid_enum_value(register_enum: None):
    """
    Deserialize propagates decode_enum's TypeError for an invalid member value.

    Given: JSON bytes holding a value that isn't any member of the target enum.
    When: deserialize is called with that enum as the target type.
    Then: TypeError is raised.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    data = json.dumps("purple").encode()

    # Act & Assert
    with pytest.raises(TypeError, match="not a valid member value"):
        deserialize(data, type=Color)


def test_deserialize_subclass_specific_handler_takes_priority_over_shared_enum_handler():
    """
    A handler registered for one specific enum wins over the shared Enum handler.

    Given: A handler registered directly for an enum, overriding the shared
        Enum handler with case-insensitive lookup.
    When: deserialize is called with that enum as the target type, using a
        value the shared handler alone would reject.
    Then: The enum-specific handler runs, not the shared one.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    @deserialize.register(Color)
    def _(value: Any) -> Color:
        return Color(value.lower())

    data = json.dumps("RED").encode()

    # Act
    result = deserialize(data, type=Color)

    # Assert
    assert result is Color.RED


def test_decode_enum_by_name_looks_up_member_by_name():
    """
    A raw value matching a member's name is looked up on the target enum.

    Given: The name of an Enum member.
    When: decode_enum_by_name is called with that name and the enum as the type.
    Then: The matching member is returned.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act
    result = decode_enum_by_name("RED", Color)

    # Assert
    assert result is Color.RED


def test_decode_enum_by_name_accepts_native_instance():
    """
    An already-native member of the target enum is accepted as-is.

    Given: An Enum member, not its name.
    When: decode_enum_by_name is called with that member and the enum as the type.
    Then: The same member is returned.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act
    result = decode_enum_by_name(Color.GREEN, Color)

    # Assert
    assert result is Color.GREEN


def test_decode_enum_by_name_rejects_invalid_name():
    """
    A name with no matching member on the target enum is rejected.

    Given: A string that isn't any member's name on the target enum.
    When: decode_enum_by_name is called with that value and the enum as the type.
    Then: TypeError is raised.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act & Assert
    with pytest.raises(TypeError, match="not a valid member name"):
        decode_enum_by_name("PURPLE", Color)


def test_decode_enum_by_name_rejects_member_value_used_as_name():
    """
    A member's raw value doesn't work as a name unless it happens to match one.

    Given: A member's value, not its name.
    When: decode_enum_by_name is called with that value and the enum as the type.
    Then: TypeError is raised, proving the lookup is name-based, not value-based.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act & Assert
    with pytest.raises(TypeError, match="not a valid member name"):
        decode_enum_by_name("red", Color)


def test_decode_enum_by_name_rejects_unhashable_value():
    """
    An unhashable value is rejected with the same error as any other bad name.

    Given: A list, which can't be used as a member-name lookup key.
    When: decode_enum_by_name is called with that value and the enum as the type.
    Then: TypeError is raised.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    # Act & Assert
    with pytest.raises(TypeError, match="not a valid member name"):
        decode_enum_by_name(["red"], Color)


def test_deserialize_uses_decode_enum_by_name_when_registered(register_enum_by_name: None):
    """
    decode_enum_by_name works end-to-end once registered for a specific enum.

    Given: JSON bytes holding an Enum member's name, and decode_enum_by_name
        registered for that enum in place of the shared value-based handler.
    When: deserialize is called with that enum as the target type.
    Then: The matching member is returned, looked up by name.
    """

    # Arrange
    class Color(Enum):
        RED = "red"
        GREEN = "green"

    data = json.dumps("RED").encode()

    # Act
    result = deserialize(data, type=Color)

    # Assert
    assert result is Color.RED


def test_deserialize_int_enum_uses_shared_enum_handler_over_int(register_enum: None):
    """
    An IntEnum target reaches the shared Enum handler, not the int handler.

    Given: JSON bytes holding an IntEnum member's raw value, and decode_enum
        registered on Enum.
    When: deserialize is called with the IntEnum as the target type.
    Then: The matching member is returned, not a plain int, even though int
        comes before Enum in IntEnum's MRO.
    """

    # Arrange
    class Priority(IntEnum):
        LOW = 1
        HIGH = 2

    data = json.dumps(2).encode()

    # Act
    result = deserialize(data, type=Priority)

    # Assert
    assert result is Priority.HIGH


def test_deserialize_int_enum_by_name_uses_shared_enum_handler(register_enum_by_name: None):
    """
    An IntEnum target decodes by name when decode_enum_by_name is registered on Enum.

    Given: JSON bytes holding an IntEnum member's name, and decode_enum_by_name
        registered on Enum.
    When: deserialize is called with the IntEnum as the target type.
    Then: The member with that name is returned, rather than the int handler
        rejecting the name as not int-shaped.
    """

    # Arrange
    class Priority(IntEnum):
        LOW = 1
        HIGH = 2

    data = json.dumps("HIGH").encode()

    # Act
    result = deserialize(data, type=Priority)

    # Assert
    assert result is Priority.HIGH


def test_deserialize_str_enum_by_name_uses_shared_enum_handler(register_enum_by_name: None):
    """
    A StrEnum target reaches the shared Enum handler, not the str handler.

    Given: JSON bytes holding a StrEnum member's name, and decode_enum_by_name
        registered on Enum.
    When: deserialize is called with the StrEnum as the target type.
    Then: The member with that name is returned, not the name as a plain str.
    """

    # Arrange
    class Shade(StrEnum):
        LIGHT = "light"
        DARK = "dark"

    data = json.dumps("DARK").encode()

    # Act
    result = deserialize(data, type=Shade)

    # Assert
    assert result is Shade.DARK


@pytest.mark.parametrize(
    ("base", "value"),
    [(Enum, "light"), (IntEnum, 1), (StrEnum, "light")],
    ids=["enum", "int-enum", "str-enum"],
)
def test_deserialize_enum_without_handler_raises(base: type[Enum], value: Any):
    """
    An enum target with no Enum handler registered raises, whatever its mixed-in type.

    Given: JSON bytes holding an enum member's raw value, and no handler registered
        for the enum or any Enum base.
    When: deserialize is called with a plain Enum, IntEnum, or StrEnum as the target.
    Then: TypeError is raised for the missing handler, rather than the int or str
        handler building a plain value, since value-or-name is the application's choice.
    """
    # Arrange
    Shade = base("Shade", {"LIGHT": value})
    data = json.dumps(value).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="no handler registered"):
        deserialize(data, type=Shade)
