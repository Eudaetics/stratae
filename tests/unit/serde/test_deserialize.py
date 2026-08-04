"""Test the deserialize function for various payloads and failure modes."""

import json
from dataclasses import dataclass, field
from typing import Any, Literal, Optional
from uuid import UUID, uuid4

import pytest

from stratae.serde import Deserializer, deserialize, serialize


def deserialize_with(deserializer: Deserializer, data: bytes, payload_type: type[Any]) -> Any:
    """Call through the protocol exactly the way an adapter would."""
    return deserializer(data, type=payload_type)


def test_satisfies_deserializer_protocol():
    """A callable with the protocol's shape binds and runs through it."""

    def compatible(data: bytes, /, type: type[Any]) -> Any:
        return type(data)

    assert deserialize_with(compatible, b"payload", bytes) == b"payload"


def test_fails_protocol():
    """A callable without the protocol's shape fails to bind at call time."""

    def incompatible() -> str:
        return "test"

    deserializer: Any = incompatible
    with pytest.raises(TypeError):
        deserialize_with(deserializer, b"payload", bytes)


def test_deserialize_plain_dataclass():
    """
    Test that a flat dataclass round-trips through deserialize.

    Given: JSON bytes matching a flat dataclass's fields.
    When: deserialize is called with that dataclass as the target type.
    Then: An instance of the dataclass is constructed with matching fields.
    """

    # Arrange
    @dataclass
    class Widget:
        name: str
        count: int

    data = json.dumps({"name": "sprocket", "count": 3}).encode()

    # Act
    result = deserialize(data, type=Widget)

    # Assert
    assert result == Widget(name="sprocket", count=3)


def test_deserialize_missing_optional_field_uses_class_default():
    """
    Test that a field omitted from the payload falls back to its class default.

    Given: JSON missing a field that has a default value on the dataclass.
    When: deserialize is called.
    Then: The constructed instance uses the class default for that field.
    """

    # Arrange
    @dataclass
    class Widget:
        name: str
        count: int = 0

    data = json.dumps({"name": "sprocket"}).encode()

    # Act
    result = deserialize(data, type=Widget)

    # Assert
    assert result == Widget(name="sprocket", count=0)


def test_deserialize_dict_type():
    """
    Test that deserializing into dict just reconstructs the mapping.

    Given: JSON object bytes.
    When: deserialize is called with dict as the target type.
    Then: A plain dict equal to the JSON payload is returned.
    """
    # Arrange
    data = json.dumps({"a": 1, "b": 2}).encode()

    # Act
    # Intentionally ignoring type subscripts
    result = deserialize(data, type=dict)  # pyright: ignore[reportUnknownVariableType]

    # Assert
    assert result == {"a": 1, "b": 2}


def test_deserialize_invalid_json_raises():
    """
    Test that malformed JSON bytes surface as a JSON decode error.

    Given: Bytes that aren't valid JSON.
    When: deserialize is called.
    Then: json.JSONDecodeError propagates.
    """
    with pytest.raises(json.JSONDecodeError):
        deserialize(b"{not valid json", type=dict)


def test_deserialize_empty_bytes_raises():
    """
    Test that empty input surfaces as a JSON decode error.

    Given: Empty bytes.
    When: deserialize is called.
    Then: json.JSONDecodeError propagates.
    """
    with pytest.raises(json.JSONDecodeError):
        deserialize(b"", type=dict)


def test_deserialize_invalid_utf8_raises():
    """
    Test that bytes which aren't valid UTF-8 fail to decode.

    Given: Bytes that aren't valid UTF-8.
    When: deserialize is called.
    Then: UnicodeDecodeError propagates from json.loads' own decode step.
    """
    with pytest.raises(UnicodeDecodeError):
        deserialize(b"\xff\xfe\x00", type=dict)


@pytest.mark.parametrize(
    ("target_type", "value"),
    [
        (list, [1, 2, 3]),
        (str, "hi"),
        (int, 42),
        (float, 3.14),
        (bool, True),
        (dict, {"a": 1, "b": 2}),
    ],
    ids=["list", "str", "int", "float", "bool", "dict"],
)
def test_deserialize_round_trips_native_shape(target_type: type[Any], value: Any):
    """
    Test that a top-level JSON value round-trips when type matches its native shape.

    Given: JSON bytes whose top level is a plain, JSON-native value.
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
    Test that omitting type just decodes the JSON value, equivalent to json.loads.

    Given: JSON bytes of any native shape.
    When: deserialize is called with no type argument.
    Then: The decoded value is returned exactly as json.loads would produce it.
    """
    assert deserialize(data) == expected


@pytest.mark.parametrize(
    ("item_type", "value"),
    [
        (int, [1, 2, 3]),
        (str, ["a", "b", "c"]),
        (float, [1.5, 2.5]),
    ],
    ids=["int", "str", "float"],
)
def test_deserialize_list_of_primitives(item_type: type[Any], value: list[Any]):
    """
    Test that a parameterized list[T] target reconstructs each element as T.

    Given: A JSON array of primitives.
    When: deserialize is called with list[T] as the target type.
    Then: A list of T instances equal to the original values is returned.
    """
    # Arrange
    data = json.dumps(value).encode()

    # Act
    result = deserialize(data, type=list[item_type])

    # Assert
    assert result == value


def test_deserialize_list_of_dataclasses():
    """
    Test that list[SomeDataclass] reconstructs each element as that dataclass.

    Given: A JSON array of objects.
    When: deserialize is called with list[Item] as the target type.
    Then: Each element is reconstructed as an Item instance via the same
        keyword-construction rule used for a single dataclass.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

    data = json.dumps([{"sku": "A1"}, {"sku": "B2"}]).encode()

    # Act
    result = deserialize(data, type=list[Item])

    # Assert
    assert result == [Item(sku="A1"), Item(sku="B2")]


def test_deserialize_empty_list_of_dataclasses():
    """
    Test that an empty JSON array with a list[T] target returns an empty list.

    Given: An empty JSON array.
    When: deserialize is called with list[Item] as the target type.
    Then: An empty list is returned, without attempting to construct any Item.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

    data = b"[]"

    # Act
    result = deserialize(data, type=list[Item])

    # Assert
    assert result == []


def test_deserialize_nested_list():
    """
    Test that list[list[T]] recurses through both levels of nesting.

    Given: A JSON array of arrays.
    When: deserialize is called with list[list[int]] as the target type.
    Then: The nested structure is reconstructed unchanged, having recursed
        through the outer list into each inner list.
    """
    # Arrange
    data = json.dumps([[1, 2], [3, 4]]).encode()

    # Act
    result = deserialize(data, type=list[list[int]])

    # Assert
    assert result == [[1, 2], [3, 4]]


@pytest.mark.parametrize(
    ("item_type", "value"),
    [
        (int, [1, 2, 3]),
        (str, ["a", "b", "c"]),
    ],
    ids=["int", "str"],
)
def test_deserialize_set_of_primitives(item_type: type[Any], value: list[Any]):
    """
    Test that a parameterized set[T] target reconstructs each element as T.

    Given: A JSON array of primitives.
    When: deserialize is called with set[T] as the target type.
    Then: A set of T instances equal to the original values is returned.
    """
    # Arrange
    data = json.dumps(value).encode()

    # Act
    result = deserialize(data, type=set[item_type])

    # Assert
    assert result == set(value)


def test_deserialize_set_of_uuids():
    """
    Test that set[UUID] reconstructs each element via the generic positional fallback.

    Given: A JSON array of UUID string forms.
    When: deserialize is called with set[UUID] as the target type.
    Then: A set of UUID instances equal to the originals is returned - the
        same fallback used for UUID as a raw target type (see
        test_deserialize_uuid) applies per element here too.
    """
    # Arrange
    values = [uuid4(), uuid4()]
    data = json.dumps([str(v) for v in values]).encode()

    # Act
    result = deserialize(data, type=set[UUID])

    # Assert
    assert result == set(values)


def test_deserialize_tuple_variadic():
    """
    Test that tuple[T, ...] reconstructs a variable-length tuple of T.

    Given: A JSON array of primitives.
    When: deserialize is called with tuple[T, ...] as the target type.
    Then: A tuple of T instances equal to the original values is returned.
    """
    # Arrange
    data = json.dumps([1, 2, 3]).encode()

    # Act
    result = deserialize(data, type=tuple[int, ...])

    # Assert
    assert result == (1, 2, 3)


def test_deserialize_tuple_variadic_of_uuids():
    """
    Test that tuple[UUID, ...] reconstructs each element via the generic positional fallback.

    Given: A JSON array of UUID string forms.
    When: deserialize is called with tuple[UUID, ...] as the target type.
    Then: A tuple of UUID instances equal to the originals is returned.
    """
    # Arrange
    values = [uuid4(), uuid4(), uuid4()]
    data = json.dumps([str(v) for v in values]).encode()

    # Act
    result = deserialize(data, type=tuple[UUID, ...])

    # Assert
    assert result == tuple(values)


def test_deserialize_tuple_fixed_length():
    """
    Test that tuple[T1, T2, T3] reconstructs a fixed-length heterogeneous tuple.

    Given: A JSON array matching a fixed sequence of distinct types.
    When: deserialize is called with tuple[int, str, bool] as the target type.
    Then: Each element is reconstructed against its own positional type.
    """
    # Arrange
    data = json.dumps([1, "two", True]).encode()

    # Act
    result = deserialize(data, type=tuple[int, str, bool])

    # Assert
    assert result == (1, "two", True)


def test_deserialize_tuple_fixed_length_with_subtype():
    """
    Test that a fixed-length tuple can mix a raw-fallback subtype with primitives.

    Given: A JSON array whose first element is a UUID string form.
    When: deserialize is called with tuple[UUID, str] as the target type.
    Then: The first element is reconstructed as a UUID, the second stays a str.
    """
    # Arrange
    value = uuid4()
    data = json.dumps([str(value), "sprocket"]).encode()

    # Act
    result = deserialize(data, type=tuple[UUID, str])

    # Assert
    assert result == (value, "sprocket")


def test_deserialize_tuple_fixed_length_mismatch_raises():
    """
    Test that a fixed-length tuple target rejects a mismatched element count.

    Given: A JSON array with fewer elements than the declared tuple type.
    When: deserialize is called with a fixed-length tuple target.
    Then: ValueError is raised for the length mismatch, from zip's strict mode.
    """
    # Arrange
    data = json.dumps([1, "two"]).encode()

    # Act & Assert
    with pytest.raises(ValueError, match="zip"):
        deserialize(data, type=tuple[int, str, bool])


def test_deserialize_optional_with_value():
    """
    Test that X | None reconstructs the value against its non-None member.

    Given: JSON bytes holding a plain int.
    When: deserialize is called with int | None as the target type.
    Then: The int is reconstructed via the single non-None member.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act
    result = deserialize(data, type=int | None)

    # Assert
    assert result == 42


def test_deserialize_optional_with_none():
    """
    Test that X | None passes a JSON null straight through as None.

    Given: JSON bytes that are the literal null.
    When: deserialize is called with int | None as the target type.
    Then: None is returned, without attempting to construct int.
    """
    # Act
    result = deserialize(b"null", type=int | None)

    # Assert
    assert result is None


def test_deserialize_optional_uuid():
    """
    Test that UUID | None reconstructs a UUID via the generic positional fallback.

    Given: JSON bytes holding a UUID string form.
    When: deserialize is called with UUID | None as the target type.
    Then: A UUID equal to the original is returned.
    """
    # Arrange
    value = uuid4()
    data = json.dumps(str(value)).encode()

    # Act
    result = deserialize(data, type=UUID | None)

    # Assert
    assert result == value


def test_deserialize_typing_optional():
    """
    Test that typing.Optional[X] resolves through the same path as X | None.

    Given: JSON bytes holding a plain str.
    When: deserialize is called with typing.Optional[str] as the target type.
    Then: The str is returned, confirming both union spellings are handled.
    """
    # Arrange
    data = json.dumps("hi").encode()

    # Act
    result = deserialize(data, type=Optional[str])

    # Assert
    assert result == "hi"


def test_deserialize_ambiguous_union_raises():
    """
    Test that a union with more than one non-None member is rejected.

    Given: JSON bytes holding a plain int.
    When: deserialize is called with int | str as the target type.
    Then: TypeError is raised for the ambiguous union, rather than guessing
        which member to construct.
    """
    # Arrange
    data = json.dumps(1).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="ambiguous union"):
        deserialize(data, type=int | str)


def test_deserialize_literal_type_raises():
    """
    Test that a type that isn't a real constructor fails when construction is attempted.

    Given: JSON bytes holding a plain str.
    When: deserialize is called with a Literal as the target type, a typing
        special form rather than a callable class.
    Then: TypeError propagates from Python's own attempt to call it -
        callable() doesn't reliably distinguish special forms like this
        from real constructors, so no error is manufactured here.
    """
    # Arrange
    data = json.dumps("a").encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=Literal["a", "b"])


def test_deserialize_dict_keeps_nested_values_as_decoded():
    """
    Test that a dict[str, Any] target passes nested object/array values through untouched.

    Given: A JSON object whose values are themselves objects and arrays.
    When: deserialize is called with dict[str, Any] as the target type.
    Then: The nested values come back exactly as json.loads produced them -
        Any means no constraint, so each value is returned unchanged rather
        than an attempt being made to construct something from it.
    """
    # Arrange
    data = json.dumps({"a": {"nested": 1}, "b": [1, 2, 3]}).encode()

    # Act
    result = deserialize(data, type=dict[str, Any])

    # Assert
    assert result == {"a": {"nested": 1}, "b": [1, 2, 3]}


def test_deserialize_uses_registered_handler():
    """
    Test that a handler registered for a type overrides the default construction.

    Given: A type with a handler registered via deserialize.register.
    When: deserialize is called with that type as the target.
    Then: The registered handler runs instead of the default keyword
        construction, receiving the already-decoded value directly.
    """

    # Arrange
    @dataclass
    class Widget:
        name: str

    @deserialize.register(Widget)
    def _(value: Any) -> Widget:
        return Widget(name=value["name"].upper())

    data = json.dumps({"name": "sprocket"}).encode()

    # Act
    result = deserialize(data, type=Widget)

    # Assert
    assert result == Widget(name="SPROCKET")


def test_deserialize_registered_handler_applies_inside_list():
    """
    Test that a registered handler also applies to list[T] elements, not just the top level.

    Given: A type with a handler registered via deserialize.register.
    When: deserialize is called with list[that type] as the target, so the
        type is reached through _construct's recursion rather than directly.
    Then: The registered handler runs for each element - registration
        applies wherever the type gets constructed, not only when it's the
        top-level type argument.
    """

    # Arrange
    @dataclass
    class Widget:
        name: str

    @deserialize.register(Widget)
    def _(value: Any) -> Widget:
        return Widget(name=value["name"].upper())

    data = json.dumps([{"name": "sprocket"}, {"name": "cog"}]).encode()

    # Act
    result = deserialize(data, type=list[Widget])

    # Assert
    assert result == [Widget(name="SPROCKET"), Widget(name="COG")]


def test_deserialize_dict_of_dataclasses():
    """
    Test that dict[str, SomeDataclass] reconstructs each value as that dataclass.

    Given: A JSON object whose values are themselves objects.
    When: deserialize is called with dict[str, Item] as the target type.
    Then: Each value is reconstructed as an Item instance, via the same
        recursion used for list[T], while keys are left as the plain
        strings JSON objects always decode to.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

    data = json.dumps({"a": {"sku": "A1"}, "b": {"sku": "B2"}}).encode()

    # Act
    result = deserialize(data, type=dict[str, Item])

    # Assert
    assert result == {"a": Item(sku="A1"), "b": Item(sku="B2")}


def test_deserialize_registered_handler_applies_inside_dict():
    """
    Test that a registered handler also applies to dict[str, T] values, not just the top level.

    Given: A type with a handler registered via deserialize.register.
    When: deserialize is called with dict[str, that type] as the target, so
        the type is reached through _construct's recursion rather than
        directly.
    Then: The registered handler runs for each value - registration applies
        wherever the type gets constructed, the same as it does for list[T].
    """

    # Arrange
    @dataclass
    class Widget:
        name: str

    @deserialize.register(Widget)
    def _(value: Any) -> Widget:
        return Widget(name=value["name"].upper())

    data = json.dumps({"a": {"name": "sprocket"}, "b": {"name": "cog"}}).encode()

    # Act
    result = deserialize(data, type=dict[str, Widget])

    # Assert
    assert result == {"a": Widget(name="SPROCKET"), "b": Widget(name="COG")}


def test_deserialize_register_is_the_escape_hatch_for_field_coercion():
    """
    Test that registering a dataclass itself is how to get its fields coerced.

    Given: A dataclass with a UUID-typed field - deserialize does no
        per-field type coercion by default, so this field would otherwise
        come back as a plain string (see the "leaves ... as" tests).
    When: A handler is registered for the dataclass that does the
        conversion itself.
    Then: deserialize produces a fully-coerced instance, without any change
        to deserialize's own default behavior - the registry is the
        intended escape hatch for this, not a built-in guess.
    """

    # Arrange
    @dataclass
    class Widget:
        id: UUID

    @deserialize.register(Widget)
    def _(value: Any) -> Widget:
        return Widget(id=UUID(value["id"]))

    original_id = uuid4()
    data = json.dumps({"id": str(original_id)}).encode()

    # Act
    result = deserialize(data, type=Widget)

    # Assert
    assert result == Widget(id=original_id)
    assert isinstance(result.id, UUID)


def test_deserialize_missing_required_field_raises():
    """
    Test that a required field missing from the payload fails construction.

    Given: JSON missing a field the target's __init__ requires.
    When: deserialize is called.
    Then: TypeError is raised for the missing argument.
    """

    # Arrange
    @dataclass
    class Widget:
        name: str
        count: int

    data = json.dumps({"name": "sprocket"}).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="missing"):
        deserialize(data, type=Widget)


def test_deserialize_unknown_field_raises():
    """
    Test that a field the target's __init__ doesn't accept fails construction.

    Given: JSON with a field name the target's __init__ doesn't declare.
    When: deserialize is called.
    Then: TypeError is raised for the unexpected keyword argument.
    """

    # Arrange
    @dataclass
    class Widget:
        name: str

    data = json.dumps({"name": "sprocket", "extra": "surprise"}).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        deserialize(data, type=Widget)


def test_deserialize_init_false_field_raises():
    """
    Test that a computed, init=False field round-tripped in the payload breaks construction.

    Given: A dataclass field marked init=False - excluded from __init__ but
        still present if a caller's own to_dict includes it.
    When: deserialize is called with that field included in the payload.
    Then: TypeError is raised since __init__ doesn't accept it.
    """

    # Arrange
    @dataclass
    class Widget:
        name: str
        computed: int = field(init=False, default=0)

        def __post_init__(self):
            self.computed = len(self.name)

    data = json.dumps({"name": "sprocket", "computed": 8}).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        deserialize(data, type=Widget)


def test_deserialize_positional_only_params_raises():
    """
    Test that a constructor requiring positional-only parameters can't be satisfied.

    Given: A target whose __init__ only accepts positional-only parameters.
    When: deserialize is called, which can only ever pass keyword arguments.
    Then: TypeError is raised since none of the fields can bind.
    """

    # Arrange
    class Widget:
        def __init__(self, name: str, /):
            self.name = name

    data = json.dumps({"name": "sprocket"}).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=Widget)


def test_deserialize_builtin_without_kwargs_raises():
    """
    Test that a builtin whose constructor takes no keyword arguments rejects the payload.

    Given: A target type (list) whose constructor doesn't accept keyword
        arguments at all.
    When: deserialize is called with a non-empty payload.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps({"key": "value"}).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=list)


def test_deserialize_propagates_validation_error_from_target_init():
    """
    Test that validation errors raised while constructing the target propagate unchanged.

    Given: A dataclass whose __post_init__ validates its own fields.
    When: deserialize is called with data that fails that validation.
    Then: The validation error propagates unchanged from deserialize.
    """

    # Arrange
    @dataclass
    class Widget:
        count: int

        def __post_init__(self):
            if self.count < 0:
                raise ValueError("count must be non-negative")

    data = json.dumps({"count": -1}).encode()

    # Act & Assert
    with pytest.raises(ValueError, match="count must be non-negative"):
        deserialize(data, type=Widget)


def test_deserialize_uuid():
    """
    Test that UUID works as a raw target type, via the generic positional fallback.

    This isn't UUID-specific special-casing - it's the same `type(value)` positional
    fallback used for any non-dict value, and UUID's constructor happens to accept
    its own string form as that single positional argument.

    Given: JSON bytes holding a UUID's string form, matching what
        encode_uuid produces.
    When: deserialize is called with UUID as the target type directly.
    Then: A UUID equal to the original is returned.
    """
    # Arrange
    value = uuid4()
    data = json.dumps(str(value)).encode()

    # Act
    result = deserialize(data, type=UUID)

    # Assert
    assert result == value


def test_deserialize_does_not_coerce_uuid_field():
    """
    Test that a UUID-typed field nested in a dataclass is left as a raw string.

    Unlike UUID as a raw target type (see test_deserialize_uuid above), a field
    nested inside a dataclass is never individually inspected: dict-shaped
    construction just spreads the whole dict as keyword arguments, so getting
    this field coerced needs `__post_init__` on the dataclass, or
    `deserialize.register` for a class you don't control.

    Given: JSON where a UUID-typed field is present as the string form that
        encode_uuid produces.
    When: deserialize is called with a dataclass declaring that field as UUID.
    Then: The restored field is left as a str, not a UUID.
    """

    # Arrange
    @dataclass
    class Widget:
        id: UUID

    value = uuid4()
    data = json.dumps({"id": str(value)}).encode()

    # Act
    result = deserialize(data, type=Widget)

    # Assert
    assert isinstance(result.id, str)
    assert result != Widget(id=value)


def test_deserialize_does_not_reconstruct_nested_dataclass_field():
    """
    Test that a nested custom-typed field is left as a raw dict.

    Same category as test_deserialize_does_not_coerce_uuid_field: dict-shaped
    construction spreads the whole dict as keyword arguments without
    inspecting any of it, so a field's own declared type is never consulted.
    deserialize is the simple fallback for ordinary serde, not a schema-aware
    decoder like msgspec. Reconstructing this field needs `__post_init__` on
    the dataclass, or `deserialize.register` for a class you don't control.
    This is the same escape hatch as any other field-level coercion.

    Given: A payload produced by serialize() for a dataclass with a nested
        dataclass field.
    When: deserialize is called with the outer dataclass as target.
    Then: The nested field comes back as a plain dict, not an instance of
        the nested dataclass.
    """

    # Arrange
    @dataclass
    class Address:
        city: str

        def to_dict(self):
            return {"city": self.city}

    @dataclass
    class Person:
        name: str
        address: Address

        def to_dict(self):
            return {"name": self.name, "address": self.address}

    original = Person(name="Ada", address=Address(city="London"))
    data = serialize(original)

    # Act
    restored = deserialize(data, type=Person)

    # Assert
    assert isinstance(restored.address, dict)
    assert restored != original


def test_deserialize_does_not_reconstruct_list_field_of_nested_dataclasses():
    """
    Test that a list of nested custom-typed items, as a dataclass field, is left as raw dicts.

    Unlike list[Item] as a raw target type (see test_deserialize_list_of_dataclasses),
    this list is never reached as its own `type` argument. It's just one more
    value inside the dict spread across Order's keyword arguments.

    Given: A payload produced by serialize() for a dataclass with a field
        that's a list of nested dataclass instances.
    When: deserialize is called with the outer dataclass as target.
    Then: The list comes back as a list of plain dicts, not instances of the
        nested dataclass.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

        def to_dict(self):
            return {"sku": self.sku}

    @dataclass
    class Order:
        items: list[Item]

        def to_dict(self):
            return {"items": self.items}

    original = Order(items=[Item(sku="A1"), Item(sku="B2")])
    data = serialize(original)

    # Act
    restored = deserialize(data, type=Order)

    # Assert
    assert all(isinstance(item, dict) for item in restored.items)
    assert restored != original
