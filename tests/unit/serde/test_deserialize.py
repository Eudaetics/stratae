"""Test the deserialize function for various payloads and failure modes."""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from types import NoneType
from typing import Any, Literal, Optional
from uuid import UUID, uuid4

import pytest

from stratae.serde import Deserializer, deserialize, encode, serialize

# region protocol conformance


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


# endregion
# region dataclass registration


def test_deserialize_plain_dataclass():
    """
    Test that a flat dataclass round-trips through deserialize once registered.

    Given: JSON bytes matching a flat dataclass's fields, and a handler
        registered for that dataclass.
    When: deserialize is called with that dataclass as the target type.
    Then: An instance of the dataclass is constructed with matching fields.
    """

    # Arrange
    @dataclass
    class Product:
        name: str
        count: int

    @deserialize.register(Product)
    def _(value: dict[str, Any]) -> Product:
        return Product(**value)

    data = json.dumps({"name": "sprocket", "count": 3}).encode()

    # Act
    result = deserialize(data, type=Product)

    # Assert
    assert result == Product(name="sprocket", count=3)


def test_deserialize_unregistered_dataclass_raises():
    """
    Test that deserializing into a type with no registered handler fails.

    Given: A dataclass with no handler registered via deserialize.register.
    When: deserialize is called with that dataclass as the target type.
    Then: TypeError is raised.
    """

    # Arrange
    @dataclass
    class Product:
        name: str

    data = json.dumps({"name": "sprocket"}).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="no handler registered"):
        deserialize(data, type=Product)


# endregion
# region dict handling


def test_deserialize_unconstrained_dict_type():
    """
    Test that dict[Any, Any] is the explicit way to get an unconstrained mapping back.

    Given: JSON object bytes.
    When: deserialize is called with dict[Any, Any] as the target type.
    Then: A plain dict equal to the JSON payload is returned.
    """
    # Arrange
    data = json.dumps({"a": 1, "b": 2}).encode()

    # Act
    result = deserialize(data, type=dict[Any, Any])

    # Assert
    assert result == {"a": 1, "b": 2}


# endregion
# region decode errors


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


# endregion
# region native shape passthrough


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
    Test that a top-level JSON scalar round-trips when type matches its native shape.

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
    Test that omitting type just decodes the JSON value, equivalent to json.loads.

    Given: JSON bytes of any native shape.
    When: deserialize is called with no type argument.
    Then: The decoded value is returned exactly as json.loads would produce it.
    """
    assert deserialize(data) == expected


# endregion
# region list recursion


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

    Given: A JSON array of objects, and a handler registered for the
        element dataclass.
    When: deserialize is called with list[Item] as the target type.
    Then: Each element is reconstructed as an Item.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

    @deserialize.register(Item)
    def _(value: dict[str, Any]) -> Item:
        return Item(**value)

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


# endregion
# region set recursion


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
    Test that set[UUID] reconstructs each element via UUID's pre-registered handler.

    Given: A JSON array of UUID string forms.
    When: deserialize is called with set[UUID] as the target type.
    Then: A set of UUID instances equal to the originals is returned.
    """
    # Arrange
    values = [uuid4(), uuid4()]
    data = json.dumps([str(v) for v in values]).encode()

    # Act
    result = deserialize(data, type=set[UUID])

    # Assert
    assert result == set(values)


# endregion
# region tuple recursion


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
    Test that tuple[UUID, ...] reconstructs each element via UUID's pre-registered handler.

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
    Test that a fixed-length tuple can mix a pre-registered subtype with primitives.

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


# endregion
# region union and optional


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
    Test that UUID | None reconstructs a UUID via its pre-registered handler.

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


# endregion
# region typing special forms


def test_deserialize_literal_type_raises():
    """
    Test that a typing special form is rejected the same as any other unregistered type.

    Literal isn't a real class. It can never have a registered handler,
    match a structural generic, or be a bare native type.

    Given: JSON bytes holding a plain str.
    When: deserialize is called with a Literal as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps("a").encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=Literal["a", "b"])


# endregion
# region type aliases


type _Coordinates = tuple[float, float]
type _Point = _Coordinates


def test_deserialize_type_alias():
    """
    Test that a PEP 695 type alias resolves to its underlying type with no registration.

    Given: JSON bytes shaped like the alias's underlying type.
    When: deserialize is called with the alias itself as the target type.
    Then: The value is reconstructed as that underlying type.
    """
    # Arrange
    data = json.dumps([1.0, 2.0]).encode()

    # Act
    result = deserialize(data, type=_Coordinates)

    # Assert
    assert result == (1.0, 2.0)


def test_deserialize_chained_type_alias():
    """
    Test that a type alias of a type alias resolves all the way to its underlying type.

    Given: JSON bytes shaped like the underlying type, and a target type
        that's an alias of an alias.
    When: deserialize is called with the outer alias as the target type.
    Then: The value is reconstructed as the underlying type, having
        unwrapped both levels of aliasing.
    """
    # Arrange
    data = json.dumps([1.0, 2.0]).encode()

    # Act
    result = deserialize(data, type=_Point)

    # Assert
    assert result == (1.0, 2.0)


# endregion
# region Any passthrough


def test_deserialize_dict_keeps_nested_values_as_decoded():
    """
    Test that a dict[str, Any] target passes nested object/array values through untouched.

    Given: A JSON object whose values are themselves objects and arrays.
    When: deserialize is called with dict[str, Any] as the target type.
    Then: The nested values come back exactly as json.loads produced them.
    """
    # Arrange
    data = json.dumps({"a": {"nested": 1}, "b": [1, 2, 3]}).encode()

    # Act
    result = deserialize(data, type=dict[str, Any])

    # Assert
    assert result == {"a": {"nested": 1}, "b": [1, 2, 3]}


# endregion
# region registered handler mechanics


def test_deserialize_uses_registered_handler():
    """
    Test that a handler registered for a type is what runs when deserializing it.

    Given: A type with a handler registered via deserialize.register.
    When: deserialize is called with that type as the target.
    Then: The registered handler runs, receiving the already-decoded value.
    """

    # Arrange
    @dataclass
    class Product:
        name: str

    @deserialize.register(Product)
    def _(value: Any) -> Product:
        return Product(name=value["name"].upper())

    data = json.dumps({"name": "sprocket"}).encode()

    # Act
    result = deserialize(data, type=Product)

    # Assert
    assert result == Product(name="SPROCKET")


def test_deserialize_registered_handler_applies_inside_list():
    """
    Test that a registered handler also applies to list[T] elements, not just the top level.

    Given: A type with a handler registered via deserialize.register.
    When: deserialize is called with list[that type] as the target, so the
        type is reached through construct's recursion rather than directly.
    Then: The registered handler runs for each element.
    """

    # Arrange
    @dataclass
    class Product:
        name: str

    @deserialize.register(Product)
    def _(value: Any) -> Product:
        return Product(name=value["name"].upper())

    data = json.dumps([{"name": "sprocket"}, {"name": "cog"}]).encode()

    # Act
    result = deserialize(data, type=list[Product])

    # Assert
    assert result == [Product(name="SPROCKET"), Product(name="COG")]


def test_deserialize_dict_of_dataclasses():
    """
    Test that dict[str, SomeDataclass] reconstructs each value as that dataclass.

    Given: A JSON object whose values are themselves objects, and a handler
        registered for the value dataclass.
    When: deserialize is called with dict[str, Item] as the target type.
    Then: Each value is reconstructed as an Item instance.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

    @deserialize.register(Item)
    def _(value: dict[str, Any]) -> Item:
        return Item(**value)

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
        the type is reached through construct's recursion rather than
        directly.
    Then: The registered handler runs for each value.
    """

    # Arrange
    @dataclass
    class Product:
        name: str

    @deserialize.register(Product)
    def _(value: Any) -> Product:
        return Product(name=value["name"].upper())

    data = json.dumps({"a": {"name": "sprocket"}, "b": {"name": "cog"}}).encode()

    # Act
    result = deserialize(data, type=dict[str, Product])

    # Assert
    assert result == {"a": Product(name="SPROCKET"), "b": Product(name="COG")}


def test_deserialize_handler_covers_subclass_via_mro():
    """
    Test that a handler registered for a base class also covers its subclasses.

    Given: A handler registered for a base class, and a subclass with no
        handler of its own.
    When: deserialize is called with the subclass as the target type.
    Then: The base class's handler is used to decode.
    """

    # Arrange
    @dataclass
    class Product:
        name: str

    class SpecialProduct(Product):
        pass

    @deserialize.register(Product)
    def _(value: dict[str, Any]) -> Product:
        return Product(**value)

    data = json.dumps({"name": "sprocket"}).encode()

    # Act
    result = deserialize(data, type=SpecialProduct)

    # Assert
    assert result == Product(name="sprocket")
    assert type(result) is Product


def test_deserialize_subclass_handler_takes_priority_over_base():
    """
    Test that a handler registered for the subclass itself wins over the base class's.

    Given: Handlers registered for both a base class and a subclass.
    When: deserialize is called with the subclass as the target type.
    Then: The subclass's own handler runs, not the base class's.
    """

    # Arrange
    @dataclass
    class Product:
        name: str

    @dataclass
    class SpecialProduct(Product):
        pass

    @deserialize.register(Product)
    def _(value: dict[str, Any]) -> Product:
        return Product(name=value["name"])

    @deserialize.register(SpecialProduct)
    def _(value: dict[str, Any]) -> SpecialProduct:
        return SpecialProduct(name=value["name"].upper())

    data = json.dumps({"name": "sprocket"}).encode()

    # Act
    result = deserialize(data, type=SpecialProduct)

    # Assert
    assert result == SpecialProduct(name="SPROCKET")


def test_deserialize_register_as_the_escape_hatch_for_field_coercion():
    """
    Test that registering a dataclass itself is how to get its fields coerced.

    Given: A dataclass with a UUID-typed field.
    When: A handler is registered for the dataclass that does the
        conversion itself.
    Then: deserialize produces a fully-coerced instance.
    """

    # Arrange
    @dataclass
    class Product:
        id: UUID

    @deserialize.register(Product)
    def _(value: Any) -> Product:
        return Product(id=UUID(value["id"]))

    original_id = uuid4()
    data = json.dumps({"id": str(original_id)}).encode()

    # Act
    result = deserialize(data, type=Product)

    # Assert
    assert result == Product(id=original_id)
    assert isinstance(result.id, UUID)


def test_deserialize_propagates_validation_error_from_target_init():
    """
    Test that validation errors raised while constructing the target propagate unchanged.

    Given: A registered handler that spreads the payload as keyword
        arguments, and a dataclass whose __post_init__ validates its own
        fields.
    When: deserialize is called with data that fails that validation.
    Then: The validation error propagates unchanged from deserialize.
    """

    # Arrange
    @dataclass
    class Product:
        count: int

        def __post_init__(self):
            if self.count < 0:
                raise ValueError("count must be non-negative")

    @deserialize.register(Product)
    def _(value: dict[str, Any]) -> Product:
        return Product(**value)

    data = json.dumps({"count": -1}).encode()

    # Act & Assert
    with pytest.raises(ValueError, match="count must be non-negative"):
        deserialize(data, type=Product)


# endregion
# region non-recursion into dataclass fields


def test_deserialize_does_not_coerce_uuid_field():
    """
    Test that a UUID-typed field nested in a dataclass is left as a raw string.

    Unlike UUID as a raw target type (see test_deserialize_uuid above), a field
    nested inside a dataclass is never individually inspected: the registered
    handler below just spreads the whole dict as keyword arguments, so getting
    this field coerced needs `__post_init__` on the dataclass, or a smarter
    handler for the outer type.

    Given: A registered handler that spreads the payload as keyword
        arguments, and JSON where a UUID-typed field is present as the
        string form that encode_uuid produces.
    When: deserialize is called with a dataclass declaring that field as UUID.
    Then: The restored field is left as a str, not a UUID.
    """

    # Arrange
    @dataclass
    class Product:
        id: UUID

    @deserialize.register(Product)
    def _(value: dict[str, Any]) -> Product:
        return Product(**value)

    value = uuid4()
    data = json.dumps({"id": str(value)}).encode()

    # Act
    result = deserialize(data, type=Product)

    # Assert
    assert isinstance(result.id, str)
    assert result != Product(id=value)


def test_deserialize_does_not_reconstruct_nested_dataclass_field():
    """
    Test that a nested custom-typed field is left as a raw dict.

    Same category as test_deserialize_does_not_coerce_uuid_field: the
    registered handler spreads the whole dict as keyword arguments without
    inspecting any of it, so a field's own declared type is never consulted.
    deserialize is the simple fallback for ordinary serde, not a schema-aware
    decoder like msgspec. Reconstructing this field needs `__post_init__` on
    the dataclass, or a smarter handler for the outer type.

    Given: A payload produced by serialize() for a dataclass with a nested
        dataclass field, and a registered handler for the outer type that
        spreads the payload as keyword arguments.
    When: deserialize is called with the outer dataclass as target.
    Then: The nested field comes back as a plain dict, not an instance of
        the nested dataclass.
    """

    # Arrange
    @dataclass
    class Address:
        city: str

    @dataclass
    class Person:
        name: str
        address: Address

    @encode.register
    def _(obj: Person) -> dict[str, Any]:
        return asdict(obj)

    @deserialize.register(Person)
    def _(value: dict[str, Any]) -> Person:
        return Person(**value)

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
        that's a list of nested dataclass instances, and a registered
        handler for the outer type that spreads the payload as keyword
        arguments.
    When: deserialize is called with the outer dataclass as target.
    Then: The list comes back as a list of plain dicts, not instances of the
        nested dataclass.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

    @dataclass
    class Order:
        items: list[Item]

    @encode.register
    def _(obj: Order) -> dict[str, Any]:
        return asdict(obj)

    @deserialize.register(Order)
    def _(value: dict[str, Any]) -> Order:
        return Order(**value)

    original = Order(items=[Item(sku="A1"), Item(sku="B2")])
    data = serialize(original)

    # Act
    restored = deserialize(data, type=Order)

    # Assert
    assert all(isinstance(item, dict) for item in restored.items)
    assert restored != original


# endregion
# region int coercion


def test_deserialize_int_accepts_whole_float():
    """
    Test that a whole-numbered float is accepted for an int target.

    Given: JSON bytes holding a whole-numbered float.
    When: deserialize is called with int as the target type.
    Then: The value is returned as an int.
    """
    # Arrange
    data = json.dumps(42.0).encode()

    # Act
    result = deserialize(data, type=int)

    # Assert
    assert result == 42
    assert isinstance(result, int)


def test_deserialize_int_accepts_int_string():
    """
    Test that a string cleanly representing an int is accepted for an int target.

    Given: JSON bytes holding the string "42".
    When: deserialize is called with int as the target type.
    Then: The value is returned as an int.
    """
    # Arrange
    data = json.dumps("42").encode()

    # Act
    result = deserialize(data, type=int)

    # Assert
    assert result == 42
    assert isinstance(result, int)


def test_deserialize_int_accepts_whole_float_string():
    """
    Test that a string representing a whole-numbered float is accepted for an int target.

    Given: JSON bytes holding the string "42.0".
    When: deserialize is called with int as the target type.
    Then: The value is returned as an int.
    """
    # Arrange
    data = json.dumps("42.0").encode()

    # Act
    result = deserialize(data, type=int)

    # Assert
    assert result == 42
    assert isinstance(result, int)


def test_deserialize_int_rejects_fractional_float():
    """
    Test that a fractional float is rejected for an int target.

    Given: JSON bytes holding a fractional float.
    When: deserialize is called with int as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42.7).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="whole number"):
        deserialize(data, type=int)


def test_deserialize_int_rejects_fractional_float_string():
    """
    Test that a string representing a fractional float is rejected for an int target.

    Given: JSON bytes holding the string "42.7".
    When: deserialize is called with int as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps("42.7").encode()

    # Act & Assert
    with pytest.raises(TypeError, match="whole number"):
        deserialize(data, type=int)


def test_deserialize_int_rejects_bool():
    """
    Test that a bool is rejected for an int target.

    Given: JSON bytes holding a bool.
    When: deserialize is called with int as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(True).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=int)


def test_deserialize_int_rejects_non_numeric_string():
    """
    Test that a non-numeric string is rejected for an int target.

    Given: JSON bytes holding the string "hello".
    When: deserialize is called with int as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps("hello").encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=int)


# endregion
# region float coercion


def test_deserialize_float_accepts_int_string():
    """
    Test that a string cleanly representing an int is accepted for a float target.

    Given: JSON bytes holding the string "42".
    When: deserialize is called with float as the target type.
    Then: The value is returned as a float.
    """
    # Arrange
    data = json.dumps("42").encode()

    # Act
    result = deserialize(data, type=float)

    # Assert
    assert result == 42.0
    assert isinstance(result, float)


def test_deserialize_float_accepts_float_string():
    """
    Test that a string representing a float is accepted for a float target.

    Given: JSON bytes holding the string "31.5".
    When: deserialize is called with float as the target type.
    Then: The value is returned as a float.
    """
    # Arrange
    data = json.dumps("31.5").encode()

    # Act
    result = deserialize(data, type=float)

    # Assert
    assert result == 31.5
    assert isinstance(result, float)


def test_deserialize_float_rejects_bool():
    """
    Test that a bool is rejected for a float target.

    Given: JSON bytes holding a bool.
    When: deserialize is called with float as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(False).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=float)


def test_deserialize_float_rejects_non_numeric_string():
    """
    Test that a non-numeric string is rejected for a float target.

    Given: JSON bytes holding the string "hello".
    When: deserialize is called with float as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps("hello").encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=float)


# endregion
# region bool coercion


@pytest.mark.parametrize(
    "value",
    ["true", "True", "TRUE", "tRuE"],
    ids=["lower", "title", "upper", "mixed"],
)
def test_deserialize_bool_accepts_true_string_case_insensitively(value: str):
    """
    Test that "true", in any casing, is accepted for a bool target.

    Given: JSON bytes holding a string spelling "true" in some casing.
    When: deserialize is called with bool as the target type.
    Then: True is returned.
    """
    # Arrange
    data = json.dumps(value).encode()

    # Act
    result = deserialize(data, type=bool)

    # Assert
    assert result is True


@pytest.mark.parametrize(
    "value",
    ["false", "False", "FALSE", "fAlSe"],
    ids=["lower", "title", "upper", "mixed"],
)
def test_deserialize_bool_accepts_false_string_case_insensitively(value: str):
    """
    Test that "false", in any casing, is accepted for a bool target.

    Given: JSON bytes holding a string spelling "false" in some casing.
    When: deserialize is called with bool as the target type.
    Then: False is returned.
    """
    # Arrange
    data = json.dumps(value).encode()

    # Act
    result = deserialize(data, type=bool)

    # Assert
    assert result is False


def test_deserialize_bool_rejects_other_strings():
    """
    Test that a string other than "true"/"false" is rejected for a bool target.

    Given: JSON bytes holding the string "yes".
    When: deserialize is called with bool as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps("yes").encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=bool)


def test_deserialize_bool_rejects_int():
    """
    Test that an int is rejected for a bool target.

    Given: JSON bytes holding an int.
    When: deserialize is called with bool as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(1).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=bool)


# endregion
# region str confirm-only


def test_deserialize_str_rejects_int():
    """
    Test that an int is rejected for a str target.

    Given: JSON bytes holding an int.
    When: deserialize is called with str as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=str)


def test_deserialize_str_rejects_dict():
    """
    Test that a dict is rejected for a str target.

    Given: JSON bytes holding an object.
    When: deserialize is called with str as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps({"a": 1}).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=str)


# endregion
# region NoneType handling


def test_deserialize_bare_none_normalizes_to_nonetype():
    """
    Test that a bare None target normalizes to NoneType.

    Given: JSON bytes holding null.
    When: deserialize is called with None as the target type.
    Then: None is returned.
    """
    # Arrange
    data = json.dumps(None).encode()

    # Act
    result = deserialize(data, type=None)

    # Assert
    assert result is None


def test_deserialize_nonetype_confirms_none():
    """
    Test that NoneType as the target type confirms a null value.

    Given: JSON bytes holding null.
    When: deserialize is called with NoneType as the target type.
    Then: None is returned.
    """
    # Arrange
    data = json.dumps(None).encode()

    # Act
    result = deserialize(data, type=NoneType)

    # Assert
    assert result is None


def test_deserialize_nonetype_rejects_non_none():
    """
    Test that a non-null value is rejected for a NoneType target.

    Given: JSON bytes holding an int.
    When: deserialize is called with NoneType as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=NoneType)


# endregion
# region UUID/datetime/Decimal


def test_deserialize_uuid():
    """
    Test that UUID works as a raw target type, via its pre-registered handler.

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


def test_deserialize_datetime():
    """
    Test that datetime works as a raw target type, via its pre-registered handler.

    Given: JSON bytes holding a datetime's ISO 8601 string form, matching
        what encode_datetime produces.
    When: deserialize is called with datetime as the target type directly.
    Then: A datetime equal to the original is returned.
    """
    # Arrange
    value = datetime(2024, 1, 15, 9, 30)
    data = json.dumps(value.isoformat()).encode()

    # Act
    result = deserialize(data, type=datetime)

    # Assert
    assert result == value


def test_deserialize_decimal():
    """
    Test that Decimal works as a raw target type, via its pre-registered handler.

    Given: JSON bytes holding a Decimal's string form, matching what
        encode_decimal produces.
    When: deserialize is called with Decimal as the target type directly.
    Then: A Decimal equal to the original is returned.
    """
    # Arrange
    value = Decimal("19.99")
    data = json.dumps(str(value)).encode()

    # Act
    result = deserialize(data, type=Decimal)

    # Assert
    assert result == value


def test_deserialize_uuid_accepts_native_instance():
    """
    Test that an already-native UUID instance is accepted for a UUID target.

    Given: A UUID instance, not a string.
    When: deserialize.construct is called with UUID as the target type.
    Then: The same UUID is returned.
    """
    # Arrange
    value = uuid4()

    # Act
    result = deserialize.construct(UUID, value)

    # Assert
    assert result == value


def test_deserialize_uuid_rejects_non_string():
    """
    Test that a non-string, non-UUID value is rejected for a UUID target.

    Given: JSON bytes holding an int.
    When: deserialize is called with UUID as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=UUID)


def test_deserialize_datetime_accepts_native_instance():
    """
    Test that an already-native datetime instance is accepted for a datetime target.

    Given: A datetime instance, not a string.
    When: deserialize.construct is called with datetime as the target type.
    Then: The same datetime is returned.
    """
    # Arrange
    value = datetime(2024, 1, 15, 9, 30)

    # Act
    result = deserialize.construct(datetime, value)

    # Assert
    assert result == value


def test_deserialize_datetime_rejects_non_string():
    """
    Test that a non-string, non-datetime value is rejected for a datetime target.

    Given: JSON bytes holding a list.
    When: deserialize is called with datetime as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps([2024, 1, 15]).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=datetime)


def test_deserialize_decimal_accepts_native_instance():
    """
    Test that an already-native Decimal instance is accepted for a Decimal target.

    Given: A Decimal instance, not a string.
    When: deserialize.construct is called with Decimal as the target type.
    Then: The same Decimal is returned.
    """
    # Arrange
    value = Decimal("19.99")

    # Act
    result = deserialize.construct(Decimal, value)

    # Assert
    assert result == value


def test_deserialize_decimal_rejects_float():
    """
    Test that a raw float is rejected for a Decimal target.

    Given: A float value.
    When: deserialize.construct is called with Decimal as the target type.
    Then: TypeError is raised.
    """
    # Act & Assert
    with pytest.raises(TypeError):
        deserialize.construct(Decimal, 19.99)


def test_deserialize_decimal_rejects_non_string():
    """
    Test that a non-string, non-Decimal value is rejected for a Decimal target.

    Given: JSON bytes holding a dict.
    When: deserialize is called with Decimal as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps({"a": 1}).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=Decimal)


# endregion
# region container interconversion


def test_deserialize_bare_dict_confirms_dict():
    """
    Test that a dict value is confirmed for a bare dict target.

    Given: JSON object bytes.
    When: deserialize is called with dict, unsubscripted, as the target type.
    Then: A dict equal to the JSON payload is returned.
    """
    # Arrange
    data = json.dumps({"a": 1, "b": 2}).encode()

    # Act
    result = deserialize(data, type=dict)  # pyright: ignore[reportUnknownVariableType]

    # Assert
    assert result == {"a": 1, "b": 2}


def test_deserialize_bare_dict_rejects_non_dict():
    """
    Test that a non-dict value is rejected for a bare dict target.

    Given: JSON bytes holding an int.
    When: deserialize is called with dict, unsubscripted, as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=dict)


def test_deserialize_bare_list_confirms_list():
    """
    Test that a list value is confirmed for a bare list target.

    Given: A list value.
    When: deserialize.construct is called with list as the target type.
    Then: The same list is returned.
    """
    # Act
    result = deserialize.construct(list, [1, 2, 3])

    # Assert
    assert result == [1, 2, 3]


def test_deserialize_bare_list_rejects_incompatible_value():
    """
    Test that a value with no container form is rejected for a bare list target.

    Given: JSON bytes holding an int.
    When: deserialize is called with list, unsubscripted, as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=list)


def test_deserialize_bare_tuple_confirms_tuple():
    """
    Test that a tuple value is confirmed for a bare tuple target.

    Given: A tuple value.
    When: deserialize.construct is called with tuple as the target type.
    Then: The same tuple is returned.
    """
    # Act
    result = deserialize.construct(tuple, (1, 2, 3))

    # Assert
    assert result == (1, 2, 3)


def test_deserialize_bare_tuple_rejects_incompatible_value():
    """
    Test that a value with no container form is rejected for a bare tuple target.

    Given: JSON bytes holding an int.
    When: deserialize is called with tuple, unsubscripted, as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=tuple)


def test_deserialize_bare_set_confirms_set():
    """
    Test that a set value is confirmed for a bare set target.

    Given: A set value.
    When: deserialize.construct is called with set as the target type.
    Then: The same set is returned.
    """
    # Act
    result = deserialize.construct(set, {1, 2, 3})

    # Assert
    assert result == {1, 2, 3}


def test_deserialize_bare_set_rejects_incompatible_value():
    """
    Test that a value with no container form is rejected for a bare set target.

    Given: JSON bytes holding an int.
    When: deserialize is called with set, unsubscripted, as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=set)


def test_deserialize_bare_frozenset_confirms_frozenset():
    """
    Test that a frozenset value is confirmed for a bare frozenset target.

    Given: A frozenset value.
    When: deserialize.construct is called with frozenset as the target type.
    Then: The same frozenset is returned.
    """
    # Act
    result = deserialize.construct(frozenset, frozenset({1, 2, 3}))

    # Assert
    assert result == frozenset({1, 2, 3})


def test_deserialize_bare_frozenset_rejects_incompatible_value():
    """
    Test that a value with no container form is rejected for a bare frozenset target.

    Given: JSON bytes holding an int.
    When: deserialize is called with frozenset, unsubscripted, as the target type.
    Then: TypeError is raised.
    """
    # Arrange
    data = json.dumps(42).encode()

    # Act & Assert
    with pytest.raises(TypeError):
        deserialize(data, type=frozenset)


def test_deserialize_list_accepts_tuple_source():
    """
    Test that a tuple-sourced value is accepted for a list target.

    Given: A tuple value.
    When: deserialize.construct is called with list as the target type.
    Then: A list with the same elements is returned.
    """
    # Act
    result = deserialize.construct(list, (1, 2, 3))

    # Assert
    assert result == [1, 2, 3]


def test_deserialize_list_accepts_set_source():
    """
    Test that a set-sourced value is accepted for a list target.

    Given: A set value.
    When: deserialize.construct is called with list as the target type.
    Then: A list with the same elements is returned.
    """
    # Act
    result = deserialize.construct(list, {1, 2, 3})

    # Assert
    assert sorted(result) == [1, 2, 3]


def test_deserialize_list_accepts_frozenset_source():
    """
    Test that a frozenset-sourced value is accepted for a list target.

    Given: A frozenset value.
    When: deserialize.construct is called with list as the target type.
    Then: A list with the same elements is returned.
    """
    # Act
    result = deserialize.construct(list, frozenset({1, 2, 3}))

    # Assert
    assert sorted(result) == [1, 2, 3]


def test_deserialize_tuple_accepts_list_source():
    """
    Test that a list-sourced value is accepted for a tuple target.

    Given: A list value.
    When: deserialize.construct is called with tuple as the target type.
    Then: A tuple with the same elements is returned.
    """
    # Act
    result = deserialize.construct(tuple, [1, 2, 3])

    # Assert
    assert result == (1, 2, 3)


def test_deserialize_tuple_accepts_set_source():
    """
    Test that a set-sourced value is accepted for a tuple target.

    Given: A set value.
    When: deserialize.construct is called with tuple as the target type.
    Then: A tuple with the same elements is returned.
    """
    # Act
    result = deserialize.construct(tuple, {1, 2, 3})

    # Assert
    assert sorted(result) == [1, 2, 3]


def test_deserialize_tuple_accepts_frozenset_source():
    """
    Test that a frozenset-sourced value is accepted for a tuple target.

    Given: A frozenset value.
    When: deserialize.construct is called with tuple as the target type.
    Then: A tuple with the same elements is returned.
    """
    # Act
    result = deserialize.construct(tuple, frozenset({1, 2, 3}))

    # Assert
    assert sorted(result) == [1, 2, 3]


def test_deserialize_set_accepts_tuple_source():
    """
    Test that a tuple-sourced value is accepted for a set target.

    Given: A tuple value.
    When: deserialize.construct is called with set as the target type.
    Then: A set with the same elements is returned.
    """
    # Act
    result = deserialize.construct(set, (1, 2, 3))

    # Assert
    assert result == {1, 2, 3}


def test_deserialize_set_accepts_frozenset_source():
    """
    Test that a frozenset-sourced value is accepted for a set target.

    Given: A frozenset value.
    When: deserialize.construct is called with set as the target type.
    Then: A set with the same elements is returned.
    """
    # Act
    result = deserialize.construct(set, frozenset({1, 2, 3}))

    # Assert
    assert result == {1, 2, 3}


def test_deserialize_frozenset_accepts_list_source():
    """
    Test that a list-sourced value is accepted for a frozenset target.

    Given: A list value.
    When: deserialize.construct is called with frozenset as the target type.
    Then: A frozenset with the same elements is returned.
    """
    # Act
    result = deserialize.construct(frozenset, [1, 2, 3])

    # Assert
    assert result == frozenset({1, 2, 3})


def test_deserialize_frozenset_accepts_tuple_source():
    """
    Test that a tuple-sourced value is accepted for a frozenset target.

    Given: A tuple value.
    When: deserialize.construct is called with frozenset as the target type.
    Then: A frozenset with the same elements is returned.
    """
    # Act
    result = deserialize.construct(frozenset, (1, 2, 3))

    # Assert
    assert result == frozenset({1, 2, 3})


def test_deserialize_frozenset_accepts_set_source():
    """
    Test that a set-sourced value is accepted for a frozenset target.

    Given: A set value.
    When: deserialize.construct is called with frozenset as the target type.
    Then: A frozenset with the same elements is returned.
    """
    # Act
    result = deserialize.construct(frozenset, {1, 2, 3})

    # Assert
    assert result == frozenset({1, 2, 3})


# endregion
# region frozenset recursion


def test_deserialize_bare_frozenset_from_json():
    """
    Test that a bare frozenset target works directly from JSON bytes.

    Given: JSON bytes holding an array.
    When: deserialize is called with frozenset as the target type.
    Then: A frozenset with the same elements is returned.
    """
    # Arrange
    data = json.dumps([1, 2, 3]).encode()

    # Act
    result = deserialize(data, type=frozenset)  # pyright: ignore[reportUnknownVariableType]

    # Assert
    assert result == frozenset({1, 2, 3})


def test_deserialize_parameterized_frozenset():
    """
    Test that frozenset[T] reconstructs each element as T.

    Given: JSON bytes holding an array of ints.
    When: deserialize is called with frozenset[int] as the target type.
    Then: A frozenset of ints equal to the original values is returned.
    """
    # Arrange
    data = json.dumps([1, 2, 3]).encode()

    # Act
    result = deserialize(data, type=frozenset[int])

    # Assert
    assert result == frozenset({1, 2, 3})


# endregion
# region nested generic combinations


def test_deserialize_dict_of_list_of_set_of_int():
    """
    Test that dict[str, list[set[int]]] recurses through all three levels.

    Given: JSON bytes shaped like a dict of lists of int arrays.
    When: deserialize is called with dict[str, list[set[int]]] as the target type.
    Then: Each innermost array is reconstructed as a set of ints.
    """
    # Arrange
    data = json.dumps({"a": [[1, 2], [3]], "b": [[4, 5, 6]]}).encode()

    # Act
    result = deserialize(data, type=dict[str, list[set[int]]])

    # Assert
    assert result == {"a": [{1, 2}, {3}], "b": [{4, 5, 6}]}


def test_deserialize_list_of_dict_of_str_int():
    """
    Test that list[dict[str, int]] recurses through both levels.

    Given: JSON bytes holding an array of objects mapping strings to ints.
    When: deserialize is called with list[dict[str, int]] as the target type.
    Then: A list of dicts with int values is returned.
    """
    # Arrange
    data = json.dumps([{"a": 1}, {"b": 2, "c": 3}]).encode()

    # Act
    result = deserialize(data, type=list[dict[str, int]])

    # Assert
    assert result == [{"a": 1}, {"b": 2, "c": 3}]


def test_deserialize_tuple_of_list_and_set():
    """
    Test that a fixed-length tuple[list[int], set[str]] reconstructs each element's own type.

    Given: JSON bytes holding a two-element array: an int array and a string array.
    When: deserialize is called with tuple[list[int], set[str]] as the target type.
    Then: The first element is a list of ints, the second a set of strs.
    """
    # Arrange
    data = json.dumps([[1, 2, 3], ["a", "b"]]).encode()

    # Act
    result = deserialize(data, type=tuple[list[int], set[str]])

    # Assert
    assert result == ([1, 2, 3], {"a", "b"})


def test_deserialize_dict_of_tuple_of_int():
    """
    Test that dict[str, tuple[int, ...]] recurses through both levels.

    Given: JSON bytes holding a dict of int arrays.
    When: deserialize is called with dict[str, tuple[int, ...]] as the target type.
    Then: Each value is reconstructed as a tuple of ints.
    """
    # Arrange
    data = json.dumps({"a": [1, 2], "b": [3, 4, 5]}).encode()

    # Act
    result = deserialize(data, type=dict[str, tuple[int, ...]])

    # Assert
    assert result == {"a": (1, 2), "b": (3, 4, 5)}


def test_deserialize_list_of_optional_int():
    """
    Test that list[int | None] passes None through and reconstructs int elements.

    Given: JSON bytes holding an array mixing ints and nulls.
    When: deserialize is called with list[int | None] as the target type.
    Then: A list mixing ints and None, matching the original positions, is returned.
    """
    # Arrange
    data = json.dumps([1, None, 2, None]).encode()

    # Act
    result = deserialize(data, type=list[int | None])

    # Assert
    assert result == [1, None, 2, None]


def test_deserialize_dict_of_optional_int():
    """
    Test that dict[str, int | None] passes None through and reconstructs int values.

    Given: JSON bytes holding a dict mixing int and null values.
    When: deserialize is called with dict[str, int | None] as the target type.
    Then: A dict mixing ints and None, matching the original keys, is returned.
    """
    # Arrange
    data = json.dumps({"a": 1, "b": None}).encode()

    # Act
    result = deserialize(data, type=dict[str, int | None])

    # Assert
    assert result == {"a": 1, "b": None}


def test_deserialize_set_of_frozenset():
    """
    Test that set[frozenset[int]] reconstructs each inner array as a frozenset.

    Given: JSON bytes holding an array of int arrays.
    When: deserialize is called with set[frozenset[int]] as the target type.
    Then: A set of frozensets, each matching an original inner array, is returned.
    """
    # Arrange
    data = json.dumps([[1, 2], [3, 4], [5]]).encode()

    # Act
    result = deserialize(data, type=set[frozenset[int]])

    # Assert
    assert result == {frozenset({1, 2}), frozenset({3, 4}), frozenset({5})}


def test_deserialize_triple_nested_list():
    """
    Test that list[list[list[int]]] recurses through all three levels.

    Given: JSON bytes holding a three-levels-deep nested array of ints.
    When: deserialize is called with list[list[list[int]]] as the target type.
    Then: The nested structure is reconstructed unchanged.
    """
    # Arrange
    data = json.dumps([[[1, 2], [3]], [[4]]]).encode()

    # Act
    result = deserialize(data, type=list[list[list[int]]])

    # Assert
    assert result == [[[1, 2], [3]], [[4]]]


# endregion
# region coercion applies during recursion


def test_deserialize_list_of_int_from_string_shaped_elements():
    """
    Test that list[int] coerces each element from a numeric string.

    Given: JSON bytes holding an array of numeric strings.
    When: deserialize is called with list[int] as the target type.
    Then: A list of ints, each coerced from its string element, is returned.
    """
    # Arrange
    data = json.dumps(["1", "2", "3"]).encode()

    # Act
    result = deserialize(data, type=list[int])

    # Assert
    assert result == [1, 2, 3]


def test_deserialize_dict_of_str_bool_from_string_values():
    """
    Test that dict[str, bool] coerces each value from a "true"/"false" string.

    Given: JSON bytes holding a dict whose values are "true"/"false" strings.
    When: deserialize is called with dict[str, bool] as the target type.
    Then: A dict with real bool values, matching the original keys, is returned.
    """
    # Arrange
    data = json.dumps({"active": "false", "admin": "true"}).encode()

    # Act
    result = deserialize(data, type=dict[str, bool])

    # Assert
    assert result == {"active": False, "admin": True}


# endregion
