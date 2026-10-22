"""
Test deserialize.register and the dispatch mechanics built on top of it.

Covers registering a handler for a dataclass, the TypeError raised for an
unregistered type, a registered handler running wherever its type is
reached (top-level, inside a list, inside a dict), MRO fallback to a base
class's handler, a subclass's own handler taking priority, using
registration as the escape hatch for field coercion, validation errors
propagating unchanged, and the fact that a handler which just spreads a
dict as keyword arguments never recurses into its own fields.
"""

import json
from dataclasses import asdict, dataclass
from typing import Any
from uuid import UUID, uuid4

import pytest

from stratae.serde import deserialize, encode, serialize


def test_deserialize_plain_dataclass():
    """
    A flat dataclass round-trips through deserialize once registered.

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
    Deserializing into a type with no registered handler fails.

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


def test_deserialize_uses_registered_handler():
    """
    A handler registered for a type is what runs when deserializing it.

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
    A registered handler also applies to list[T] elements, not just the top level.

    Given: A type with a handler registered via deserialize.register.
    When: deserialize is called with list[that type] as the target.
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
    dict[str, SomeDataclass] reconstructs each value as that dataclass.

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
    A registered handler also applies to dict[str, T] values, not just the top level.

    Given: A type with a handler registered via deserialize.register.
    When: deserialize is called with dict[str, that type] as the target.
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
    A handler registered for a base class also covers its subclasses.

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
    A handler registered for the subclass itself wins over the base class's.

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
    Registering a dataclass itself is how to get its fields coerced.

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
    Validation errors raised while constructing the target propagate unchanged.

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


def test_deserialize_does_not_coerce_uuid_field():
    """
    A UUID-typed field nested in a dataclass is left as a raw string.

    Unlike UUID as a raw target type, a field nested inside a dataclass is
    never individually inspected: the registered handler below just spreads
    the whole dict as keyword arguments, so getting this field coerced needs
    `__post_init__` on the dataclass, or a smarter handler for the outer type.

    Given: A registered handler that spreads the payload as keyword
        arguments, and JSON where a UUID-typed field is present as a string.
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
    A nested custom-typed field is left as a raw dict.

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
    A list of nested custom-typed items, as a dataclass field, is left as raw dicts.

    Unlike list[Item] as a raw target type, this list is never reached as
    its own `type` argument. It's just one more value inside the dict
    spread across Order's keyword arguments.

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


def test_deserializer_deregister():
    """
    Deregistering a handler for a type should remove it from the deserializer.

    Given: A deserializer with a handler registered for a type.
    When: `deregister` is called for that type.
    Then: It should remove the handler and no longer handle that type directly.
    """

    # Arrange
    @dataclass
    class Item:
        sku: str

    @deserialize.register(Item)
    def _(value: dict[str, Any]) -> Item:
        return Item(**value)

    data = '{"sku": "test"}'.encode()
    restored = deserialize(data, type=Item)

    # Act
    deserialize.deregister(Item)

    # Assert
    assert isinstance(restored, Item)
    with pytest.raises(TypeError, match="Cannot deserialize into.*Item.*: no handler registered"):
        deserialize(data, type=Item)


def test_deserializer_deregister_no_handler():
    """
    Deregistering a handler for a type without a handler should raise.

    Given: A deserializer,
    When: `deregister` is called for a type that is not registered,
    Then: A KeyError should raise
    """

    class Item:
        sku: str

    with pytest.raises(ValueError, match="Cannot deregister.*Item.*no handler registered"):
        deserialize.deregister(Item)
