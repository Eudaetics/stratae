"""
Test construct's routing and recursion: what happens before or around a registered handler.

Covers resolving type aliases and rejecting typing special forms like
Literal, normalizing a bare None target to NoneType, recursing into a
union's single non-None member, recursing element-wise/value-wise into the
structural generics (list, set, tuple, dict, frozenset) including
arbitrarily nested combinations of them, and the fact that per-type
coercion (e.g. numeric strings into int) still applies to every element
reached this way, not just a bare top-level target.
"""

import json
from dataclasses import dataclass
from typing import Any, Literal, Optional
from uuid import UUID, uuid4

import pytest

from stratae.serde import deserialize

# region type resolution: aliases, special forms, and None normalization


type _Coordinates = tuple[float, float]
type _Point = _Coordinates


def test_deserialize_type_alias():
    """
    A PEP 695 type alias resolves to its underlying type with no registration.

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
    A type alias of a type alias resolves all the way to its underlying type.

    Given: JSON bytes shaped like the underlying type, and a target type
        that's an alias of an alias.
    When: deserialize is called with the outer alias as the target type.
    Then: The value is reconstructed as the underlying type.
    """
    # Arrange
    data = json.dumps([1.0, 2.0]).encode()

    # Act
    result = deserialize(data, type=_Point)

    # Assert
    assert result == (1.0, 2.0)


def test_deserialize_literal_type_raises():
    """
    A typing special form is rejected the same as any other unregistered type.

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


def test_deserialize_bare_none_normalizes_to_nonetype():
    """
    A bare None target normalizes to NoneType.

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


# endregion
# region union and optional


def test_deserialize_optional_with_value():
    """
    X | None reconstructs the value against its non-None member.

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
    X | None passes a JSON null straight through as None.

    Given: JSON bytes that are the literal null.
    When: deserialize is called with int | None as the target type.
    Then: None is returned.
    """
    # Act
    result = deserialize(b"null", type=int | None)

    # Assert
    assert result is None


def test_deserialize_optional_uuid():
    """
    UUID | None reconstructs a UUID via its pre-registered handler.

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
    typing.Optional[X] resolves through the same path as X | None.

    Given: JSON bytes holding a plain str.
    When: deserialize is called with typing.Optional[str] as the target type.
    Then: The str is returned.
    """
    # Arrange
    data = json.dumps("hi").encode()

    # Act
    result = deserialize(data, type=Optional[str])

    # Assert
    assert result == "hi"


def test_deserialize_ambiguous_union_raises():
    """
    A union with more than one non-None member is rejected.

    Given: JSON bytes holding a plain int.
    When: deserialize is called with int | str as the target type.
    Then: TypeError is raised for the ambiguous union.
    """
    # Arrange
    data = json.dumps(1).encode()

    # Act & Assert
    with pytest.raises(TypeError, match="ambiguous union"):
        deserialize(data, type=int | str)


# endregion
# region parameterized generic recursion


def test_deserialize_unconstrained_dict_type():
    """
    dict[Any, Any] is the explicit way to get an unconstrained mapping back.

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


def test_deserialize_dict_keeps_nested_values_as_decoded():
    """
    A dict[str, Any] target passes nested object/array values through untouched.

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
    A parameterized list[T] target reconstructs each element as T.

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
    list[SomeDataclass] reconstructs each element as that dataclass.

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
    An empty JSON array with a list[T] target returns an empty list.

    Given: An empty JSON array.
    When: deserialize is called with list[Item] as the target type.
    Then: An empty list is returned.
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
    list[list[T]] recurses through both levels of nesting.

    Given: A JSON array of arrays.
    When: deserialize is called with list[list[int]] as the target type.
    Then: The nested structure is reconstructed unchanged.
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
    A parameterized set[T] target reconstructs each element as T.

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
    set[UUID] reconstructs each element via UUID's pre-registered handler.

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


def test_deserialize_tuple_variadic():
    """
    tuple[T, ...] reconstructs a variable-length tuple of T.

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
    tuple[UUID, ...] reconstructs each element via UUID's pre-registered handler.

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
    tuple[T1, T2, T3] reconstructs a fixed-length heterogeneous tuple.

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
    A fixed-length tuple can mix a pre-registered subtype with primitives.

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
    A fixed-length tuple target rejects a mismatched element count.

    Given: A JSON array with fewer elements than the declared tuple type.
    When: deserialize is called with a fixed-length tuple target.
    Then: ValueError is raised for the length mismatch.
    """
    # Arrange
    data = json.dumps([1, "two"]).encode()

    # Act & Assert
    with pytest.raises(ValueError, match="zip"):
        deserialize(data, type=tuple[int, str, bool])


def test_deserialize_bare_frozenset_from_json():
    """
    A bare frozenset target works directly from JSON bytes.

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
    frozenset[T] reconstructs each element as T.

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
    dict[str, list[set[int]]] recurses through all three levels.

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
    list[dict[str, int]] recurses through both levels.

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
    A fixed-length tuple[list[int], set[str]] reconstructs each element's own type.

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
    dict[str, tuple[int, ...]] recurses through both levels.

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
    list[int | None] passes None through and reconstructs int elements.

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
    dict[str, int | None] passes None through and reconstructs int values.

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
    set[frozenset[int]] reconstructs each inner array as a frozenset.

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
    list[list[list[int]]] recurses through all three levels.

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
    list[int] coerces each element from a numeric string.

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
    dict[str, bool] coerces each value from a "true"/"false" string.

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
