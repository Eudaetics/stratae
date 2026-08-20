"""
Serialization and deserialization tools for encoding/decoding data.

{py:func}`serialize` turns a payload into bytes, using {py:func}`encode` to
convert individual fields that aren't natively JSON-serializable.
{py:class}`Serializer` is the structural protocol for adapters to use as the
interface for using a serializer. {py:class}`Deserializer` is the structural
counterpart on the decode side. It is a type-directed deserializer shaped like
`msgspec.json.decode(data, type=T)`. {py:obj}`deserialize` is the default,
dependency-free implementation. Register a type with
`@encode.register`/`@serialize.register` on the way out and
`@deserialize.register` on the way back in. See
{py:mod}`stratae.integrations.msgspec` for a faster `serialize` registered
for `msgspec.Struct` payloads.

````{example} Round-tripping a dataclass through serialize and deserialize
```{code-block} python
from dataclasses import asdict, dataclass
from uuid import UUID
from stratae.serde import serialize, deserialize, encode

@dataclass
class Invoice:
    id: UUID
    customer: str

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@encode.register
def encode_invoice(obj: Invoice) -> dict:
    return asdict(obj)

@deserialize.register(Invoice)
def decode_invoice(value: dict) -> Invoice:
    return Invoice(**value)

invoice = Invoice(
    id=UUID("47e511ef-f16c-4699-98db-a0d44abcab90"), customer="Acme Corp"
)
data = serialize(invoice)
print(data)

restored = deserialize(data, type=Invoice)
print(restored == invoice)
```
```{output}
b'{"id": "47e511ef-f16c-4699-98db-a0d44abcab90", "customer": "Acme Corp"}'
True
```
````

See {py:func}`encode`, {py:func}`serialize`, {py:class}`Deserializer`, and
{py:obj}`deserialize` for the rest of the module's API.

"""

import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import singledispatch
from types import NoneType, UnionType
from typing import (
    Any,
    Callable,
    Literal,
    Protocol,
    TypeAliasType,
    Union,
    cast,
    get_args,
    get_origin,
    overload,
)
from uuid import UUID

__all__ = ["encode", "serialize", "Serializer", "Deserializer", "deserialize"]


@singledispatch
def encode(obj: object) -> Any:
    """
    Encode a field value for serialization.

    Uses `functools.singledispatch`; register additional types via
    `@encode.register`. Pre-registered with `UUID` ({py:func}`encode_uuid`),
    `datetime` ({py:func}`encode_datetime`), and `Decimal`
    ({py:func}`encode_decimal`). Used as the `default` hook by
    {py:func}`serialize`.

    :param obj: The value to encode.
    :returns: A JSON-serializable representation of `obj`.
    :raises TypeError: If `obj` has no registered encoder.
    """
    raise TypeError(f"Object of type {type(obj)} is not encodable")


@encode.register
def encode_uuid(obj: UUID) -> str:
    """
    Encode a `UUID` as its string representation.

    Pre-registered on {py:func}`encode` as a common default. Overwrite it
    with `@encode.register` if this doesn't match your requirements.

    :param obj: The UUID to encode.
    :returns: The string form of `obj`.
    """
    return str(obj)


@encode.register
def encode_datetime(obj: datetime) -> str:
    """
    Encode a `datetime` as an ISO 8601 string.

    Pre-registered on {py:func}`encode` as a common default. Overwrite it
    with `@encode.register` if this doesn't match your requirements.

    :param obj: The datetime to encode.
    :returns: The ISO 8601 string form of `obj`.
    """
    return obj.isoformat()


@encode.register
def encode_decimal(obj: Decimal) -> str:
    """
    Encode a `Decimal` as its string form.

    Pre-registered on {py:func}`encode` as a common default. Overwrite it
    with `@encode.register` if this doesn't match your requirements.

    :param obj: The decimal to encode.
    :returns: The string form of `obj`.
    """
    return str(obj)


class Serializer[T](Protocol):
    """
    Structural protocol for a payload serializer.

    Shaped after the call signature of {py:func}`serialize`. A caller can
    type a parameter against this protocol instead of a bare `Callable` and
    swap the serializer out entirely, for example passing
    `msgspec.json.encode` directly and bypassing {py:func}`serialize`'s own
    `singledispatch` registration.
    """

    def __call__(self, obj: T, /) -> bytes:
        """
        Serialize `obj` to bytes.

        :param obj: The payload to serialize.
        :returns: The serialized payload as bytes.
        """
        ...


@singledispatch
def serialize(obj: object) -> bytes:
    """
    Serialize a payload to bytes.

    Falls back to `json.dumps`, using {py:func}`encode` as the field-level
    hook for types not natively serializable by `json`. Register faster or
    format-specific paths for particular payload types via
    `@serialize.register`; see {py:mod}`stratae.integrations.msgspec` for a
    registered path that uses `msgspec.json.encode` for `msgspec.Struct`
    payloads.

    :param obj: The payload to serialize.
    :returns: The serialized payload as bytes.
    """
    return json.dumps(obj, default=encode).encode()


class Deserializer(Protocol):
    """
    Structural protocol for a type-directed deserializer.

    Shaped after the call signature of `msgspec.json.decode(data, type=T)`.
    Adapters for other tools can be written as lambdas or thin wrappers
    around that tool's own decode function. {py:obj}`deserialize` is the
    default, dependency-free implementation.
    """

    def __call__[S](self, data: bytes, /, *, type: type[S]) -> S:
        """
        Deserialize `data` into an instance of `type`.

        :param data: The raw bytes to decode.
        :param type: The type to reconstruct.
        :returns: The reconstructed `type` instance.
        """
        ...


_UNSET: Any = object()


def _decode_any(value: Any) -> Any:
    """Return `value` unchanged; the handler pre-registered for `Any`."""
    return value


class Deserialize:
    """
    Simple implementation of the `Deserializer` protocol for per-type registration.

    Since `functools.singledispatch` does not work by type in the way needed for
    deserialization, this creates a similar registration-based method for
    adding handlers on a per-type basis.
    """

    def __init__(self) -> None:
        self._handlers: dict[Any, tuple[Callable[..., Any], bool]] = {
            Any: (_decode_any, False),
        }
        self._origin_constructors: dict[Any, Callable[[tuple[Any, ...], Any], Any]] = {
            list: self._construct_list,
            set: self._construct_set,
            frozenset: self._construct_frozenset,
            tuple: self._construct_tuple,
            dict: self._construct_dict,
            UnionType: self._construct_union,
            Union: self._construct_union,
        }

    @overload
    def __call__(self, data: bytes, /) -> Any: ...
    @overload
    def __call__[S](self, data: bytes, /, *, type: type[S]) -> S: ...
    @overload
    def __call__(self, data: bytes, /, *, type: None) -> None: ...
    @overload
    def __call__(self, data: bytes, /, *, type: UnionType) -> Any: ...
    @overload
    def __call__(self, data: bytes, /, *, type: TypeAliasType) -> Any: ...
    def __call__(self, data: bytes, /, *, type: Any = _UNSET) -> Any:
        """
        Deserialize JSON bytes, optionally into an instance of `type`.

        Decodes `data` exactly once. With no `type`, returns that decoded
        value as-is, the same as plain `json.loads`.

        A handler registered via {py:meth}`register` runs first, if one exists
        for `type` or one of its parents in the mro. Otherwise `construct`
        recurses element-wise for typed containers and similar structures.

        :param data: The raw JSON bytes to decode.
        :param type: The type to reconstruct, if any.
        :returns: The decoded value, or the reconstructed `type` instance.
        """
        value = json.loads(data)
        if type is _UNSET:
            return value
        return self.construct(type, value)

    def construct(self, type_: Any, value: Any, /) -> Any:
        """
        Construct `type` from an already-decoded JSON `value`.

        The registry is checked first, before anything else. A handler
        registered for a type applies everywhere that type gets
        constructed, a `list[T]` element or a `dict[K, V]` value included,
        not just `__call__`'s own `type` argument directly. `Any` is
        pre-registered there too, returning `value` unchanged. `Any` means
        "no constraint," not "a type named Any to construct."

        :param type: The type to reconstruct.
        :param value: The decoded JSON value to construct it from.
        :returns: The reconstructed `type` instance.
        :raises TypeError: If `type` has no registered handler and isn't
            one of the structural generics `construct` already knows how to
            recurse into.
        """
        if found := self._lookup_handler(type_):
            handler, takes_type = found
            return handler(value, type_) if takes_type else handler(value)
        if isinstance(type_, TypeAliasType):
            return self.construct(type_.__value__, value)
        if constructor := self._origin_constructors.get(get_origin(type_)):
            return constructor(get_args(type_), value)
        raise TypeError(
            f"Cannot deserialize into {type_!r}: no handler registered for it. "
            f"Register one with @deserialize.register."
        )

    def _lookup_handler(self, type: Any) -> tuple[Callable[..., Any], bool] | None:
        """
        Look up a registered handler for `type`, walking its MRO.

        Mirrors `functools.singledispatch`'s own dispatch: a handler
        registered for a base class also covers its subclasses, unless a
        more specific handler is registered for the subclass itself. `type`
        may not be a real class at all (a `UnionType`, a generic alias,
        `Any`), in which case it has no `__mro__` and only the exact lookup
        applies.
        """
        if found := self._handlers.get(type):
            return found
        for base in getattr(type, "__mro__", ())[1:]:
            if found := self._handlers.get(base):
                return found
        return None

    def _construct_list(self, args: tuple[Any, ...], value: Any) -> list[Any]:
        """Recurse into each element of a `list[T]` target."""
        (item_type,) = args
        return [self.construct(item_type, item) for item in value]

    def _construct_set(self, args: tuple[Any, ...], value: Any) -> set[Any]:
        """Recurse into each element of a `set[T]` target."""
        (item_type,) = args
        return {self.construct(item_type, item) for item in value}

    def _construct_frozenset(self, args: tuple[Any, ...], value: Any) -> frozenset[Any]:
        """Recurse into each element of a `frozenset[T]` target."""
        (item_type,) = args
        return frozenset(self.construct(item_type, item) for item in value)

    def _construct_tuple(self, args: tuple[Any, ...], value: Any) -> tuple[Any, ...]:
        """Recurse into each element of a `tuple[...]` target, fixed-length or variadic."""
        if len(args) == 2 and args[1] is Ellipsis:
            item_type = args[0]
            return tuple(self.construct(item_type, item) for item in value)
        return tuple(
            self.construct(item_type, item) for item_type, item in zip(args, value, strict=True)
        )

    def _construct_dict(self, args: tuple[Any, ...], value: Any) -> dict[Any, Any]:
        """Recurse into each key and value of a `dict[K, V]` target."""
        key_type, value_type = args
        return {
            self.construct(key_type, key): self.construct(value_type, item)
            for key, item in value.items()
        }

    def _construct_union(self, args: tuple[Any, ...], value: Any) -> Any:
        """Recurse against a union's one non-None member; None passes through unchanged."""
        if value is None:
            return None
        members = [arg for arg in args if arg is not type(None)]
        if len(members) == 1:
            return self.construct(members[0], value)
        raise TypeError(f"Cannot deserialize an ambiguous union with multiple members: {args!r}")

    @overload
    def register[F: Callable[[Any], Any]](
        self, cls: type[Any], func: None = None, /, *, takes_type: Literal[False] = False
    ) -> Callable[[F], F]: ...
    @overload
    def register[F: Callable[[Any], Any]](
        self, cls: type[Any], func: F, /, *, takes_type: Literal[False] = False
    ) -> F: ...
    @overload
    def register[F: Callable[[Any, type[Any]], Any]](
        self, cls: type[Any], func: None = None, /, *, takes_type: Literal[True]
    ) -> Callable[[F], F]: ...
    @overload
    def register[F: Callable[[Any, type[Any]], Any]](
        self, cls: type[Any], func: F, /, *, takes_type: Literal[True]
    ) -> F: ...
    @overload
    def register[F: Callable[[Any], Any]](
        self, cls: UnionType | TypeAliasType | None, func: None = None, /
    ) -> Callable[[F], F]: ...
    @overload
    def register[F: Callable[[Any], Any]](
        self, cls: UnionType | TypeAliasType | None, func: F, /
    ) -> F: ...
    def register[F: Callable[..., Any]](
        self, cls: Any, func: F | None = None, /, *, takes_type: bool = False
    ) -> Callable[[F], F] | F:
        """
        Register a handler that constructs `cls` from an already-decoded value.

        `cls` doesn't have to be a plain class. A `UnionType` like `int | str`
        or a `TypeAliasType` from a `type X = ...` statement can be
        registered too, as can bare `None`. `Any` itself isn't registered
        through this method. {py:meth}`__init__` populates that one
        directly. The handler receives the decoded JSON value directly (a
        dict, list, str, etc.), not raw bytes.

        With `takes_type=True`, the handler is called as `handler(value,
        cls)` instead of `handler(value)`. This allows the handler to work
        for a family of types, such as `Enum` subclasses, letting the handler
        create the correct data.

        :param cls: The type to register a handler for.
        :param takes_type: If `True`, call the handler as `handler(value,
            cls)` instead of `handler(value)`.
        :returns: A decorator that registers its wrapped function as `cls`'s
            handler and returns the function unchanged.
        """

        def decorator(fn: F) -> F:
            self._handlers[cls] = (fn, takes_type)
            return fn

        if func is None:
            return decorator
        return decorator(func)

    def deregister(self, type_: Any, /):
        try:
            del self._handlers[type_]
        except KeyError as exc:
            raise ValueError(f"Cannot deregister {type_}: no handler registered.") from exc


deserialize = Deserialize()


@deserialize.register(str)
def decode_str(value: Any) -> str:
    """
    Validate a value already decoded as `str`.

    Pre-registered on {py:obj}`deserialize`.

    :param value: The decoded value to confirm.
    :returns: `value` unchanged.
    :raises TypeError: If `value` isn't a str.
    """
    if not isinstance(value, str):
        raise TypeError(f"Cannot deserialize {value!r} into str: not a str")
    return value


def _to_number(value: Any) -> int | float:
    """
    Coerce a decoded value to `int` or `float`.

    `bool` is rejected. A string is tried as `int` first, then `float`.

    :param value: The decoded value to coerce.
    :returns: `value` as an int or a float.
    :raises ValueError: If `value` is a bool, or isn't a number or a
        numeric string.
    """
    if isinstance(value, bool):
        raise ValueError("bool is not a number")
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return float(value)
    raise ValueError("not a number or a numeric string")


@deserialize.register(int)
def decode_int(value: Any) -> int:
    """
    Validate a value already decoded as int, or parse something that cleanly represents one.

    Pre-registered on {py:obj}`deserialize`. A whole-numbered `float`, or a
    string representing one, like `42.0` or `"42.0"`, is accepted. A
    fractional float, or a string representing one, is rejected.

    :param value: The decoded value to confirm or parse.
    :returns: `value` as an int.
    :raises TypeError: If `value` isn't cleanly representable as an int
        without losing data.
    """
    try:
        number = _to_number(value)
    except ValueError:
        raise TypeError(
            f"Cannot deserialize {value!r} into int: not an int or an int-shaped string"
        ) from None
    if isinstance(number, int):
        return number
    if not number.is_integer():
        raise TypeError(f"Cannot deserialize {value!r} into int: not a whole number")
    return int(number)


@deserialize.register(float)
def decode_float(value: Any) -> float:
    """
    Validate a value already decoded as a number, or parse a string that represents one.

    Pre-registered on {py:obj}`deserialize`. An `int` value, or a numeric
    string like `"31.5"` or `"42"`, is accepted.

    :param value: The decoded value to confirm or parse.
    :returns: `value` as a `float`.
    :raises TypeError: If `value` isn't a number or a numeric string.
    """
    try:
        number = _to_number(value)
    except ValueError:
        raise TypeError(
            f"Cannot deserialize {value!r} into float: not a number or a numeric string"
        ) from None
    return float(number)


@deserialize.register(bool)
def decode_bool(value: Any) -> bool:
    """
    Validate a value already decoded as `bool`, or parse "true"/"false" case-insensitively.

    Pre-registered on {py:obj}`deserialize`. The literal string spellings
    of `true`/`false`, in any casing, are accepted.

    :param value: The decoded value to confirm or parse.
    :returns: `value` as a bool.
    :raises TypeError: If `value` isn't a bool or one of those strings.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    raise TypeError(f'Cannot deserialize {value!r} into bool: not a bool or "true"/"false"')


@deserialize.register(NoneType)
@deserialize.register(None)
def decode_none(value: Any) -> None:
    """
    Validate a value already decoded as `None`.

    Pre-registered on {py:obj}`deserialize` under both `NoneType` and bare
    `None`.

    :param value: The decoded value to confirm.
    :returns: `None`.
    :raises TypeError: If `value` isn't `None`.
    """
    if value is not None:
        raise TypeError(f"Cannot deserialize {value!r} into NoneType: not None")
    return value


@deserialize.register(dict)
def decode_dict(value: Any) -> dict[Any, Any]:
    """
    Validate a value already decoded as a dict.

    Pre-registered on {py:obj}`deserialize` as shorthand for
    `dict[Any, Any]`. Behaves identically, just without spelling the lack
    of constraint out.

    :param value: The already-decoded dict.
    :returns: `value` unchanged.
    :raises TypeError: If `value` isn't a dict.
    """
    if not isinstance(value, dict):
        raise TypeError(f"Cannot deserialize {value!r} into dict: not a dict")
    return cast(dict[Any, Any], value)


@deserialize.register(list)
def decode_list(value: Any) -> list[Any]:
    """
    Validate a value already decoded as a list, or convert one decoded as a tuple/set/frozenset.

    Pre-registered on {py:obj}`deserialize` as shorthand for `list[Any]`.
    A `list`, `tuple`, `set`, or `frozenset` value is accepted.

    :param value: The decoded value to confirm or convert.
    :returns: `value` as a list.
    :raises TypeError: If `value` isn't a list, tuple, set, or frozenset.
    """
    if isinstance(value, list):
        return cast(list[Any], value)
    if isinstance(value, (tuple, set, frozenset)):
        return list(cast(list[Any], value))
    raise TypeError(f"Cannot deserialize {value!r} into list: not a list, tuple, set, or frozenset")


@deserialize.register(tuple)
def decode_tuple(value: Any) -> tuple[Any, ...]:
    """
    Validate a value already decoded as a tuple, or convert one decoded as a list/set/frozenset.

    Pre-registered on {py:obj}`deserialize` as shorthand for
    `tuple[Any, ...]`. A `tuple`, `list`, `set`, or `frozenset` value is
    accepted.

    :param value: The decoded value to confirm or convert.
    :returns: `value` as a tuple.
    :raises TypeError: If `value` isn't a tuple, list, set, or frozenset.
    """
    if isinstance(value, tuple):
        return cast(tuple[Any, ...], value)
    if isinstance(value, (list, set, frozenset)):
        return tuple(cast(tuple[Any, ...], value))
    raise TypeError(
        f"Cannot deserialize {value!r} into tuple: not a tuple, list, set, or frozenset"
    )


@deserialize.register(set)
def decode_set(value: Any) -> set[Any]:
    """
    Validate a value already decoded as a set, or convert one decoded as a list/tuple/frozenset.

    Pre-registered on {py:obj}`deserialize` as shorthand for `set[Any]`.
    A `set`, `list`, `tuple`, or `frozenset` value is accepted. Converting
    from a `list` or `tuple` drops duplicate elements.

    :param value: The decoded value to confirm or convert.
    :returns: `value` as a set.
    :raises TypeError: If `value` isn't a set, list, tuple, or frozenset.
    """
    if isinstance(value, set):
        return cast(set[Any], value)
    if isinstance(value, (list, tuple, frozenset)):
        return set(cast(set[Any], value))
    raise TypeError(f"Cannot deserialize {value!r} into set: not a set, list, tuple, or frozenset")


@deserialize.register(frozenset)
def decode_frozenset(value: Any) -> frozenset[Any]:
    """
    Validate a value already decoded as a frozenset, or convert one decoded as a list/tuple/set.

    Pre-registered on {py:obj}`deserialize` as shorthand for
    `frozenset[Any]`. A `frozenset`, `list`, `tuple`, or `set` value is
    accepted. Converting from a `list` or `tuple` drops duplicate elements.

    :param value: The decoded value to confirm or convert.
    :returns: `value` as a frozenset.
    :raises TypeError: If `value` isn't a frozenset, list, tuple, or set.
    """
    if isinstance(value, frozenset):
        return cast(frozenset[Any], value)
    if isinstance(value, (list, tuple, set)):
        return frozenset(cast(frozenset[Any], value))
    raise TypeError(
        f"Cannot deserialize {value!r} into frozenset: not a frozenset, list, tuple, or set"
    )


@deserialize.register(UUID)
def decode_uuid(value: Any) -> UUID:
    """
    Validate a value already decoded as a `UUID`, or parse its string form.

    Pre-registered on {py:obj}`deserialize` as a common default, the decode
    side of {py:func}`encode_uuid`. A source that already produces native
    `UUID` instances is accepted as-is. Otherwise `value` must be a string,
    matching what `encode_uuid` produces. Overwrite it with
    `@deserialize.register(UUID)` if this doesn't match your requirements.

    :param value: The decoded value to confirm or parse.
    :returns: The UUID.
    :raises TypeError: If `value` is neither a UUID nor a string.
    """
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        raise TypeError(f"Cannot deserialize {value!r} into UUID: not a UUID or a string")
    return UUID(value)


@deserialize.register(datetime)
def decode_datetime(value: Any) -> datetime:
    """
    Validate a value already decoded as a `datetime`, or parse its ISO 8601 string form.

    Pre-registered on {py:obj}`deserialize` as a common default, the decode
    side of {py:func}`encode_datetime`. A source that already produces
    native `datetime` instances is accepted as-is. Otherwise `value` must
    be a string, matching what `encode_datetime` produces. Overwrite it
    with `@deserialize.register(datetime)` if this doesn't match your
    requirements.

    :param value: The decoded value to confirm or parse.
    :returns: The datetime.
    :raises TypeError: If `value` is neither a datetime nor a string.
    """
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise TypeError(f"Cannot deserialize {value!r} into datetime: not a datetime or a string")
    return datetime.fromisoformat(value)


@deserialize.register(Decimal)
def decode_decimal(value: Any) -> Decimal:
    """
    Validate a value already decoded as a `Decimal`, or parse its string form.

    Pre-registered on {py:obj}`deserialize` as a common default, the decode
    side of {py:func}`encode_decimal`. A source that already produces
    native `Decimal` instances is accepted as-is. Otherwise `value` must
    be a string, matching what `encode_decimal` produces. A raw `float` is
    not accepted. Overwrite it with `@deserialize.register(Decimal)` if
    this doesn't match your requirements.

    :param value: The decoded value to confirm or parse.
    :returns: The Decimal.
    :raises TypeError: If `value` is neither a Decimal nor a string.
    """
    if isinstance(value, Decimal):
        return value
    if not isinstance(value, str):
        raise TypeError(f"Cannot deserialize {value!r} into Decimal: not a Decimal or a string")
    return Decimal(value)


def decode_enum[T: Enum](value: Any, type_: type[T]) -> T:
    """
    Validate an enum by value using the provided type to look up by member value.

    Not pre-registered on {py:obj}`deserialize`. This is provided as a
    pre-configured option for deserializing enum by value. Register using
    `deserialize.register(Enum, takes_type = True)` to deserialize
    any Enum by value, or use a specific class or base class to only
    deserialize matching enums.

    :param value: The decoded value to confirm or look up.
    :param type_: The concrete `Enum` subclass being constructed.
    :returns: The matching member of `type_`.
    :raises TypeError: If `value` isn't already a `type_` member and isn't
        one of `type_`'s member values.
    """
    if isinstance(value, type_):
        return value
    try:
        return type_(value)
    except ValueError:
        raise TypeError(
            f"Cannot deserialize {value!r} into {type_.__name__}: not a valid member value"
        ) from None


def decode_enum_by_name(value: Any, type_: type[Enum]) -> Enum:
    """
    Validate an enum by name using the provided type as a constructor.

    Not pre-registered on {py:obj}`deserialize`. This is provided as a
    pre-configured option for deserializing enum by name. Register using
    `deserialize.register(Enum, takes_type = True)` to deserialize
    any Enum by name, or use a specific class or base class to only
    deserialize matching enums.

    :param value: The decoded value to confirm or look up.
    :param type_: The concrete `Enum` subclass being constructed.
    :returns: The matching member of `type_`.
    :raises TypeError: If `value` isn't already a `type_` member and isn't
        one of `type_`'s member names.
    """
    if isinstance(value, type_):
        return value
    try:
        return type_[value]
    except (KeyError, TypeError):
        raise TypeError(
            f"Cannot deserialize {value!r} into {type_.__name__}: not a valid member name"
        ) from None
