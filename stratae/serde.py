"""
Serialization and deserialization of payloads to and from bytes.

{py:func}`serialize` turns a payload into JSON bytes, using {py:func}`encode` for fields
`json` can't handle. {py:obj}`deserialize` decodes bytes into a target type. Extend them with
`@encode.register`, `@serialize.register`, and `@deserialize.register`. {py:class}`Serializer`
and {py:class}`Deserializer` are the protocols adapters accept, so either side can be swapped
out. See {py:mod}`stratae.integrations.msgspec` for a faster path for `msgspec.Struct`.

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
"""

import json
from datetime import datetime
from decimal import Decimal
from enum import Enum, EnumType
from functools import singledispatch
from types import NoneType, UnionType
from typing import (
    Any,
    Callable,
    Literal,
    Protocol,
    Sequence,
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

    A `functools.singledispatch` function used as {py:func}`serialize`'s `default` hook.
    `UUID`, `datetime`, and `Decimal` are pre-registered.

    :param obj: The value to encode.
    :returns: A JSON-serializable representation of `obj`.
    :raises TypeError: If `obj` has no registered encoder.
    """
    raise TypeError(f"Object of type {type(obj)} is not encodable")


@encode.register
def encode_uuid(obj: UUID) -> str:
    """
    Encode a `UUID` as its string form.

    Pre-registered on {py:func}`encode`; override with `@encode.register`.

    :param obj: The UUID to encode.
    :returns: The string form of `obj`.
    """
    return str(obj)


@encode.register
def encode_datetime(obj: datetime) -> str:
    """
    Encode a `datetime` as an ISO 8601 string.

    Pre-registered on {py:func}`encode`; override with `@encode.register`.

    :param obj: The datetime to encode.
    :returns: The ISO 8601 string form of `obj`.
    """
    return obj.isoformat()


@encode.register
def encode_decimal(obj: Decimal) -> str:
    """
    Encode a `Decimal` as its string form.

    Pre-registered on {py:func}`encode`; override with `@encode.register`.

    :param obj: The decimal to encode.
    :returns: The string form of `obj`.
    """
    return str(obj)


class Serializer[T](Protocol):
    """
    Structural protocol for a payload serializer, shaped like {py:func}`serialize`.

    Lets callers swap in another serializer, such as `msgspec.json.encode`.
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
    Serialize a payload to JSON bytes.

    Uses `json.dumps` with {py:func}`encode` as the field hook. Register faster paths for
    specific payload types with `@serialize.register`.

    :param obj: The payload to serialize.
    :returns: The serialized payload as bytes.
    """
    return json.dumps(obj, default=encode).encode()


class Deserializer(Protocol):
    """
    Structural protocol for a type-directed deserializer.

    Shaped like `msgspec.json.decode(data, type=T)`. {py:obj}`deserialize` is the default
    implementation.
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


def _dispatch_bases(type_: Any) -> Sequence[Any]:
    """
    Return the bases to check for a handler once `type_` itself has none.

    That's the MRO minus `type_`, except an `Enum` subclass checks only its `Enum` bases.
    Otherwise a mixed-in type like `int` would decode an `IntEnum` as a plain int.
    """
    bases: tuple[Any, ...] = getattr(type_, "__mro__", ())[1:]
    if isinstance(type_, EnumType):
        return [base for base in bases if issubclass(base, Enum)]
    return bases


_ARRAY_TYPES = (list, tuple, set, frozenset)


def _require_array(value: Any, target: str) -> None:
    """
    Raise unless `value` is a list, tuple, set, or frozenset.

    Guards against a str or dict being iterated into characters or keys.
    """
    if not isinstance(value, _ARRAY_TYPES):
        raise TypeError(
            f"Cannot deserialize {value!r} into {target}: not a list, tuple, set, or frozenset"
        )


class Deserialize:
    """
    Default {py:class}`Deserializer` with per-type handler registration.

    Like `functools.singledispatch`, but dispatching on the target type rather than the
    argument's type.
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

        With no `type`, behaves like `json.loads`. Otherwise a handler registered with
        {py:meth}`register` for `type` or a base runs first, falling back to element-wise
        construction of typed containers.

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
        Construct `type` from an already-decoded JSON value.

        Registered handlers apply wherever `type` appears, including container elements.

        :param type: The type to reconstruct.
        :param value: The decoded JSON value.
        :returns: The reconstructed `type` instance.
        :raises TypeError: If `type` has no handler and isn't a supported generic.
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
        Look up a registered handler for `type`, walking its MRO like `singledispatch`.

        Types without an `__mro__`, such as unions and generic aliases, only match exactly.
        """
        if found := self._handlers.get(type):
            return found
        for base in _dispatch_bases(type):
            if found := self._handlers.get(base):
                return found
        return None

    def _construct_list(self, args: tuple[Any, ...], value: Any) -> list[Any]:
        """Recurse into each element of a `list[T]` target."""
        (item_type,) = args
        _require_array(value, "list")
        return [self.construct(item_type, item) for item in value]

    def _construct_set(self, args: tuple[Any, ...], value: Any) -> set[Any]:
        """Recurse into each element of a `set[T]` target."""
        (item_type,) = args
        _require_array(value, "set")
        return {self.construct(item_type, item) for item in value}

    def _construct_frozenset(self, args: tuple[Any, ...], value: Any) -> frozenset[Any]:
        """Recurse into each element of a `frozenset[T]` target."""
        (item_type,) = args
        _require_array(value, "frozenset")
        return frozenset(self.construct(item_type, item) for item in value)

    def _construct_tuple(self, args: tuple[Any, ...], value: Any) -> tuple[Any, ...]:
        """Recurse into each element of a `tuple[...]` target."""
        _require_array(value, "tuple")
        if len(args) == 2 and args[1] is Ellipsis:
            item_type = args[0]
            return tuple(self.construct(item_type, item) for item in value)
        return self._construct_fixed_tuple(args, value)

    def _construct_fixed_tuple(self, args: tuple[Any, ...], value: Any) -> tuple[Any, ...]:
        """Recurse into a fixed-length `tuple[...]` target, checking its length."""
        if len(value) != len(args):
            raise TypeError(
                f"Cannot deserialize {value!r} into tuple: expected {len(args)} elements, "
                f"got {len(value)}"
            )
        return tuple(
            self.construct(item_type, item) for item_type, item in zip(args, value, strict=True)
        )

    def _construct_dict(self, args: tuple[Any, ...], value: Any) -> dict[Any, Any]:
        """Recurse into each key and value of a `dict[K, V]` target."""
        key_type, value_type = args
        if not isinstance(value, dict):
            raise TypeError(f"Cannot deserialize {value!r} into dict: not a dict")
        return {
            self.construct(key_type, key): self.construct(value_type, item)
            for key, item in cast(dict[Any, Any], value).items()
        }

    def _construct_union(self, args: tuple[Any, ...], value: Any) -> Any:
        """Recurse against a union's one non-None member."""
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

        `cls` may also be a union, a `type X = ...` alias, or `None`. The handler receives
        the decoded JSON value, not bytes.

        :param cls: The type to register a handler for.
        :param takes_type: Call the handler as `handler(value, cls)`, so one handler can serve a
            family of types such as `Enum` subclasses.
        :returns: A decorator that registers the function and returns it unchanged.
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
    Validate a decoded `str`.

    :param value: The decoded value.
    :returns: `value` unchanged.
    :raises TypeError: If `value` isn't a str.
    """
    if not isinstance(value, str):
        raise TypeError(f"Cannot deserialize {value!r} into str: not a str")
    return value


def _to_number(value: Any) -> int | float:
    """
    Coerce a decoded value to `int` or `float`, rejecting `bool`.

    :raises ValueError: If `value` isn't a number or numeric string.
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
    Validate a decoded int, or parse a value that cleanly represents one.

    Whole-numbered floats and strings like `42.0` or `"42"` are accepted; fractional ones
    are rejected.

    :param value: The decoded value.
    :returns: `value` as an int.
    :raises TypeError: If `value` can't become an int without losing data.
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
    Validate a decoded number, or parse a numeric string.

    :param value: The decoded value.
    :returns: `value` as a float.
    :raises TypeError: If `value` isn't a number or numeric string.
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
    Validate a decoded bool, or parse `"true"`/`"false"` in any casing.

    :param value: The decoded value.
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
    Validate a decoded `None`. Registered under both `NoneType` and `None`.

    :param value: The decoded value.
    :returns: `None`.
    :raises TypeError: If `value` isn't `None`.
    """
    if value is not None:
        raise TypeError(f"Cannot deserialize {value!r} into NoneType: not None")
    return value


@deserialize.register(dict)
def decode_dict(value: Any) -> dict[Any, Any]:
    """
    Validate a decoded dict.

    Pre-registered on {py:obj}`deserialize` as shorthand for `dict[Any, Any]`.

    :param value: The decoded value.
    :returns: `value` unchanged.
    :raises TypeError: If `value` isn't a dict.
    """
    if not isinstance(value, dict):
        raise TypeError(f"Cannot deserialize {value!r} into dict: not a dict")
    return cast(dict[Any, Any], value)


@deserialize.register(list)
def decode_list(value: Any) -> list[Any]:
    """
    Validate a decoded list, or convert a decoded tuple, set, or frozenset.

    Pre-registered on {py:obj}`deserialize` as shorthand for `list[Any]`.

    :param value: The decoded value.
    :returns: `value` as a list.
    :raises TypeError: If `value` isn't one of those types.
    """
    if isinstance(value, list):
        return cast(list[Any], value)
    if isinstance(value, (tuple, set, frozenset)):
        return list(cast(list[Any], value))
    raise TypeError(f"Cannot deserialize {value!r} into list: not a list, tuple, set, or frozenset")


@deserialize.register(tuple)
def decode_tuple(value: Any) -> tuple[Any, ...]:
    """
    Validate a decoded tuple, or convert a decoded list, set, or frozenset.

    Pre-registered on {py:obj}`deserialize` as shorthand for `tuple[Any, ...]`.

    :param value: The decoded value.
    :returns: `value` as a tuple.
    :raises TypeError: If `value` isn't one of those types.
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
    Validate a decoded set, or convert a decoded list, tuple, or frozenset.

    Pre-registered on {py:obj}`deserialize` as shorthand for `set[Any]`. Duplicates are dropped.

    :param value: The decoded value.
    :returns: `value` as a set.
    :raises TypeError: If `value` isn't one of those types.
    """
    if isinstance(value, set):
        return cast(set[Any], value)
    if isinstance(value, (list, tuple, frozenset)):
        return set(cast(set[Any], value))
    raise TypeError(f"Cannot deserialize {value!r} into set: not a set, list, tuple, or frozenset")


@deserialize.register(frozenset)
def decode_frozenset(value: Any) -> frozenset[Any]:
    """
    Validate a decoded frozenset, or convert a decoded list, tuple, or set.

    Pre-registered on {py:obj}`deserialize` as shorthand for `frozenset[Any]`. Duplicates are
    dropped.

    :param value: The decoded value.
    :returns: `value` as a frozenset.
    :raises TypeError: If `value` isn't one of those types.
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
    Validate a decoded `UUID`, or parse its string form.

    Pre-registered on {py:obj}`deserialize`, mirroring {py:func}`encode_uuid`. Override with
    `@deserialize.register(UUID)`.

    :param value: The decoded value.
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
    Validate a decoded `datetime`, or parse its ISO 8601 string.

    Pre-registered on {py:obj}`deserialize`, mirroring {py:func}`encode_datetime`. Override with
    `@deserialize.register(datetime)`.

    :param value: The decoded value.
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
    Validate a decoded `Decimal`, or parse its string form; floats are rejected.

    Pre-registered on {py:obj}`deserialize`, mirroring {py:func}`encode_decimal`. Override with
    `@deserialize.register(Decimal)`.

    :param value: The decoded value.
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
    Look up an enum member by value.

    Not pre-registered. Opt in with `deserialize.register(Enum, takes_type=True)`, or a
    specific enum class.

    :param value: The decoded value.
    :param type_: The `Enum` subclass being constructed.
    :returns: The matching member of `type_`.
    :raises TypeError: If `value` isn't a member or member value of `type_`.
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
    Look up an enum member by name.

    Not pre-registered. Opt in with `deserialize.register(Enum, takes_type=True)`, or a
    specific enum class.

    :param value: The decoded value.
    :param type_: The `Enum` subclass being constructed.
    :returns: The matching member of `type_`.
    :raises TypeError: If `value` isn't a member or member name of `type_`.
    """
    if isinstance(value, type_):
        return value
    try:
        return type_[value]
    except (KeyError, TypeError):
        raise TypeError(
            f"Cannot deserialize {value!r} into {type_.__name__}: not a valid member name"
        ) from None
