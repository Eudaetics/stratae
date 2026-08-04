"""
Serialization and deserialization tools for encoding/decoding data.

{py:func}`serialize` turns a payload into bytes, using {py:func}`encode` to
convert individual fields that aren't natively JSON-serializable (UUIDs,
datetimes, Decimals, or objects exposing `to_dict`/`model_dump`).
{py:class}`Serializer` is the structural protocol for a serializer shaped
like `serialize`, for adapters that let callers swap it out entirely rather
than extend it via registration. {py:class}`Deserializer` is the structural
counterpart on the decode side: a type-directed deserializer shaped like
`msgspec.json.decode(data, type=T)`, with {py:obj}`deserialize` as the
default, dependency-free implementation. Register additional types with
`@encode.register` or `@serialize.register` as needed; see
{py:mod}`stratae.integrations.msgspec` for a faster `serialize` registered
for `msgspec.Struct` payloads.

````{example} Round-tripping a dataclass through the default serialize/deserialize pair
```{code-block} python
from dataclasses import asdict, dataclass
from uuid import UUID
from stratae.serde import serialize, deserialize

@dataclass
class Widget:
    id: UUID
    name: str

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

    def to_dict(self):
        return asdict(self)

widget = Widget(
    id=UUID("47e511ef-f16c-4699-98db-a0d44abcab90"), name="sprocket"
)
data = serialize(widget)
print(data)

restored = deserialize(data, type=Widget)
print(restored == widget)
```
```{output}
b'{"id": "47e511ef-f16c-4699-98db-a0d44abcab90", "name": "sprocket"}'
True
```
````

See {py:func}`encode`, {py:func}`serialize`, {py:class}`Deserializer`, and
{py:obj}`deserialize` for the rest of the module's API.

"""

import json
from datetime import datetime
from decimal import Decimal
from functools import singledispatch
from types import UnionType
from typing import Any, Callable, Protocol, Union, get_args, get_origin, overload
from uuid import UUID
from weakref import WeakKeyDictionary

__all__ = ["encode", "serialize", "Serializer", "Deserializer", "deserialize"]


@singledispatch
def encode(obj: object) -> Any:
    """
    Encode a field value for serialization.

    Falls back to `to_dict()` or `model_dump()` if present on `obj`, covering
    plain dataclass-like and Pydantic-style objects without a registered
    encoder. Uses `functools.singledispatch`; register additional types via
    `@encode.register`. Pre-registered for `UUID` ({py:func}`encode_uuid`),
    `datetime` ({py:func}`encode_datetime`), and `Decimal`
    ({py:func}`encode_decimal`). Used as the `default` hook by
    {py:func}`serialize`.

    :param obj: The value to encode.
    :returns: A JSON-serializable representation of `obj`.
    :raises TypeError: If `obj` has no registered encoder and no `to_dict` or
        `model_dump` method.
    """
    if to_dict := getattr(obj, "to_dict", None):
        return to_dict()
    if model_dump := getattr(obj, "model_dump", None):
        return model_dump()
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

    Shaped after the call signature of `msgspec.json.decode(data, type=T)`,
    so adapters for other tools can be written as lambdas or thin wrappers
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


class _Deserialize:
    """
    Callable implementing {py:obj}`deserialize`, with per-type registration.

    A plain function can't gain a `functools.singledispatch`-style
    `.register()` here: dispatch needs to key off the `type` argument's
    *value*, not off the runtime type of a positional argument the way
    `singledispatch` dispatches. This wraps the default behavior in a
    callable object so `.register()` can be a real, statically-typed method
    instead of an attribute bolted onto a function.
    """

    def __init__(self) -> None:
        self._handlers: WeakKeyDictionary[type | UnionType, Callable[[Any], Any]] = (
            WeakKeyDictionary()
        )
        self._origin_constructors: dict[Any, Callable[[tuple[Any, ...], Any], Any]] = {
            list: self._construct_list,
            set: self._construct_set,
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
    def __call__(self, data: bytes, /, *, type: UnionType) -> Any: ...
    def __call__(self, data: bytes, /, *, type: Any = _UNSET) -> Any:
        """
        Deserialize JSON bytes, optionally into an instance of `type`.

        Decodes `data` exactly once. With no `type`, returns that decoded
        value as-is, the same as plain `json.loads`.

        With `type`, delegates to {py:meth}`_construct`. A handler
        registered via {py:meth}`register` runs first, if one exists for
        `type`. Otherwise `_construct` recurses element-wise for a
        `list[T]` target, or value-wise for a `dict[K, V]` target. A JSON
        object constructs `type` from keyword arguments. Any other value
        constructs `type` directly. The keyword-argument path is the
        default, dependency-free behavior. It's the inverse of
        {py:func}`serialize`'s default, and it covers plain
        keyword-constructible classes and dataclasses.

        Typed via `@overload`, not this implementation's own signature: a
        plain `type[S]` returns `S`. A `UnionType` returns `Any` - Python's
        type system can't express "give back whatever this union was"
        before 3.13's TypeForm, so this is typed as the honest limitation
        it is, not a precise type it can't back up.

        :param data: The raw JSON bytes to decode.
        :param type: The type to reconstruct, if any.
        :returns: The decoded value, or the reconstructed `type` instance.
        """
        value = json.loads(data)
        if type is _UNSET:
            return value
        return self._construct(type, value)

    def _construct(self, type: type | UnionType, value: Any) -> Any:
        """
        Construct `type` from an already-decoded JSON `value`.

        Not statically verifiable against a fixed return type. Which branch
        runs, and what it returns, depends on runtime introspection of
        `type` and the shape of `value`. `__call__`'s own `-> S` is the
        typed boundary. This is its dynamic, untyped interior.

        `Any` short-circuits to returning `value` unchanged. It means "no
        constraint," not "a type named Any to construct." Otherwise the
        registry is checked first. A handler registered for a type applies
        everywhere that type gets constructed. That includes a `list[T]`
        element or a `dict[K, V]` value, not just `__call__`'s own `type`
        argument directly.

        :param type: The type to reconstruct.
        :param value: The decoded JSON value to construct it from.
        :returns: The reconstructed `type` instance.
        """
        if type is Any:
            return value
        if handler := self._lookup_handler(type):
            return handler(value)
        return self._construct_by_origin(type, value)

    def _lookup_handler(self, type: Any) -> Callable[[Any], Any] | None:
        """Look up a registered handler for `type`, or None if it can't have one."""
        try:
            return self._handlers.get(type)
        except TypeError:
            return None

    def _construct_by_origin(self, type: type | UnionType, value: Any) -> Any:
        """Dispatch construction based on `type`'s generic origin, if it has one."""
        constructor = self._origin_constructors.get(get_origin(type))
        if constructor is None:
            return self._construct_default(type, value)
        return constructor(get_args(type), value)

    def _construct_list(self, args: tuple[Any, ...], value: Any) -> list[Any]:
        """Recurse into each element of a `list[T]` target."""
        (item_type,) = args
        return [self._construct(item_type, item) for item in value]

    def _construct_set(self, args: tuple[Any, ...], value: Any) -> set[Any]:
        """Recurse into each element of a `set[T]` target."""
        (item_type,) = args
        return {self._construct(item_type, item) for item in value}

    def _construct_tuple(self, args: tuple[Any, ...], value: Any) -> tuple[Any, ...]:
        """Recurse into each element of a `tuple[...]` target, fixed-length or variadic."""
        if len(args) == 2 and args[1] is Ellipsis:
            item_type = args[0]
            return tuple(self._construct(item_type, item) for item in value)
        return tuple(
            self._construct(item_type, item) for item_type, item in zip(args, value, strict=True)
        )

    def _construct_dict(self, args: tuple[Any, ...], value: Any) -> dict[Any, Any]:
        """Recurse into each key and value of a `dict[K, V]` target."""
        key_type, value_type = args
        return {
            self._construct(key_type, key): self._construct(value_type, item)
            for key, item in value.items()
        }

    def _construct_union(self, args: tuple[Any, ...], value: Any) -> Any:
        """Recurse against a union's one non-None member; None passes through unchanged."""
        if value is None:
            return None
        members = [arg for arg in args if arg is not type(None)]
        if len(members) == 1:
            return self._construct(members[0], value)
        raise TypeError(f"Cannot deserialize an ambiguous union with multiple members: {args!r}")

    def _construct_default(self, type: Any, value: Any) -> Any:
        """Construct `type` directly: keyword arguments for a dict, positional otherwise."""
        if isinstance(value, dict):
            return type(**value)
        return type(value)

    def register[S](self, cls: type[S], /) -> Callable[[Callable[[Any], S]], Callable[[Any], S]]:
        """
        Register a handler that constructs `cls` from an already-decoded value.

        Held with a weak reference to `cls`, so registering a short-lived
        type (e.g. one defined inside a test) doesn't keep it alive forever.
        The handler receives the decoded JSON value directly (a dict, list,
        str, etc.) rather than raw bytes, since {py:meth}`_construct` may
        reach it partway through decoding a larger structure, with no raw
        bytes for just that piece to hand over.

        :param cls: The type to register a handler for.
        :returns: A decorator that registers its wrapped function as `cls`'s
            handler and returns the function unchanged.
        """

        def decorator(func: Callable[[Any], S]) -> Callable[[Any], S]:
            self._handlers[cls] = func
            return func

        return decorator


deserialize = _Deserialize()
