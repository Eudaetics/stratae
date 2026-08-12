# Serde

`stratae.serde` is a small set of primitives for turning arbitrary Python objects into bytes and back: `serialize`, `encode`, and `deserialize`. `Serializer`/`Deserializer` are protocols for swapping either side out entirely. This module is meant to cover a serialization boundary such as an HTTP response or a queue message without hand-writing a (de)serializer for every type that crosses it.

```{motivation}
stratae tries to assume as little as possible about how a project is built, and leaves choices about tooling with the developer instead of committing to one upfront. That shows up in `serde` as not requiring anything of the objects it's given. Every serialization boundary, an HTTP response or a queue message, needs some way to turn a domain object into bytes. Not every object crossing that boundary is something a project gets to redefine: a third-party return value, an ORM model, a plain dataclass from somewhere else in the codebase. Requiring those objects to conform to some base class or interface before they can be serialized isn't always possible, and isn't something `serde` tries to do either. `serialize`/`encode` ask nothing of the object passed in: no base class, no required method, nothing added to the type itself.
```

## Serializing objects

`serialize(obj)` turns an object into a byte representation. By default it uses `json.dumps` with `encode` as the field-level hook for whenever it hits something it doesn't already know how to serialize.

````{example} Serializing values json already understands
```{code-block} python
from stratae.serde import serialize

order = {"sku": "A1", "quantity": 3, "backordered": False}
print(serialize(order))
```
```{output}
b'{"sku": "A1", "quantity": 3, "backordered": false}'
```
````

Anything `json.dumps` already understands on its own, dicts, lists, strings, numbers, booleans, `None`, passes straight through without ever reaching `encode`. `encode` only gets involved for the individual values inside that structure it can't already handle natively.

A handful of common types are pre-registered on `encode` for exactly that reason. `UUID`, `datetime`, and `Decimal` all serialize with no class definition and no registration, whether one shows up as the whole payload or buried inside a plain dict.

````{example} Serializing pre-registered types inside a plain dict
```{code-block} python
from datetime import datetime
from decimal import Decimal
from uuid import UUID
from stratae.serde import serialize

order = {
    "id": UUID("47e511ef-f16c-4699-98db-a0d44abcab90"),
    "total": Decimal("19.99"),
    "placed_at": datetime(2024, 1, 15, 9, 30),
}
print(serialize(order))
```
```{output}
b'{"id": "47e511ef-f16c-4699-98db-a0d44abcab90", "total": "19.99", "placed_at": "2024-01-15T09:30:00"}'
```
````

`UUID`, `datetime`, and `Decimal` are the only types `encode` has a default for. There's no fallback to `to_dict()` or `model_dump()`, so a dataclass and a Pydantic model each need an encoder registered for them the same as any other type.

````{example} Registering encoders for a dataclass and a Pydantic model
```{code-block} python
from dataclasses import asdict, dataclass
from uuid import UUID
from pydantic import BaseModel
from stratae.serde import encode, serialize

@dataclass
class Invoice:
    id: UUID
    customer: str

    def __post_init__(self) -> None:
        if isinstance(self.id, str):
            self.id = UUID(self.id)

@encode.register
def _(obj: Invoice) -> dict:
    return asdict(obj)

class Shipment(BaseModel):
    id: int
    carrier: str

@encode.register
def _(obj: Shipment) -> dict:
    return obj.model_dump()

data = serialize({
    "invoice": Invoice(id=UUID("47e511ef-f16c-4699-98db-a0d44abcab90"), customer="Acme Corp"),
    "shipment": Shipment(id=2, carrier="UPS"),
})
print(data)
```
```{output}
b'{"invoice": {"id": "47e511ef-f16c-4699-98db-a0d44abcab90", "customer": "Acme Corp"}, "shipment": {"id": 2, "carrier": "UPS"}}'
```
````

A registered encoder isn't limited to returning a dict either. It can return anything `json.dumps` can already handle on its own.

````{example} Registering an encoder that returns a plain value
```{code-block} python
from stratae.serde import encode, serialize

class Priority:
    def __init__(self, level: int) -> None:
        self.level = level

@encode.register
def _(obj: Priority) -> int:
    return obj.level

data = serialize({"priority": Priority(2)})
print(data)
```
```{output}
b'{"priority": 2}'
```
````

## Deserializing objects

`deserialize` is `serialize`'s counterpart. Passed a `type`, it doesn't guess at how to build one. `list[T]`, `set[T]`, `tuple[...]`, `dict[K, V]`, and unions are the only targets it knows how to recurse into on its own. Anything else, a dataclass included, needs a handler registered for it before `deserialize` can construct one.

`type` is optional. Called without one, `deserialize` just decodes the JSON, the same as `json.loads`.

````{example} Deserializing without a type
```{code-block} python
from stratae.serde import deserialize

data = b'{"name": "sprocket", "count": 3}'
print(deserialize(data))
```
```{output}
{'name': 'sprocket', 'count': 3}
```
````

With a `type`, `deserialize` has its own registration, separate from `encode`/`serialize`'s. It can't be `functools.singledispatch`, since dispatch here has to key off the value of the `type` argument, not the runtime type of a positional argument. Registering a handler is how `Invoice` gets built at all, the same principle as `encode.register`, just for the decode side, and required rather than optional since `deserialize` has no default construction to fall back on.

````{example} Registering a handler for field coercion
```{code-block} python
import json
from dataclasses import dataclass
from uuid import UUID, uuid4
from stratae.serde import deserialize

@dataclass
class Invoice:
    id: UUID

@deserialize.register(Invoice)
def _(value: dict) -> Invoice:
    return Invoice(id=UUID(value["id"]))

original_id = uuid4()
data = json.dumps({"id": str(original_id)}).encode()
restored = deserialize(data, type=Invoice)
print(restored.id == original_id, isinstance(restored.id, UUID))
```
```{output}
True True
```
````

Like `encode.register`, this registration falls back through a class's parents. A handler registered for a base class also covers its subclasses, unless a more specific handler is registered for the subclass itself.

A `type` can also be parameterized. `list[T]`, `set[T]`, `tuple[...]` (fixed-length or variadic), and `dict[K, V]` targets all recurse the same way, reconstructing each element or value in turn, including through a registered handler if one applies.

````{example} Deserializing a parameterized list
```{code-block} python
from dataclasses import dataclass
from stratae.serde import deserialize

@dataclass
class Item:
    sku: str

@deserialize.register(Item)
def _(value: dict) -> Item:
    return Item(**value)

data = b'[{"sku": "A1"}, {"sku": "B2"}]'
print(deserialize(data, type=list[Item]))
```
```{output}
[Item(sku='A1'), Item(sku='B2')]
```
````

A union works the same way, one non-`None` member gets recursively constructed, and `None` passes through unchanged.

````{example} Deserializing an optional field
```{code-block} python
from dataclasses import dataclass
from stratae.serde import deserialize

@dataclass
class Item:
    sku: str

@deserialize.register(Item)
def _(value: dict) -> Item:
    return Item(**value)

print(deserialize(b'{"sku": "A1"}', type=Item | None))
print(deserialize(b'null', type=Item | None))
```
```{output}
Item(sku='A1')
None
```
````

A union with more than one non-`None` member, `int | str`, isn't allowed. `deserialize` raises `TypeError` there, since there's no way to know which member to construct.

```{attention}
That recursion only happens when the nesting is spelled out in `deserialize`'s own `type` argument. A dataclass field typed `items: list[Item]` doesn't get reconstructed when the outer type is deserialized. It comes back as a list of plain dicts, since a handler that does `Outer(**value)` just spreads the payload without inspecting field annotations. Reconstructing a field like that still needs `__post_init__` on the class, or a handler registered for the outer type itself.
```

## Extending encode and serialize

Both `encode` and `serialize` are `functools.singledispatch` functions. Register a type-specific implementation the same way you would for any singledispatch function, and `serialize` picks it up automatically since it calls `encode` as its fallback hook.

````{example} Registering a custom encoder
```{code-block} python
from dataclasses import dataclass
from stratae.serde import encode, serialize

@dataclass
class Money:
    amount: float

@encode.register
def _(obj: Money) -> str:
    return f"{obj.amount:.2f}"

data = serialize({"price": Money(19.5)})
print(data)
```
```{output}
b'{"price": "19.50"}'
```
````

`serialize` itself can be registered too, not just `encode`. Registering a type directly on `serialize` replaces the whole `json.dumps` call for that type, which is how a faster or format-specific serializer gets swapped in without touching anything that already calls `serialize`.

## Serializer and Deserializer as protocols

`serialize` and `deserialize` are each backed by a small structural protocol describing their call shape. A callable matching that shape can stand in for either, entirely bypassing `serialize`'s and `deserialize`'s own registration mechanisms. The only adapter code necessary for tools that don't support that call shape is a closure or lambda to bridge the different call shapes.

`Serializer` matches `serialize`'s own call shape: `__call__(obj: object, /) -> bytes`. `msgspec.json.encode` already fits it, since it takes an object and returns bytes the same way.

````{example} Swapping in msgspec as a Serializer
```{code-block} python
import msgspec
from stratae.serde import Serializer

def publish(obj: object, *, serializer: Serializer) -> None:
    print(serializer(obj))

publish({"id": 1, "customer": "Acme Corp"}, serializer=msgspec.json.encode)
```
```{output}
b'{"id":1,"customer":"Acme Corp"}'
```
````

`Deserializer` matches `deserialize`'s call shape: `__call__(data: bytes, *, type: type[S]) -> S`. `msgspec.json.decode` fits it exactly too.

````{example} Swapping in msgspec as a Deserializer
```{code-block} python
import msgspec
from stratae.serde import serialize

class Invoice(msgspec.Struct):
    id: int
    customer: str

data = serialize({"id": 1, "customer": "Acme Corp"})
restored = msgspec.json.decode(data, type=Invoice)
print(restored)
```
```{output}
Invoice(id=1, customer='Acme Corp')
```
````

Nothing about `serialize` or `deserialize` changes to make either swap work. Each protocol only describes one call shape. Any callable matching it can stand in.

## Integrations

### Stratae modules

`stratae.integrations.msgspec` uses the same registration mechanism described above to attach a `msgspec.Struct`-specific fast path onto `serialize`, using `msgspec.json.encode` instead of `json.dumps` for that type. The registration only takes effect once that module has actually been imported somewhere in the process, since `@serialize.register` runs at import time.

### External tools

`encode` has no default for a Pydantic model any more than it does for a dataclass. Registering `model_dump()` as its encoder is enough to have it serialize through `serialize`.

````{example} Registering a Pydantic model's encoder
```{code-block} python
from pydantic import BaseModel
from stratae.serde import encode, serialize

class Invoice(BaseModel):
    id: int
    customer: str

@encode.register
def _(obj: Invoice) -> dict:
    return obj.model_dump()

invoice = Invoice(id=1, customer="Acme Corp")
data = serialize(invoice)
print(data)
```
```{output}
b'{"id": 1, "customer": "Acme Corp"}'
```
````

Full signatures: {doc}`stratae.serde API reference <../apidocs/stratae.serde/stratae.serde>`.
