"""A read-only mapping that is a value: it compares and hashes by its items."""

from collections.abc import Iterator, Mapping


class FrozenMapping[K, V](Mapping[K, V]):
    """
    A read-only copy of a mapping, hashed by its items.

    It compares equal to any mapping with the same items, and hashes if its values do.
    """

    __slots__ = ("_items",)

    def __init__(self, items: Mapping[K, V]) -> None:
        """Copy `items`, so later changes to them don't reach this mapping."""
        self._items: dict[K, V] = dict(items)

    def __getitem__(self, key: K) -> V:
        return self._items[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return hash(frozenset(self._items.items()))

    def __repr__(self) -> str:
        return repr(self._items)
