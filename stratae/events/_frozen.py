"""Freeze the tuple fields of a frozen dataclass, such as hooks or middleware."""

_NOT_A_TUPLE = "{owner} {field}= takes a tuple of {kind}, e.g. {field}=({example},)"

_NOT_CALLABLE = "{owner} {field}= takes a tuple of {kind}, but holds {item!r}, which isn't callable"


def freeze_tuples(instance: object, fields: tuple[str, ...], kind: str, example: str) -> None:
    """
    Store each field as a tuple, refusing a lone callable or string given in place of one.

    :param instance: The frozen dataclass, in its `__post_init__`.
    :param fields: The fields to freeze.
    :param kind: What the tuples hold, for the error message, such as `"hooks"`.
    :param example: A name to show in the error message's example.
    :raises TypeError: If a field holds a single callable or a string instead of a tuple of
        callables, or holds anything that isn't callable.
    """
    owner = type(instance).__name__
    for field in fields:
        items = getattr(instance, field)
        # A string would otherwise be split into its characters.
        if callable(items) or isinstance(items, str):
            raise TypeError(
                _NOT_A_TUPLE.format(owner=owner, field=field, kind=kind, example=example)
            )
        frozen = tuple(items)
        for item in frozen:
            if not callable(item):
                raise TypeError(
                    _NOT_CALLABLE.format(owner=owner, field=field, kind=kind, item=item)
                )
        object.__setattr__(instance, field, frozen)
