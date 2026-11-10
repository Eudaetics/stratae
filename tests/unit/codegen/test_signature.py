"""Test suite for parameter-list signature rendering."""

from collections.abc import Callable
from inspect import Parameter, signature
from typing import Any

import pytest

from stratae.codegen import defaults, kwdefaults, render_arguments, render_parameters


def _typed_empty(): ...
def _typed_pk(x: int): ...
def _typed_pkd(x: int = 1): ...
def _typed_pk_pkd(x: int, y: int = 1): ...
def _typed_po_pod(x: int, y: int = 1, /): ...
def _typed_pk_pkd_ko(x: int, y: int = 1, *, z: int): ...
def _typed_po_pod_pkd(x: int, y: int = 1, /, z: int = 2): ...
def _typed_po_pod_ko(x: int, y: int = 1, /, *, z: int): ...
def _typed_po_pod_kod(x: int, y: int = 1, /, *, z: int = 2): ...
def _typed_va(*args: int): ...
def _typed_vk(**kwargs: int): ...
def _typed_va_vk(*args: int, **kwargs: int): ...
def _typed_pk_va(x: int, *args: int): ...
def _typed_pk_va_vk(x: int, *args: int, **kwargs: int): ...
def _typed_pk_pkd_va(x: int, y: int = 1, *args: int): ...
def _typed_va_ko(*args: int, z: int): ...
def _typed_va_kod(*args: int, z: int = 2): ...
def _typed_va_ko_vk(*args: int, z: int, **kwargs: int): ...
def _typed_po_va(x: int, /, *args: int): ...
def _typed_po_pod_va_ko_vk(x: int, y: int = 1, /, *args: int, z: int, **kwargs: int): ...
def _typed_pk_ko_vk(x: int, *, z: int, **kwargs: int): ...
def _typed_po_pod_ko_vk(x: int, y: int = 1, /, *, z: int, **kwargs: int): ...
def _typed_ko(*, z: int): ...
def _typed_ko_ko(*, a: int, b: int): ...
def _typed_po_vk(x: int, /, **kwargs: int): ...


_CASES: list[tuple[Callable[..., Any], str]] = [
    (_typed_empty, ""),
    (_typed_pk, "x"),
    (_typed_pkd, "x"),
    (_typed_pk_pkd, "x, y"),
    (_typed_po_pod, "x, y, /"),
    (_typed_pk_pkd_ko, "x, y, *, z"),
    (_typed_po_pod_pkd, "x, y, /, z"),
    (_typed_po_pod_ko, "x, y, /, *, z"),
    (_typed_po_pod_kod, "x, y, /, *, z"),
    (_typed_va, "*args"),
    (_typed_vk, "**kwargs"),
    (_typed_va_vk, "*args, **kwargs"),
    (_typed_pk_va, "x, *args"),
    (_typed_pk_va_vk, "x, *args, **kwargs"),
    (_typed_pk_pkd_va, "x, y, *args"),
    (_typed_va_ko, "*args, z"),
    (_typed_va_kod, "*args, z"),
    (_typed_va_ko_vk, "*args, z, **kwargs"),
    (_typed_po_va, "x, /, *args"),
    (_typed_po_pod_va_ko_vk, "x, y, /, *args, z, **kwargs"),
    (_typed_pk_ko_vk, "x, *, z, **kwargs"),
    (_typed_po_pod_ko_vk, "x, y, /, *, z, **kwargs"),
    (_typed_ko, "*, z"),
    (_typed_ko_ko, "*, a, b"),
    (_typed_po_vk, "x, /, **kwargs"),
]


@pytest.mark.parametrize(("func", "expected"), _CASES, ids=[func.__name__ for func, _ in _CASES])
def test_render_parameters(func: Callable[..., Any], expected: str):
    """
    render_parameters should render valid parameter-list source for any parameter-kind combination.

    Given: A function covering a specific combination of parameter kinds.
    When: Rendering its parameters with render_parameters.
    Then: The output should match the expected parameter-list source text.
    """
    # Arrange
    parameters = list(signature(func).parameters.values())

    # Act
    result = render_parameters(parameters)

    # Assert
    assert result == expected


def _every_kind(a: int = 1, /, b: int = 2, *args: int, c: int = 3, d: int, **kwargs: int) -> None:
    """Take a parameter of every kind, some with defaults."""


def _no_defaults(a: int, /, b: int, *, c: int) -> None:
    """Take parameters without defaults."""


def _parameters(func: Callable[..., Any]) -> list[Parameter]:
    return list(signature(func).parameters.values())


def test_render_arguments_forwards_every_kind():
    """
    render_arguments should forward each parameter in its calling convention.

    Given: A function taking a parameter of every kind
    When: Its call arguments are rendered
    Then: Positional ones are passed bare, *args and **kwargs unpacked, and keyword-only ones
        by name
    """
    # Act
    rendered = render_arguments(_parameters(_every_kind))

    # Assert
    assert rendered == "a, b, *args, c=c, d=d, **kwargs"


def test_render_arguments_takes_each_source_from_value():
    """
    render_arguments should take each argument's source from `value`, keeping its convention.

    Given: A function taking a parameter of every kind, and a value renderer
    When: Its call arguments are rendered with the renderer
    Then: Each argument is the renderer's source, unpacked or named as its kind requires
    """
    # Act
    rendered = render_arguments(_parameters(_every_kind), lambda param: f"v_{param.name}")

    # Assert
    assert rendered == "v_a, v_b, *v_args, c=v_c, d=v_d, **v_kwargs"


def test_defaults_read_every_kind():
    """
    Defaults and kwdefaults should read a function's defaults from its parameters.

    Given: A function with positional-only, positional-or-keyword and keyword-only defaults,
        and parameters without them
    When: Its defaults and keyword defaults are read from its parameters
    Then: They match the function's own __defaults__ and __kwdefaults__
    """
    # Act
    params = _parameters(_every_kind)

    # Assert
    assert defaults(params) == _every_kind.__defaults__ == (1, 2)
    assert kwdefaults(params) == _every_kind.__kwdefaults__ == {"c": 3}


def test_defaults_none_without_defaults():
    """
    Defaults and kwdefaults should be None for parameters without defaults.

    Given: A function whose parameters have no defaults
    When: Its defaults and keyword defaults are read
    Then: Both are None, as the function's own are
    """
    # Act
    params = _parameters(_no_defaults)

    # Assert
    assert defaults(params) is None
    assert _no_defaults.__defaults__ is None
    assert kwdefaults(params) is None
    assert _no_defaults.__kwdefaults__ is None
