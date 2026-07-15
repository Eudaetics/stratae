"""Test suite for parameter-list signature rendering."""

from inspect import Parameter, signature
from typing import Any, Callable

import pytest

from stratae.codegen import InvalidTransitionError, render_parameters


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


_INVALID_TRANSITIONS: list[tuple[list[Parameter], str]] = [
    (
        [
            Parameter("z", kind=Parameter.KEYWORD_ONLY),
            Parameter("x", kind=Parameter.POSITIONAL_ONLY),
        ],
        "keyword_only_before_positional_only",
    ),
    (
        [
            Parameter("args", kind=Parameter.VAR_POSITIONAL),
            Parameter("x", kind=Parameter.POSITIONAL_ONLY),
        ],
        "var_positional_before_positional_only",
    ),
    (
        [
            Parameter("args", kind=Parameter.VAR_POSITIONAL),
            Parameter("x", kind=Parameter.POSITIONAL_OR_KEYWORD),
        ],
        "var_positional_before_standard",
    ),
    (
        [
            Parameter("kwargs", kind=Parameter.VAR_KEYWORD),
            Parameter("x", kind=Parameter.POSITIONAL_ONLY),
        ],
        "var_keyword_before_positional_only",
    ),
    (
        [
            Parameter("kwargs", kind=Parameter.VAR_KEYWORD),
            Parameter("x", kind=Parameter.POSITIONAL_OR_KEYWORD),
        ],
        "var_keyword_before_standard",
    ),
    (
        [
            Parameter("kwargs", kind=Parameter.VAR_KEYWORD),
            Parameter("args", kind=Parameter.VAR_POSITIONAL),
        ],
        "var_keyword_before_var_positional",
    ),
    (
        [
            Parameter("kwargs", kind=Parameter.VAR_KEYWORD),
            Parameter("z", kind=Parameter.KEYWORD_ONLY),
        ],
        "var_keyword_before_keyword_only",
    ),
    (
        [
            Parameter("z", kind=Parameter.KEYWORD_ONLY),
            Parameter("args", kind=Parameter.VAR_POSITIONAL),
        ],
        "keyword_only_before_var_positional",
    ),
]


@pytest.mark.parametrize(
    "parameters",
    [case for case, _ in _INVALID_TRANSITIONS],
    ids=[case_id for _, case_id in _INVALID_TRANSITIONS],
)
def test_render_parameters_invalid_transition(parameters: list[Parameter]):
    """
    render_parameters should reject a parameter list with an invalid kind ordering.

    Given: A parameter list whose parameter kinds are out of valid signature order.
    When: Rendering those parameters with render_parameters.
    Then: An InvalidTransitionError should be raised.
    """
    # Act & Assert
    with pytest.raises(InvalidTransitionError):
        render_parameters(parameters)
