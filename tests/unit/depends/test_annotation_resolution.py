"""Test how inject handles annotations that aren't evaluated to objects."""

import __future__

import sys
import types
from typing import Any

import pytest

from stratae.depends import inject
from stratae.depends.exceptions import InjectionSignatureError

_PRELUDE = """
from typing import TYPE_CHECKING, Annotated
from stratae.depends import Depends, inject

if TYPE_CHECKING:
    from decimal import Decimal

def get_number() -> int:
    return 1
"""

requires_314 = pytest.mark.skipif(
    sys.version_info < (3, 14), reason="annotations are only evaluated lazily on 3.14+"
)


def _run(source: str, future_annotations: bool = False, **names: Any) -> dict[str, Any]:
    """Execute `source` after a shared prelude, with `names` bound."""
    flags = __future__.annotations.compiler_flag if future_annotations else 0
    namespace: dict[str, Any] = dict(names)
    exec(compile(_PRELUDE + source, "<test>", "exec", flags=flags), namespace)  # noqa: S102
    return namespace


def test_future_annotations_with_injected_parameter_raises():
    """
    A function from a module using the future import can't be injected.

    Given: A function compiled with `from __future__ import annotations`, with an
        injected parameter
    When: inject is applied to it
    Then: InjectionSignatureError explains the future import isn't supported
    """
    # Arrange
    handler = _run(
        "def handler(n: Annotated[int, Depends(get_number)]) -> int:\n    return n\n",
        future_annotations=True,
    )["handler"]

    # Act & Assert
    with pytest.raises(InjectionSignatureError, match="from __future__ import annotations"):
        inject(handler)


def test_provider_with_future_annotations_and_required_parameter_raises():
    """
    A provider from a module using the future import, with a required parameter, raises.

    Given: A provider compiled with the future import, taking a required parameter
    When: A function depending on it is injected
    Then: InjectionSignatureError names the provider
    """
    # Arrange
    get_label = _run(
        "def get_label(n: Annotated[int, Depends(get_number)]) -> str:\n    return str(n)\n",
        future_annotations=True,
    )["get_label"]
    handler = _run(
        "def handler(label: Annotated[str, Depends(get_label)]) -> str:\n    return label\n",
        get_label=get_label,
    )["handler"]

    # Act & Assert
    with pytest.raises(InjectionSignatureError, match="get_label"):
        inject(handler)


def test_future_annotations_provider_without_required_parameters_is_allowed():
    """
    A provider from a module using the future import still works with no required parameters.

    Given: A provider compiled with the future import, taking only a defaulted parameter
    When: A function depending on it is injected and called
    Then: The provider's value is injected
    """
    # Arrange
    get_label = _run(
        "def get_label(prefix: str = 'ok') -> str:\n    return prefix\n",
        future_annotations=True,
    )["get_label"]
    handler = _run(
        "def handler(label: Annotated[str, Depends(get_label)]) -> str:\n    return label\n",
        get_label=get_label,
    )["handler"]

    # Act
    result = inject(handler)()

    # Assert
    assert result == "ok"


def test_hand_quoted_annotation_is_allowed():
    """
    A hand-quoted forward reference, without the future import, doesn't block injection.

    Given: A function with a quoted annotation on an ordinary required parameter, and an
        injected parameter, compiled without the future import
    When: inject is applied and the function is called
    Then: The injected parameter is filled and the quoted one is passed through
    """
    # Arrange
    handler = _run(
        "def handler(user: 'User', n: Annotated[int, Depends(get_number)]) -> int:\n    return n\n"
    )["handler"]

    # Act
    result = inject(handler)(None)

    # Assert
    assert result == 1


@requires_314
def test_unresolvable_annotations_on_other_parameters_are_allowed():
    """
    A forward reference or TYPE_CHECKING-only name on another parameter doesn't break inject.

    Given: A function whose ordinary parameters are annotated with a class defined later
        and a name imported only under TYPE_CHECKING, plus an injected parameter
    When: inject is applied before the class is defined, and the function is called
    Then: The injected parameter is filled
    """
    # Arrange
    namespace = _run(
        "def handler(later: Later, amount: Decimal, n: Annotated[int, Depends(get_number)]):\n"
        "    return n\n"
        "handler = inject(handler)\n"
        "class Later: ...\n"
    )

    # Act
    result = namespace["handler"](None, None)

    # Assert
    assert result == 1


@requires_314
def test_injected_type_defined_later_is_allowed():
    """
    An injected parameter whose type is defined later is still injected.

    Given: A function whose injected parameter's type is a class defined later
    When: inject is applied before the class is defined, and the function is called
    Then: The injected parameter is filled
    """
    # Arrange
    namespace = _run(
        "def handler(n: Annotated[Later, Depends(get_number)]):\n"
        "    return n\n"
        "handler = inject(handler)\n"
        "class Later: ...\n"
    )

    # Act
    result = namespace["handler"]()

    # Assert
    assert result == 1


def test_hand_quoted_injected_parameter_raises():
    """
    An injected parameter whose whole annotation is quoted raises instead of going uninjected.

    Given: A function whose injected parameter's annotation is a string, compiled without
        the future import
    When: inject is applied to it
    Then: InjectionSignatureError names the parameter
    """
    # Arrange
    handler = _run("def handler(n: 'Annotated[int, Depends(get_number)]') -> int:\n    return n\n")[
        "handler"
    ]

    # Act & Assert
    with pytest.raises(InjectionSignatureError, match="parameter 'n' has a string annotation"):
        inject(handler)


def test_future_annotations_dataclass_with_injected_field_raises():
    """
    A dataclass from a module using the future import, with an injected field, raises.

    Its generated `__init__` doesn't carry the module's future flag, so it's caught by
    evaluating the field's string annotation instead.

    Given: A dataclass compiled with the future import, with an injected field
    When: inject is applied to it
    Then: InjectionSignatureError names the field
    """
    # Arrange
    module = types.ModuleType("stratae_test_future_dataclass")
    sys.modules[module.__name__] = module
    try:
        source = (
            _PRELUDE
            + "from dataclasses import dataclass\n"
            + "@dataclass\n"
            + "class Config:\n"
            + "    n: Annotated[int, Depends(get_number)]\n"
        )
        flags = __future__.annotations.compiler_flag
        exec(compile(source, "<test>", "exec", flags=flags), module.__dict__)  # noqa: S102

        # Act & Assert
        with pytest.raises(InjectionSignatureError, match="parameter 'n' has a string annotation"):
            inject(module.Config)
    finally:
        del sys.modules[module.__name__]
