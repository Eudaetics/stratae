"""
Unit tests for compiling generated wrappers.

This test suite verifies the following behaviors:

compile_wrapper:
- It returns the `wrapper` function the source defines, running in the given namespace.
- The wrapper takes the target's metadata and its parameters' defaults.
- inspect.getsource on the wrapper's code returns the generated source.
- A traceback through the wrapper shows its source line.
- The wrapper's source is dropped once the wrapper is garbage collected.
- Two wrappers of the same target keep their own sources.
"""

import gc
import inspect
import linecache
import traceback
from inspect import signature

import pytest

from stratae.codegen import compile_wrapper

_SOURCE = ["def wrapper(x):", "    return __func__(x) + 1"]
_RAISING = ["def wrapper():", "    raise ValueError('generated')"]


def _target(x: int) -> int:
    return x * 2


_PARAMS = list(signature(_target).parameters.values())


def test_returns_wrapper_running_in_namespace():
    """
    compile_wrapper should return the wrapper, resolving names from the namespace.

    Given: Source defining `wrapper`, which calls `__func__` from its namespace.
    When: The source is compiled and the wrapper called.
    Then: The wrapper calls the namespace's `__func__`.
    """
    # Act
    wrapper = compile_wrapper(_SOURCE, _target, {"__func__": _target}, _PARAMS)

    # Assert
    assert wrapper(3) == 7


def test_getsource_returns_generated_source():
    """
    inspect.getsource on the wrapper's code should return the generated source.

    Given: A compiled wrapper.
    When: inspect.getsource is called on its code object.
    Then: It returns the source the wrapper was compiled from.
    """
    # Arrange
    wrapper = compile_wrapper(_SOURCE, _target, {"__func__": _target}, _PARAMS)

    # Act
    source = inspect.getsource(wrapper.__code__)

    # Assert
    assert source == "\n".join(_SOURCE)


def test_traceback_shows_source_line():
    """
    A traceback through the wrapper should show its source line.

    Given: A compiled wrapper that raises.
    When: It's called and the traceback formatted.
    Then: The traceback names the target and shows the raising line.
    """
    # Arrange
    wrapper = compile_wrapper(_RAISING, _target, {}, [])

    # Act
    with pytest.raises(ValueError, match="generated") as raised:
        wrapper()

    # Assert
    formatted = "".join(traceback.format_exception(raised.value))
    assert "_target#" in formatted
    assert "raise ValueError('generated')" in formatted


def test_source_dropped_when_wrapper_collected():
    """
    The wrapper's source should be dropped once the wrapper is garbage collected.

    Given: A compiled wrapper whose source is registered.
    When: The wrapper is deleted and garbage collected.
    Then: Its source is no longer registered.
    """
    # Arrange
    wrapper = compile_wrapper(_SOURCE, _target, {"__func__": _target}, _PARAMS)
    filename = wrapper.__code__.co_filename
    assert filename in linecache.cache

    # Act
    del wrapper
    gc.collect()

    # Assert
    assert filename not in linecache.cache


def test_wrappers_of_same_target_keep_own_source():
    """
    Two wrappers of the same target should keep their own sources.

    Given: Two different sources, both wrapping the same target.
    When: Both are compiled.
    Then: Each wrapper's code returns its own source.
    """
    # Act
    first = compile_wrapper(_SOURCE, _target, {"__func__": _target}, _PARAMS)
    second = compile_wrapper(_RAISING, _target, {}, [])

    # Assert
    assert inspect.getsource(first.__code__) == "\n".join(_SOURCE)
    assert inspect.getsource(second.__code__) == "\n".join(_RAISING)


def _defaulted(a: int, b: int = 2, *, c: int = 3) -> int:
    """Add up the arguments."""
    return a + b + c


def test_wrapper_takes_targets_metadata_and_defaults():
    """
    compile_wrapper should dress the wrapper as its target, with its parameters' defaults.

    Given: Source listing a target's parameters without their defaults.
    When: It's compiled with the target's parameters.
    Then: The wrapper has the target's name, docstring and `__wrapped__`, and its defaults.
    """
    # Arrange
    params = list(signature(_defaulted).parameters.values())
    lines = ["def wrapper(a, b, *, c):", "    return __func__(a, b, c=c)"]

    # Act
    wrapper = compile_wrapper(lines, _defaulted, {"__func__": _defaulted}, params)

    # Assert
    assert (wrapper.__name__, wrapper.__doc__) == ("_defaulted", "Add up the arguments.")
    assert wrapper.__wrapped__ is _defaulted
    assert wrapper(1) == 6
