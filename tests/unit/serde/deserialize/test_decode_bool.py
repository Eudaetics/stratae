"""Test decode_bool directly: confirm-only, or parse "true"/"false" case-insensitively."""

import pytest

from stratae.serde import decode_bool


@pytest.mark.parametrize(
    "value",
    ["true", "True", "TRUE", "tRuE"],
    ids=["lower", "title", "upper", "mixed"],
)
def test_decode_bool_accepts_true_string_case_insensitively(value: str):
    """
    "true", in any casing, is accepted for a bool target.

    Given: A string spelling "true" in some casing.
    When: decode_bool is called with that value.
    Then: True is returned.
    """
    assert decode_bool(value) is True


@pytest.mark.parametrize(
    "value",
    ["false", "False", "FALSE", "fAlSe"],
    ids=["lower", "title", "upper", "mixed"],
)
def test_decode_bool_accepts_false_string_case_insensitively(value: str):
    """
    "false", in any casing, is accepted for a bool target.

    Given: A string spelling "false" in some casing.
    When: decode_bool is called with that value.
    Then: False is returned.
    """
    assert decode_bool(value) is False


def test_decode_bool_rejects_other_strings():
    """
    A string other than "true"/"false" is rejected for a bool target.

    Given: The string "yes".
    When: decode_bool is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_bool("yes")


def test_decode_bool_rejects_int():
    """
    An int is rejected for a bool target.

    Given: An int value.
    When: decode_bool is called with that value.
    Then: TypeError is raised.
    """
    with pytest.raises(TypeError):
        decode_bool(1)
