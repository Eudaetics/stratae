"""
Tests for source held as lists of lines.

This test suite verifies the following behaviors:

Blocks:
- A block is its header followed by its body, indented one level under it, and blocks nest.
"""

from stratae.codegen import block


def test_block_nests_its_body_under_its_header():
    """
    A block should indent its body under its header, so blocks nest.

    Given: A block whose body holds another block.
    When: Building it.
    Then: Each level of body is indented four more spaces.
    """
    lines = block("while True:", [*block("if x:", ["break"]), "x = 1"])

    assert lines == ["while True:", "    if x:", "        break", "    x = 1"]
