"""Writer validation tests for code generation."""

import pytest

from stratae.codegen import Writer


def test_writer_init():
    """
    Writer init should default to PEP 8 conventions.

    Given: A Writer class.
    When: Initializing an instance of that class.
    Then: The whitespace rules should follow PEP 8 conventions.
    """
    # Arrange & Act
    writer = Writer()

    # Assert
    assert writer._nesting == 0  # pyright: ignore[reportPrivateUsage]


def test_writer_write():
    """
    Writer write should append a line at the current nesting level.

    Given: A Writer instance at nesting level 0.
    When: Writing a line.
    Then: The rendered output should be the line with no indentation.
    """
    # Arrange
    writer = Writer()

    # Act
    writer.write("x = 1")

    # Assert
    assert writer.render() == "x = 1"


def test_writer_write_strips_whitespace():
    """
    Writer write should strip leading and trailing whitespace from input.

    Given: A Writer instance.
    When: Writing a line with surrounding whitespace.
    Then: The whitespace should be stripped before the indent prefix is applied.
    """
    # Arrange
    writer = Writer()

    # Act
    writer.write("\t x = 1   ")

    # Assert
    assert writer.render() == "x = 1"


def test_writer_write_blank_line():
    """
    Writer write should produce a blank line with no prefix for empty input.

    Given: A Writer instance inside a block.
    When: Writing an empty string.
    Then: The line should be blank with no indentation prefix.
    """
    # Arrange
    writer = Writer()

    # Act
    with writer.block():
        writer.write("")

    # Assert
    assert writer.render() == ""


def test_writer_write_indented():
    """
    Writer write should apply the current indentation level to non-empty lines.

    Given: A Writer instance inside a block.
    When: Writing a line.
    Then: The line should be prefixed with one indent unit.
    """
    # Arrange
    writer = Writer()

    # Act
    with writer.block():
        writer.write("x = 1")

    # Assert
    assert writer.render() == "    x = 1"


def test_writer_block_nested():
    """
    Writer block should accumulate indentation across nested blocks.

    Given: A Writer instance.
    When: Writing inside two nested blocks.
    Then: The line should be prefixed with two indent units.
    """
    # Arrange
    writer = Writer()

    # Act
    with writer.block(), writer.block():
        writer.write("x = 1")

    # Assert
    assert writer.render() == "        x = 1"


def test_writer_block_restores_after_block():
    """
    Writer block should restore the nesting level after the block exits.

    Given: A Writer instance.
    When: Exiting a block.
    Then: Subsequent lines should be written at the prior nesting level.
    """
    # Arrange
    writer = Writer()

    # Act
    with writer.block():
        pass
    writer.write("x = 1")

    # Assert
    assert writer.render() == "x = 1"


def test_writer_block_restores_after_exception():
    """
    Writer block should restore the nesting level even when the block raises.

    Given: A Writer instance.
    When: An exception is raised inside a block.
    Then: The nesting level should be restored to its prior value.
    """
    # Arrange
    writer = Writer()

    # Act
    with pytest.raises(RuntimeError), writer.block():
        raise RuntimeError

    # Assert
    assert writer._nesting == 0  # pyright: ignore[reportPrivateUsage]


def test_writer_render():
    """
    Writer render should join all written lines with newlines.

    Given: A Writer instance with multiple lines written.
    When: Calling render.
    Then: The output should be all lines joined by newline characters.
    """
    # Arrange
    writer = Writer()
    writer.write("a")
    writer.write("b")
    writer.write("c")

    # Act
    result = writer.render()

    # Assert
    assert result == "a\nb\nc"


def test_writer_render_empty():
    """
    Writer render should return an empty string when no lines have been written.

    Given: A Writer instance with no lines written.
    When: Calling render.
    Then: The output should be an empty string.
    """
    # Arrange
    writer = Writer()

    # Act
    result = writer.render()

    # Assert
    assert result == ""


def test_writer_str():
    """
    Writer str should produce the same output as render.

    Given: A Writer instance with lines written.
    When: Calling str on the instance.
    Then: The output should match render.
    """
    # Arrange
    writer = Writer()
    writer.write("x = 1")

    # Act
    result = str(writer)

    # Assert
    assert result == writer.render()
