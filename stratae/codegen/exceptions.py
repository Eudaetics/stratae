"""Exceptions for errors in code generation."""


class CodegenError(Exception):
    """Base class for all code generation related exceptions."""


class InvalidTransitionError(CodegenError, ValueError):
    """Exception raised when a parameter list has an invalid parameter-kind ordering."""

    def __init__(self, section: str, next_section: str, parameter: str) -> None:
        """
        Describe the invalid transition.

        :param section: The parameter kind the list was in.
        :param next_section: The kind it moved to.
        :param parameter: The parameter where it moved.
        """
        super().__init__(
            f"Cannot transition from {section} to {next_section} at parameter {parameter!r}."
        )


class InvalidIndentWidthError(CodegenError, ValueError):
    """Exception raised when a {py:class}`Writer <stratae.codegen.Writer>` gets a width below 1."""

    def __init__(self) -> None:
        """Describe the valid widths."""
        super().__init__(
            "Indent width must be a positive integer. "
            "The recommended indent width per PEP 8 is 4 spaces."
        )
