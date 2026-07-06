"""Exceptions for errors in code generation."""


class CodegenError(Exception):
    """Base class for all code generation related exceptions."""


class InvalidTransitionError(CodegenError, ValueError):
    """Exception raised when a parameter list has an invalid parameter-kind ordering."""
