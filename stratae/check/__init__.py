"""
Decorators for running guard checks before a function.

`require` takes one or more zero-arg callables and runs them, in order,
before the wrapped function. Their return values are discarded — only
side effects and raises matter. A raise from any check propagates
immediately, stopping the remaining checks and the wrapped function.

Sync functions only accept sync checks. Async functions accept a mix of
sync and async checks.

Example:
    @require(is_admin)
    def foo(): ...

"""

from stratae.check.require import require

__all__ = ["require"]
