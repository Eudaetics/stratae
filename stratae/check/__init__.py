"""
Decorators for running guard checks before a function.

`require` takes one or more zero-arg callables and runs them before
the wrapped function in order. Any returned value is discarded. These
functions are used for their side effects, and for raising errors.
A raise from any check propagates immediately, stopping any remaining
checks as well as not running the wrapped function.

Example:
    .. code-block:: python

        @require(has_active_session, is_admin)
        def delete_user(user_id: int) -> None: ...

"""

from .require import require

__all__ = ["require"]
