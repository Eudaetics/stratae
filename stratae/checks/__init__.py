"""
Run zero-arg guard checks, either directly or as a decorator.

`check`/`check_async` run a sequence of zero-arg checks that raise on
failure. `require` is the decorator form, running checks before a
wrapped function is called. See each function's docstring for details.

Example:
    Run checks directly::

        check(is_authenticated, is_admin)

    Or as a decorator, running the same checks before the call::

        @require(is_authenticated, is_admin)
        def delete_user(user_id: int) -> None: ...

"""

from .check import check, check_async
from .require import require

__all__ = ["check", "check_async", "require"]
