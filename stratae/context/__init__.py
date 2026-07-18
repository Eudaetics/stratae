"""
Context variables for sharing values across a call stack without threading parameters.

`Context` wraps a `contextvars.ContextVar`: set a value once with `.set()`
or `with ctx.use(value):`, and any code running underneath that point
can read it back by calling the `Context` instance directly. Because
`Context` instances are callable, they work as `Depends()` providers,
letting runtime values (a request's user ID, a feature flag, a connection)
flow into injected functions without changing their signatures.

Examples:
    Setting and reading a value across nested scopes:

    .. code-block:: python

        from stratae.context import Context

        user_id = Context[int]("user_id")

        with user_id.use(123):
            assert user_id.get() == 123
            with user_id.use(42):
                assert user_id() == 42
            assert user_id() == 123

    An A/B test, where the `Context` holds the function to run:

    .. code-block:: python

        from typing import Callable

        from stratae.context import Context
        from stratae.depends import Depends, Injected, inject

        def classic_checkout() -> str: ...
        def one_click_checkout() -> str: ...

        checkout_renderer = Context[Callable[[], str]](
            "checkout_renderer", default=classic_checkout
        )

        @inject
        def checkout_page(
            render: Injected[Callable[[], str], Depends(checkout_renderer)],
        ) -> str:
            return render()

        checkout_page()  # control: classic checkout

        with checkout_renderer.use(one_click_checkout):
            checkout_page()  # experiment group: one-click checkout

"""

from .context import IGNORE, Context

__all__ = ["IGNORE", "Context"]
