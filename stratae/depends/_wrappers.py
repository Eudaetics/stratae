"""Wrappers for dependency injection in synchronous and asynchronous functions."""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Generator

from stratae.depends.exceptions import OverrideNotAllowedError

if TYPE_CHECKING:
    from stratae.depends.depends import DependsWrapper


def create_sync_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper], no_override: set[str]
) -> Callable[..., Any]:
    """Create a synchronous wrapper function that injects resolved dependencies."""
    deps_items = tuple(resolved_deps.items())

    @wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any):
        if not kwargs:
            return func(*args, **{k: v.provide() for k, v in deps_items})
        if no_override and kwargs.keys() & no_override:
            raise OverrideNotAllowedError(
                "Overriding these dependencies is not allowed: "
                f"{', '.join(k for k in kwargs if k in no_override)}"
            )
        return func(*args, **({k: v.provide() for k, v in deps_items if k not in kwargs} | kwargs))

    return sync_wrapper


def create_sync_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper], no_override: set[str]
) -> Callable[..., Any]:
    """Create a synchronous generator wrapper function that injects resolved dependencies."""
    deps_items = tuple(resolved_deps.items())

    @wraps(func)
    def sync_gen_wrapper(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        if not kwargs:
            yield from func(*args, **{k: v.provide() for k, v in resolved_deps.items()})
            return
        if no_override and any(k in no_override for k in kwargs):
            raise OverrideNotAllowedError(
                "Overriding these dependencies is not allowed: "
                f"{', '.join(k for k in kwargs if k in no_override)}"
            )
        yield from func(
            *args, **({k: v.provide() for k, v in deps_items if k not in kwargs} | kwargs)
        )

    return sync_gen_wrapper


def create_async_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper], no_override: set[str]
) -> Callable[..., Any]:
    """Create an asynchronous wrapper function that injects resolved dependencies."""
    deps_items = tuple(resolved_deps.items())

    @wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any):
        if not kwargs:
            return await func(
                *args,
                **{k: await v.aprovide() if v.is_async else v.provide() for k, v in deps_items},
            )
        if no_override and kwargs.keys() & no_override:
            raise OverrideNotAllowedError(
                "Overriding these dependencies is not allowed: "
                f"{', '.join(k for k in kwargs if k in no_override)}"
            )
        return await func(
            *args,
            **(
                {
                    k: await v.aprovide() if v.is_async else v.provide()
                    for k, v in deps_items
                    if k not in kwargs
                }
                | kwargs
            ),
        )

    return async_wrapper


def create_async_gen_wrapper(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper], no_override: set[str]
):
    """Create an asynchronous generator wrapper function that injects resolved dependencies."""
    deps_items = tuple(resolved_deps.items())

    @wraps(func)
    async def async_gen_wrapper(*args: Any, **kwargs: Any):
        if not kwargs:
            async for item in func(
                *args,
                **{k: await v.aprovide() if v.is_async else v.provide() for k, v in deps_items},
            ):
                yield item
            return

        if no_override and kwargs.keys() & no_override:
            raise OverrideNotAllowedError(
                "Overriding these dependencies is not allowed: "
                f"{', '.join(k for k in kwargs if k in no_override)}"
            )
        async for item in func(
            *args,
            **(
                {
                    k: await v.aprovide() if v.is_async else v.provide()
                    for k, v in deps_items
                    if k not in kwargs
                }
                | kwargs
            ),
        ):
            yield item

    return async_gen_wrapper
