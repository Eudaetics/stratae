"""Run a scope's start hooks, and the exit hooks and success callbacks of an ended activation."""

import logging
from collections.abc import Callable, Sequence
from inspect import isawaitable

_log = logging.getLogger("stratae.lifecycle")

type EnterHook[A] = Callable[[A], object]
"""Called with the activation's token as it starts."""

type ExitHook[A] = Callable[[A, BaseException | None], object]
"""Called with the activation's token, and the exception it ended with or `None`."""

type SuccessCallback = Callable[[], object]
"""Called with no arguments once the activation it was registered in ends cleanly."""


class HookRecord[A]:
    """
    What one activation's hooks need: its token, its exit hooks, and its success callbacks.

    The exit hooks are the scope's as the activation started, so a hook registered or removed
    later doesn't change an activation already running.
    """

    __slots__ = ("activation", "callbacks", "exit_hooks")

    def __init__(self, activation: A, exit_hooks: tuple[ExitHook[A], ...]) -> None:
        self.activation = activation
        self.exit_hooks = exit_hooks
        self.callbacks: list[SuccessCallback] = []


def run_enter[A](hooks: Sequence[EnterHook[A]], activation: A) -> None:
    """Run start hooks in order; the first that raises stops the rest."""
    for hook in hooks:
        hook(activation)


async def run_enter_async[A](hooks: Sequence[EnterHook[A]], activation: A) -> None:
    """Run start hooks in order, awaiting async ones; the first that raises stops the rest."""
    for hook in hooks:
        result = hook(activation)
        if isawaitable(result):
            await result


def run_exit[A](scope: str, record: HookRecord[A] | None, error: BaseException | None) -> None:
    """Run success callbacks if the activation ended cleanly, then exit hooks, in order."""
    if record is None:
        return
    if error is None:
        for callback in record.callbacks:
            try:
                callback()
            except Exception:
                _log.exception("success callback %r for scope '%s' failed", callback, scope)
    for hook in record.exit_hooks:
        try:
            hook(record.activation, error)
        except Exception:
            _log.exception("exit hook %r for scope '%s' failed", hook, scope)


async def run_exit_async[A](
    scope: str, record: HookRecord[A] | None, error: BaseException | None
) -> None:
    """Run success callbacks if the activation ended cleanly, then exit hooks, awaiting each."""
    if record is None:
        return
    if error is None:
        for callback in record.callbacks:
            try:
                result = callback()
                if isawaitable(result):
                    await result
            except Exception:
                _log.exception("success callback %r for scope '%s' failed", callback, scope)
    for hook in record.exit_hooks:
        try:
            result = hook(record.activation, error)
            if isawaitable(result):
                await result
        except Exception:
            _log.exception("exit hook %r for scope '%s' failed", hook, scope)
