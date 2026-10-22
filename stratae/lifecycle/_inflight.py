"""
Tracking of in-flight first computations, so concurrent misses compute each entry once.

The first caller claims an entry under a brief lock and computes with it released, since
the computation may call other cached functions. Later callers wait for the release, then
re-check the cache. Shared scopes keep `Pending`/`AsyncPending` claims in a locked
per-wrapper dict. Context scopes' sync wrappers use the same locked dict, keyed per
activation, since threads can share an activation through a copied context; the claim is
the owning thread's id, swapped for a `Pending` once another thread waits. Context scopes'
async wrappers use the owning task as a lock-free claim, swapped for a `WatchedClaim` once
another task waits.
"""

import asyncio
import threading
from typing import Any, Hashable

from stratae.lifecycle.exceptions import CacheCycleError, LifecycleConfigurationError


class Pending:
    """A claimed sync computation: its owning thread, and an event created on first wait."""

    __slots__ = ("owner", "done")

    def __init__(self, owner: int | None = None) -> None:
        """Claim for thread `owner`, or the calling thread if omitted."""
        self.owner = threading.get_ident() if owner is None else owner
        self.done: threading.Event | None = None

    def watch(self) -> threading.Event:
        """
        Return the event to wait on. Call under the wrapper's lock.

        :raises CacheCycleError: If the calling thread owns this computation.
        """
        if self.owner == threading.get_ident():
            raise CacheCycleError("Cached call depends on its own in-progress computation.")
        if self.done is None:
            self.done = threading.Event()
        return self.done

    def release(self) -> None:
        """Wake every caller waiting on this computation."""
        if self.done is not None:
            self.done.set()


class AsyncPending:
    """
    A claimed async computation: its owning task, and a future per waiter.

    Waiters may be on other threads' loops, so each future is completed on its own loop.
    """

    __slots__ = ("owner", "waiters")

    def __init__(self) -> None:
        """Claim for the calling task."""
        self.owner = asyncio.current_task()
        self.waiters: list[asyncio.Future[None]] | None = None

    def watch(self) -> asyncio.Future[None]:
        """
        Return a future to await. Call under the wrapper's lock.

        :raises CacheCycleError: If the calling task owns this computation.
        """
        if self.owner is asyncio.current_task():
            raise CacheCycleError("Cached call depends on its own in-progress computation.")
        waiter = asyncio.get_running_loop().create_future()
        if self.waiters is None:
            self.waiters = [waiter]
        else:
            self.waiters.append(waiter)
        return waiter

    def release(self) -> None:
        """Wake every waiting task on its own loop."""
        for waiter in self.waiters or ():
            if not waiter.done():
                waiter.get_loop().call_soon_threadsafe(_wake, waiter)


def _wake(waiter: asyncio.Future[None]) -> None:
    """Complete `waiter` on its loop, unless it was cancelled."""
    if not waiter.done():
        waiter.set_result(None)


class WatchedClaim:
    """A context scope's claim once a task waits on it: the owner and an event."""

    __slots__ = ("owner", "done")

    def __init__(self, owner: asyncio.Task[Any]) -> None:
        """Watch `owner`'s claim, with an event for it to set on release."""
        self.owner = owner
        self.done = asyncio.Event()


async def wait_for_claim(claims: dict[Hashable, Any], key: Hashable, claim: Any) -> None:
    """
    Wait for another task's claim on `key` to be released.

    The claim's event can only be set from its owner's loop, so a waiter on another loop
    would never wake; it raises instead.

    :raises CacheCycleError: If the calling task owns the claim.
    :raises LifecycleConfigurationError: If the claim's owner runs on another event loop.
    """
    watched = claim.__class__ is WatchedClaim
    owner = claim.owner if watched else claim
    if owner is asyncio.current_task():
        raise CacheCycleError("Cached call depends on its own in-progress computation.")
    if owner.get_loop() is not asyncio.get_running_loop():
        raise LifecycleConfigurationError(
            "Cached call awaited on a different event loop than the one computing it. "
            "Event loops on other threads sharing an activation need a scope created with "
            "threads=True."
        )
    if not watched:
        claim = claims[key] = WatchedClaim(claim)
    await claim.done.wait()


def watch_thread_claim(claims: dict[Hashable, Any], key: Hashable, claim: Any) -> threading.Event:
    """
    Return the event to wait on for a thread's claim on `key`. Call under the wrapper's lock.

    :raises CacheCycleError: If the calling thread owns the claim.
    """
    if claim.__class__ is not Pending:
        claim = claims[key] = Pending(claim)
    return claim.watch()
