"""
Tracking of in-flight first computations, so concurrent misses compute each entry once.

The first caller claims an entry under a brief lock and computes with it released, since
the computation may call other cached functions. Later callers wait for the release, then
re-check the cache. Shared scopes keep claims in a locked per-wrapper dict: the owning
thread's id or task, swapped for a `Pending` or `AsyncPending` once another caller waits.
Context scopes with `threads=True` use the same locked dict, keyed per activation, since
threads can share an activation through a copied context. Context scopes'
async wrappers use the owning task as a lock-free claim in a per-wrapper dict, also keyed
per activation, swapped for a `WatchedClaim` once another task waits.
"""

import asyncio
import threading
from collections.abc import Hashable

from stratae.lifecycle.exceptions import (
    CacheCycleError,
    CrossLoopWaitError,
)


class Pending:
    """A claimed sync computation: its owning thread, and an event created on first wait."""

    __slots__ = ("done", "owner")

    def __init__(self, owner: int) -> None:
        """Claim for thread `owner`."""
        self.owner = owner
        self.done: threading.Event | None = None

    def watch(self) -> threading.Event:
        """
        Return the event to wait on. Call under the wrapper's lock.

        :raises CacheCycleError: If the calling thread owns this computation.
        """
        if self.owner == threading.get_ident():
            raise CacheCycleError()
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

    def __init__(self, owner: asyncio.Task[object] | None) -> None:
        """Claim for task `owner`."""
        self.owner = owner
        self.waiters: list[asyncio.Future[None]] | None = None

    def watch(self) -> asyncio.Future[None]:
        """
        Return a future to await. Call under the wrapper's lock.

        :raises CacheCycleError: If the calling task owns this computation.
        """
        if self.owner is asyncio.current_task():
            raise CacheCycleError()
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

    __slots__ = ("done", "owner")

    def __init__(self, owner: asyncio.Task[object]) -> None:
        """Watch `owner`'s claim, with an event for it to set on release."""
        self.owner = owner
        self.done = asyncio.Event()


async def wait_for_claim(
    claims: dict[Hashable, asyncio.Task[object] | WatchedClaim],
    key: Hashable,
    claim: asyncio.Task[object] | WatchedClaim,
) -> None:
    """
    Wait for another task's claim on `key` to be released.

    The claim's event can only be set from its owner's loop, so a waiter on another loop
    would never wake; it raises instead.

    :raises CacheCycleError: If the calling task owns the claim.
    :raises LifecycleConfigurationError: If the claim's owner runs on another event loop.
    """
    if isinstance(claim, WatchedClaim):
        watched, owner = claim, claim.owner
    else:
        watched, owner = None, claim
    if owner is asyncio.current_task():
        raise CacheCycleError()
    if owner.get_loop() is not asyncio.get_running_loop():
        raise CrossLoopWaitError()
    if watched is None:
        watched = claims[key] = WatchedClaim(owner)
    await watched.done.wait()


def watch_thread_claim(
    claims: dict[Hashable, int | Pending], key: Hashable, claim: int | Pending
) -> threading.Event:
    """
    Return the event to wait on for a thread's claim on `key`. Call under the wrapper's lock.

    :raises CacheCycleError: If the calling thread owns the claim.
    """
    if not isinstance(claim, Pending):
        claim = claims[key] = Pending(claim)
    return claim.watch()


def watch_task_claim(
    claims: dict[Hashable, asyncio.Task[object] | AsyncPending],
    key: Hashable,
    claim: asyncio.Task[object] | AsyncPending,
) -> asyncio.Future[None]:
    """
    Return a future to await for a task's claim on `key`. Call under the wrapper's lock.

    :raises CacheCycleError: If the calling task owns the claim.
    """
    if not isinstance(claim, AsyncPending):
        claim = claims[key] = AsyncPending(claim)
    return claim.watch()
