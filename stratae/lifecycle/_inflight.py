"""
Per-wrapper tracking of first computations still running, for wrappers that claim misses.

A cached wrapper whose callers can overlap - any wrapper in a shared scope, and an async
wrapper in a context scope - must compute each entry once even when several callers miss
it at the same time. It can't hold a lock while computing: the computation may call other
cached functions, or the same function with another key, and a held non-reentrant lock
would deadlock it. Instead, the first caller claims the entry under a brief lock, computes
with the lock released, then stores and releases. Later callers that find the entry
claimed wait for that release, then re-check the cache.

A shared scope's wrapper keeps its claims in a plain dict of cache key to `Pending` (sync)
or `AsyncPending` (async), guarded by the wrapper's lock, since its callers may be on any
thread. A context scope's async wrappers keep theirs in the activation's own `claims` dict
on its exit stack, with no lock: those callers are tasks sharing one event loop, and the
claim bookkeeping never awaits, so it can't interleave. There a claim is just the owning
task, so an uncontended miss allocates nothing; the first waiter swaps in a `WatchedClaim`.
See `_write_claimed_compute` and `_write_task_claimed_compute` in
`stratae.lifecycle._wrappers`.
"""

import asyncio
import threading
from typing import Any, Hashable

from stratae.lifecycle.exceptions import CacheCycleError


class Pending:
    """
    A claimed entry's sync computation - its owning thread, and an event set on release.

    The event is only created once another caller waits, so an uncontended miss never
    allocates one.
    """

    __slots__ = ("owner", "done")

    def __init__(self) -> None:
        """Claim for the calling thread."""
        self.owner = threading.get_ident()
        self.done: threading.Event | None = None

    def watch(self) -> threading.Event:
        """
        Return the event to wait on until this computation is released.

        Must be called under the wrapper's lock, which `release` also holds.

        :raises CacheCycleError: If the calling thread owns this computation - its sync
            computation can't progress while it waits on itself.
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
    A claimed entry's async computation - its owning task, and a future per waiting task.

    A shared scope's waiters can be on other threads' event loops, so each waits on a
    future of its own loop, which `release` completes through that loop thread-safely.
    No future is created until a task waits, so an uncontended miss allocates nothing.
    """

    __slots__ = ("owner", "waiters")

    def __init__(self) -> None:
        """Claim for the calling task."""
        self.owner = asyncio.current_task()
        self.waiters: list[asyncio.Future[None]] | None = None

    def watch(self) -> asyncio.Future[None]:
        """
        Return a future to await until this computation is released.

        Must be called under the wrapper's lock, which `release` also holds.

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
        """Wake every task still waiting on this computation, each on its own loop."""
        for waiter in self.waiters or ():
            if not waiter.done():
                waiter.get_loop().call_soon_threadsafe(_wake, waiter)


def _wake(waiter: asyncio.Future[None]) -> None:
    """Complete `waiter` on its own loop, unless it was cancelled since being scheduled."""
    if not waiter.done():
        waiter.set_result(None)


class WatchedClaim:
    """A context scope's claim once another task waits on it: its owning task, and an event."""

    __slots__ = ("owner", "done")

    def __init__(self, owner: asyncio.Task[Any]) -> None:
        """Watch `owner`'s claim, with an event for it to set on release."""
        self.owner = owner
        self.done = asyncio.Event()


async def wait_for_claim(claims: dict[Hashable, Any], key: Hashable, claim: Any) -> None:
    """
    Wait for another task's claim on `key` in a context scope activation to be released.

    `claim` is the owning task itself until a first waiter swaps in a WatchedClaim, whose
    event the owner sets when it releases the claim.

    :raises CacheCycleError: If the calling task owns the claim.
    """
    if claim.__class__ is not WatchedClaim:
        if claim is asyncio.current_task():
            raise CacheCycleError("Cached call depends on its own in-progress computation.")
        claim = claims[key] = WatchedClaim(claim)
    elif claim.owner is asyncio.current_task():
        raise CacheCycleError("Cached call depends on its own in-progress computation.")
    await claim.done.wait()
