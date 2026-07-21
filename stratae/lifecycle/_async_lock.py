# Adapted from fairasyncrlock (https://github.com/Joshuaalbert/FairAsyncRLock),
# by Joshua George Albert, under the MIT License below. Behavior unchanged;
# type annotations and docstrings added to match this repo's conventions.
#
# MIT License
#
# Copyright (c) 2023 Joshua George Albert
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Fair reentrant lock for asyncio - FIFO-ordered, reentrant within the owning task."""

import asyncio
from collections import deque
from types import TracebackType
from typing import Any


class AsyncRLock:
    """A fair reentrant lock for asyncio - respects the order of acquisition."""

    def __init__(self) -> None:
        self._owner: asyncio.Task[Any] | None = None
        self._count = 0
        self._owner_transfer = False
        self._queue: deque[asyncio.Event] = deque()

    def is_owner(self, task: asyncio.Task[Any] | None = None) -> bool:
        """Whether task (default: the current task) currently holds the lock."""
        if task is None:
            task = asyncio.current_task()
        return self._owner == task

    def locked(self) -> bool:
        """Whether any task currently holds the lock."""
        return self._owner is not None

    def _try_acquire_uncontended(self, me: asyncio.Task[Any] | None) -> bool:
        """Acquire without queueing if reentrant, or the lock is free and not mid-handoff."""
        if self.is_owner(task=me):
            self._count += 1
            return True
        if self._count == 0 and not self._owner_transfer:
            self._owner = me
            self._count = 1
            return True
        return False

    def _recover_from_cancelled_wait(
        self, event: asyncio.Event, me: asyncio.Task[Any] | None
    ) -> None:
        """Unwind a cancelled queued wait, passing ownership on if the handoff already landed."""
        try:
            self._queue.remove(event)
        except ValueError:
            # Already popped for handoff when cancelled: take ownership, then
            # immediately release it again to pass it on to the next waiter.
            self._owner_transfer = False
            self._owner = me
            self._count = 1
            self._current_task_release()

    async def acquire(self) -> None:
        """Acquire the lock, queueing in FIFO order behind any task already waiting."""
        me = asyncio.current_task()
        if self._try_acquire_uncontended(me):
            return

        # Otherwise queue and wait our turn, FIFO.
        event = asyncio.Event()
        self._queue.append(event)

        try:
            await event.wait()
        except asyncio.CancelledError:
            self._recover_from_cancelled_wait(event, me)
            raise
        else:
            self._owner_transfer = False
            self._owner = me
            self._count = 1

    def _current_task_release(self) -> None:
        self._count -= 1
        if self._count == 0:
            self._owner = None
            if self._queue:
                event = self._queue.popleft()
                event.set()
                # Held until the woken waiter claims ownership, so no other task can
                # jump the queue between this release and that handoff.
                self._owner_transfer = True

    def release(self) -> None:
        """Release the lock; raises RuntimeError if the current task doesn't hold it."""
        me = asyncio.current_task()

        if self._owner is None:
            raise RuntimeError(f"Cannot release un-acquired lock. {me} tried to release.")

        if not self.is_owner(task=me):
            raise RuntimeError(f"Cannot release foreign lock. {me} tried to unlock {self._owner}.")

        self._current_task_release()

    async def __aenter__(self) -> "AsyncRLock":
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
