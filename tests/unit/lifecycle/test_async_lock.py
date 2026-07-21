"""
Tests for AsyncRLock - the fair, reentrant asyncio lock backing shared-scope caching.

Adapted from fairasyncrlock's own test suite (https://github.com/Joshuaalbert/FairAsyncRLock).
See _async_lock.py for the original license.
"""

import asyncio

import pytest

from stratae.lifecycle._async_lock import AsyncRLock


async def test_reentrant_acquisition_does_not_block():
    """
    Verify that a task holding the lock can reacquire it without blocking.

    Given: a task that has already acquired the lock,
    When: that same task acquires it again while still holding it,
    Then: the nested acquisition should succeed immediately.
    """
    # Arrange
    lock = AsyncRLock()

    # Act & Assert
    async with lock:
        async with lock:
            assert lock.is_owner()


async def test_exclusion_blocks_other_tasks():
    """
    Verify that a second task cannot acquire the lock while another task holds it.

    Given: a lock held by the current task,
    When: another task attempts to acquire the same lock,
    Then: that task should not get in until the lock is released.
    """
    # Arrange
    lock = AsyncRLock()
    got_in = False

    async def other():
        nonlocal got_in
        async with lock:
            got_in = True

    # Act
    async with lock:
        task = asyncio.create_task(other())
        await asyncio.sleep(0)

        # Assert
        assert not got_in

    await task
    assert got_in


async def test_fairness_preserves_queue_order():
    """
    Verify that waiting tasks acquire the lock in the order they queued.

    Given: several tasks all queued behind a held lock,
    When: the lock is released,
    Then: they should acquire it in FIFO order.
    """
    # Arrange
    lock = AsyncRLock()
    order: list[int] = []

    async def worker(n: int):
        async with lock:
            order.append(n)

    # Act
    async with lock:
        tasks = [asyncio.create_task(worker(i)) for i in range(5)]
        await asyncio.sleep(0)

    await asyncio.gather(*tasks)

    # Assert
    assert order == list(range(5))


async def test_release_without_acquiring_raises():
    """
    Verify that releasing a lock nobody holds raises RuntimeError.

    Given: a fresh, unacquired lock,
    When: release() is called,
    Then: a RuntimeError should be raised.
    """
    # Arrange
    lock = AsyncRLock()

    # Act & Assert
    with pytest.raises(RuntimeError, match="Cannot release un-acquired lock."):
        lock.release()


async def test_release_by_non_owner_raises():
    """
    Verify that a task cannot release a lock held by another task.

    Given: a lock held by one task,
    When: a different task calls release(),
    Then: a RuntimeError should be raised, and the owner should keep holding it.
    """
    # Arrange
    lock = AsyncRLock()
    holding = asyncio.Event()
    release_now = asyncio.Event()

    async def holder():
        async with lock:
            holding.set()
            await release_now.wait()

    # Act
    holder_task = asyncio.create_task(holder())
    await holding.wait()

    with pytest.raises(RuntimeError, match="Cannot release foreign lock."):
        lock.release()

    # Assert
    assert lock.is_owner(task=holder_task)
    release_now.set()
    await holder_task


async def test_acquire_and_release_update_state():
    """
    Verify that acquiring and releasing tracks ownership and the reentrancy count.

    Given: a fresh lock,
    When: it's acquired and then released via the context manager,
    Then: ownership/count should reflect the acquisition while held, and clear after.
    """
    # Arrange
    lock = AsyncRLock()

    # Act & Assert
    async with lock:
        assert lock.is_owner()
        assert lock._count == 1  # pyright: ignore[reportPrivateUsage]

    assert not lock.locked()
    assert lock._count == 0  # pyright: ignore[reportPrivateUsage]


async def test_state_resets_after_exception():
    """
    Verify that an exception inside the lock's context still releases it.

    Given: a lock acquired via `async with`,
    When: the body raises,
    Then: the lock should be released and left in a clean state.
    """
    # Arrange
    lock = AsyncRLock()

    # Act & Assert
    with pytest.raises(ValueError, match="boom"):
        async with lock:
            raise ValueError("boom")

    assert not lock.locked()
    assert lock._count == 0  # pyright: ignore[reportPrivateUsage]


async def test_manual_acquire_release():
    """
    Verify that acquire()/release() work directly, without the context manager.

    Given: several tasks that call acquire() and release() explicitly,
    When: they run concurrently,
    Then: every task should eventually get in, one at a time.
    """
    # Arrange
    lock = AsyncRLock()
    result: list[int] = []

    async def worker(n: int):
        await lock.acquire()
        result.append(n)
        await asyncio.sleep(0)
        lock.release()

    # Act
    tasks = [asyncio.create_task(worker(i)) for i in range(5)]
    await asyncio.gather(*tasks)

    # Assert
    assert len(result) == 5


async def test_cancellation_while_queued_removes_waiter():
    """
    Verify that cancelling a task waiting on the lock removes it from the queue.

    Given: one task holding the lock and a second task queued behind it,
    When: the queued task is cancelled long before its turn,
    Then: it should never acquire the lock, and the lock should end up clean.
    """
    # Arrange
    lock = AsyncRLock()
    holder_acquired = asyncio.Event()
    holder_release = asyncio.Event()
    waiter_acquired = asyncio.Event()

    async def holder():
        async with lock:
            holder_acquired.set()
            await holder_release.wait()

    async def waiter():
        await holder_acquired.wait()
        async with lock:
            waiter_acquired.set()

    # Act
    holder_task = asyncio.create_task(holder())
    waiter_task = asyncio.create_task(waiter())
    await holder_acquired.wait()
    await asyncio.sleep(0)
    waiter_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await waiter_task

    holder_release.set()
    await holder_task

    # Assert
    assert not waiter_acquired.is_set()
    assert not lock.locked()
    assert len(lock._queue) == 0  # pyright: ignore[reportPrivateUsage]


async def test_nested_unrelated_lock_preserves_ownership():
    """
    Verify that acquiring a different lock inside doesn't disturb the outer lock's ownership.

    Given: a task holding lock1 that then acquires an unrelated lock2,
    When: lock2 is acquired and released while lock1 is still held,
    Then: the task should remain lock1's owner throughout.
    """
    # Arrange
    lock1 = AsyncRLock()
    lock2 = AsyncRLock()
    lock1_acquired = asyncio.Event()

    async def worker():
        async with lock1:
            lock1_acquired.set()
            await asyncio.sleep(0)

    # Act
    task = asyncio.create_task(worker())
    await lock1_acquired.wait()
    assert lock1.is_owner(task=task)

    async with lock2:
        # Assert
        assert lock1.is_owner(task=task)

    await task


async def test_stress_many_tasks_many_iterations():
    """
    Verify the lock survives many concurrent tasks repeatedly acquiring and releasing.

    Given: a lock shared by many tasks doing many acquire/release cycles each,
    When: they all run concurrently,
    Then: nothing should deadlock or raise, and the lock ends up unheld.
    """
    # Arrange
    lock = AsyncRLock()
    num_tasks = 20
    iterations = 50

    async def worker():
        for _ in range(iterations):
            async with lock:
                pass

    # Act
    tasks = [asyncio.create_task(worker()) for _ in range(num_tasks)]
    await asyncio.gather(*tasks)

    # Assert
    assert not lock.locked()


async def test_is_owner_and_locked_reflect_state():
    """
    Verify that is_owner()/locked() report the lock's current state accurately.

    Given: a fresh lock with no owner,
    When: it's acquired and then released,
    Then: is_owner()/locked() should reflect each state correctly.
    """
    # Arrange
    lock = AsyncRLock()

    # Act & Assert
    assert not lock.is_owner()
    assert not lock.locked()

    async with lock:
        assert lock.is_owner()
        assert lock.locked()

    assert not lock.is_owner()
    assert not lock.locked()


async def test_regression_cancelled_waiter_does_not_block_next_waiter():
    """
    Regression test for fairasyncrlock gh-14.

    Given: four tasks queued on the same lock, where the third is cancelled right around
        the moment the lock is handed to it,
    When: the holder eventually releases,
    Then: the cancelled task never acquires the lock, and the fourth task still can.
    """
    # Arrange
    lock = AsyncRLock()
    holder_acquired = asyncio.Event()
    second_started = asyncio.Event()
    third_started = asyncio.Event()
    third_acquired = asyncio.Event()
    fourth_started = asyncio.Event()
    fourth_acquired = asyncio.Event()
    holder_done = asyncio.Event()

    async def holder():
        async with lock:
            holder_acquired.set()
            await second_started.wait()
            await third_started.wait()
            await fourth_started.wait()
            await asyncio.sleep(0.1)
            holder_done.set()

    async def canceller():
        await holder_acquired.wait()
        second_started.set()
        await holder_done.wait()
        to_be_cancelled.cancel()

    async def cancellation_target():
        await second_started.wait()
        third_started.set()
        async with lock:
            third_acquired.set()

    async def fourth_waiter():
        await third_started.wait()
        await asyncio.sleep(0.1)
        fourth_started.set()
        async with lock:
            fourth_acquired.set()

    # Act
    holder_task = asyncio.create_task(holder())
    canceller_task = asyncio.create_task(canceller())
    to_be_cancelled = asyncio.create_task(cancellation_target())
    fourth_task = asyncio.create_task(fourth_waiter())

    await holder_task
    await canceller_task
    with pytest.raises(asyncio.CancelledError):
        await to_be_cancelled

    # Assert
    assert not third_acquired.is_set()
    await asyncio.wait_for(fourth_task, timeout=1)
    assert fourth_acquired.is_set()


async def test_regression_ownership_handoff_race():
    """
    Regression test for fairasyncrlock gh-17.

    Given: three tasks queued in sequence on the same lock,
    When: they acquire and release it one after another,
    Then: none should observe a foreign-lock error from a handoff race.
    """
    # Arrange
    lock = AsyncRLock()
    task1_acquired = asyncio.Event()
    task2_acquired = asyncio.Event()

    async def task1():
        async with lock:
            task1_acquired.set()
            await asyncio.sleep(0.1)

    async def task2():
        await task1_acquired.wait()
        async with lock:
            task2_acquired.set()
            await asyncio.sleep(0.2)

    async def task3():
        await asyncio.sleep(0.1)
        async with lock:
            await task2_acquired.wait()

    # Act & Assert - no exception means the handoff stayed race-free.
    await asyncio.gather(task1(), task2(), task3())
