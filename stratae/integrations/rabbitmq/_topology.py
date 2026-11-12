"""Declaring a consume config's queue, failure queues, exchange and bindings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pamqp.commands import Queue

from stratae.integrations.rabbitmq._retry import milliseconds, retry_delays, retry_queue_name
from stratae.integrations.rabbitmq.config import RabbitMQConsumeConfig, RabbitMQFailurePolicy

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel


async def _declare_queue(
    channel: AbstractChannel, config: RabbitMQConsumeConfig
) -> Queue.DeclareOk:
    """Declare config's queue, inferring its flags."""
    named = config.queue is not None
    return await channel.queue_declare(
        config.queue or "",
        durable=named if config.durable is None else config.durable,
        exclusive=(not named) if config.exclusive is None else config.exclusive,
        auto_delete=(not named) if config.auto_delete is None else config.auto_delete,
        arguments=None if config.arguments is None else dict(config.arguments),
    )


async def declare(channel: AbstractChannel, config: RabbitMQConsumeConfig) -> str:
    """Declare config's queue, its failure queues, and its exchange and bindings if any."""
    declared = await _declare_queue(channel, config)
    queue = config.queue or declared.queue or ""
    if config.on_failure is not None:
        await _declare_failure_queues(channel, queue, config.on_failure)
    if config.exchange is None:
        return queue
    await channel.exchange_declare(
        config.exchange,
        exchange_type=config.exchange_type,
        durable=config.exchange_durable,
    )
    for binding_key in config.binding_keys:
        await channel.queue_bind(queue, config.exchange, routing_key=binding_key)
    return queue


async def _declare_failure_queues(
    channel: AbstractChannel, queue: str, policy: RabbitMQFailurePolicy
) -> None:
    """Declare a policy's retry queues, each expiring back into `queue`, and dead letters."""
    for delay in dict.fromkeys(retry_delays(policy.backoff)):
        await channel.queue_declare(
            retry_queue_name(queue, delay),
            durable=True,
            arguments={
                "x-message-ttl": milliseconds(delay),
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue,
            },
        )
    dead_letter = policy.dead_letter_target(queue)
    if dead_letter is not None:
        await channel.queue_declare(dead_letter, durable=True)
