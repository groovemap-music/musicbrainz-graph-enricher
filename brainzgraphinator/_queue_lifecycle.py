"""RabbitMQ topology owned by the MusicBrainz graph consumer."""

from typing import Any

from brainzgraphinator.catalog_contract import AMQP_EXCHANGE_TYPE
from brainzgraphinator.queue_names import (
    dead_letter_exchange_name,
    dead_letter_queue_name,
    exchange_name,
    queue_name,
)


async def declare_stream_queue(channel: Any, consumer_name: str, data_type: str) -> Any:
    """Declare and bind one source stream and its consumer-owned dead-letter path."""
    exchange = await channel.declare_exchange(
        exchange_name(data_type),
        AMQP_EXCHANGE_TYPE,
        durable=True,
        auto_delete=False,
    )
    dlx_name = dead_letter_exchange_name(consumer_name, data_type)
    dlx_exchange = await channel.declare_exchange(
        dlx_name,
        AMQP_EXCHANGE_TYPE,
        durable=True,
        auto_delete=False,
    )
    dead_letter_queue = await channel.declare_queue(
        auto_delete=False,
        durable=True,
        name=dead_letter_queue_name(consumer_name, data_type),
        arguments={"x-queue-type": "classic"},
    )
    await dead_letter_queue.bind(dlx_exchange)

    queue = await channel.declare_queue(
        auto_delete=False,
        durable=True,
        name=queue_name(consumer_name, data_type),
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": dlx_name,
            "x-delivery-limit": 20,
        },
    )
    await queue.bind(exchange)
    return queue
