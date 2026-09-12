"""Focused tests for the private projection and queue lifecycle seams."""

from typing import Any
from unittest.mock import AsyncMock, call

import pytest

from brainzgraphinator._projections import enrich_label
from brainzgraphinator._queue_lifecycle import declare_stream_queue


@pytest.mark.asyncio
async def test_projection_updates_only_the_injected_stats() -> None:
    tx = AsyncMock()
    stats = {
        "entities_enriched": 0,
        "entities_skipped_no_discogs_match": 0,
        "relationships_created": 0,
        "relationships_updated": 0,
        "relationships_skipped_missing_side": 0,
    }

    assert await enrich_label(tx, {"id": "mbid", "discogs_label_id": None}, stats) is True

    tx.run.assert_not_awaited()
    assert stats["entities_skipped_no_discogs_match"] == 1


@pytest.mark.asyncio
async def test_declares_source_queue_and_consumer_owned_dead_letter_path() -> None:
    exchange = AsyncMock()
    dead_letter_exchange = AsyncMock()
    dead_letter_queue = AsyncMock()
    queue = AsyncMock()
    channel = AsyncMock()
    channel.declare_exchange.side_effect = [exchange, dead_letter_exchange]
    channel.declare_queue.side_effect = [dead_letter_queue, queue]

    result: Any = await declare_stream_queue(channel, "brainzgraphinator", "artists")

    assert result is queue
    assert channel.declare_exchange.await_args_list == [
        call("groovemap-musicbrainz-artists", "fanout", durable=True, auto_delete=False),
        call("groovemap-musicbrainz-brainzgraphinator-artists.dlx", "fanout", durable=True, auto_delete=False),
    ]
    assert channel.declare_queue.await_args_list == [
        call(
            auto_delete=False,
            durable=True,
            name="groovemap-musicbrainz-brainzgraphinator-artists.dlq",
            arguments={"x-queue-type": "classic"},
        ),
        call(
            auto_delete=False,
            durable=True,
            name="groovemap-musicbrainz-brainzgraphinator-artists",
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": "groovemap-musicbrainz-brainzgraphinator-artists.dlx",
                "x-delivery-limit": 20,
            },
        ),
    ]
    dead_letter_queue.bind.assert_awaited_once_with(dead_letter_exchange)
    queue.bind.assert_awaited_once_with(exchange)
