"""Runtime AMQP identifiers for the brainzgraphinator consumer.

ADR 0005 (docs/adr/0005-source-owned-catalog-ingestion.md in the
groovemap-music/design repository) freezes this service's exchange and
queue identifiers across the catalog-ingestion split: exchanges remain
``{exchange_prefix}-{entity}`` and consumer queues remain
``{exchange_prefix}-{consumer}-{entity}``, with ``.dlx``/``.dlq`` suffixes
for their dead-letter exchange and queue.

The musicbrainz-only v1 binding promoted into
``brainzgraphinator/catalog_contract.py`` still exposes ``exchange_name``
and ``queue_name``, but no longer exposes dead-letter helpers directly (the
retired combined binding did). This module is the single local adapter the
service and tests import queue/exchange/dead-letter names from, deriving
the dead-letter names from the promoted binding's ``queue_name`` exactly as
the retired helpers did.
"""

from __future__ import annotations

from brainzgraphinator.catalog_contract import exchange_name as _exchange_name
from brainzgraphinator.catalog_contract import queue_name as _queue_name


def exchange_name(entity: str) -> str:
    """Return the fanout exchange name for a MusicBrainz entity."""
    return _exchange_name(entity)


def queue_name(consumer: str, entity: str) -> str:
    """Return the durable consumer queue name."""
    return _queue_name(consumer, entity)


def dead_letter_exchange_name(consumer: str, entity: str) -> str:
    """Return the dead-letter exchange name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlx"


def dead_letter_queue_name(consumer: str, entity: str) -> str:
    """Return the dead-letter queue name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlq"
