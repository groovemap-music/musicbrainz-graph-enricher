from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import threading
import time
from asyncio import run
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from aio_pika.abc import AbstractIncomingMessage  # noqa: TC002 - runtime annotation introspection
from common import (
    AsyncResilientNeo4jDriver,
    AsyncResilientRabbitMQ,
    DatabaseUnavailableError,
    HealthServer,
    OutageBackoff,
    extract_context,
    flush_span,
    get_tracer,
    neo4j_security_kwargs,
    setup_logging,
    setup_telemetry,
    shutdown_telemetry,
    start_event_loop_monitor,
)
from common.media import families_of, map_musicbrainz_release, medium_label
from common.telemetry import get_meter, provider_generation
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from orjson import loads

from brainzgraphinator.catalog_contract import (
    AMQP_EXCHANGE_TYPE,
)
from brainzgraphinator.catalog_contract import (
    ENTITY_TYPES as MUSICBRAINZ_DATA_TYPES,
)
from brainzgraphinator.config import BrainzgraphinatorConfig
from brainzgraphinator.queue_names import (
    dead_letter_exchange_name as catalog_dead_letter_exchange_name,
)
from brainzgraphinator.queue_names import (
    dead_letter_queue_name as catalog_dead_letter_queue_name,
)
from brainzgraphinator.queue_names import (
    exchange_name as catalog_exchange_name,
)
from brainzgraphinator.queue_names import (
    queue_name as catalog_queue_name,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator


logger = structlog.get_logger(__name__)

SERVICE_NAME = "musicbrainz-graph-enricher"
# This value is part of the v1 catalog-event wire contract. Renaming it would
# create a second set of durable queues and strand messages in the existing set.
WIRE_CONSUMER_NAME = "brainzgraphinator"

STARTUP_BANNER = r"""
+-----------------------------------+
| GrooveMap                         |
| musicbrainz-graph-enricher        |
+-----------------------------------+
""".strip("\n")

# Config will be initialized in main
config: BrainzgraphinatorConfig | None = None

# Progress tracking
message_counts = {"artists": 0, "labels": 0, "release-groups": 0, "releases": 0}
progress_interval = 100  # Log progress every 100 messages
last_message_time = {
    "artists": 0.0,
    "labels": 0.0,
    "release-groups": 0.0,
    "releases": 0.0,
}
completed_files: set[str] = set()  # Track which files have completed processing

# Throttles requeues while Neo4j is unavailable, so an outage cannot burn the
# quorum queue's x-delivery-limit budget and dead-letter valid records.
outage_backoff = OutageBackoff(SERVICE_NAME)

# Consumer management
consumer_tags: dict[str, str] = {}  # {"artists": "consumer-tag-123", ...}
consumer_cancel_tasks: dict[str, asyncio.Task[None]] = {}  # {"artists": asyncio.Task, ...}
queues: dict[str, Any] = {}  # {"artists": queue_object, ...}
CONSUMER_CANCEL_DELAY = int(os.environ.get("CONSUMER_CANCEL_DELAY", "300"))  # Default 5 minutes

# Periodic queue checking settings
QUEUE_CHECK_INTERVAL = int(os.environ.get("QUEUE_CHECK_INTERVAL", "3600"))  # Default 1 hour

# Interval for checking stuck state (consumers died unexpectedly)
STUCK_CHECK_INTERVAL = int(os.environ.get("STUCK_CHECK_INTERVAL", "30"))  # Default 30 seconds

# Idle mode settings
STARTUP_IDLE_TIMEOUT = int(os.environ.get("STARTUP_IDLE_TIMEOUT", "30"))
IDLE_LOG_INTERVAL = int(os.environ.get("IDLE_LOG_INTERVAL", "300"))  # 5 min between idle status logs

# Idle mode state
idle_mode = False

# Driver will be initialized in main
graph: AsyncResilientNeo4jDriver | None = None

# Legacy batch knobs remain readable for environment compatibility. Processing
# is deliberately per delivery; these values do not currently change behavior.
BATCH_MODE = os.environ.get("NEO4J_BATCH_MODE", "true").lower() == "true"
BATCH_SIZE = int(os.environ.get("NEO4J_BATCH_SIZE", "100"))
BATCH_FLUSH_INTERVAL = float(os.environ.get("NEO4J_BATCH_FLUSH_INTERVAL", "5.0"))

# Connection state tracking
rabbitmq_manager: Any = None  # Will hold AsyncResilientRabbitMQ instance
active_connection: Any = None  # Current active connection
active_channel: Any = None  # Current active channel
connection_check_task: asyncio.Task[None] | None = None

# Global shutdown flag
shutdown_requested = False

# Lock for safely merging enrichment stats from concurrent handlers
# Lazy-initialized in first async method to avoid binding to wrong event loop
_stats_lock: asyncio.Lock | None = None

# Thread-safe lock for reading enrichment_stats from the health server thread
_stats_thread_lock = threading.Lock()

# Enrichment stats
enrichment_stats = {
    "entities_enriched": 0,
    "entities_skipped_no_discogs_match": 0,
    "relationships_created": 0,
    "relationships_updated": 0,
    "relationships_skipped_missing_side": 0,
}

# MusicBrainz relationship type mapping
MB_RELATIONSHIP_MAP: dict[str, str] = {
    "member of band": "MEMBER_OF",
    "collaboration": "COLLABORATED_WITH",
    "teacher": "TAUGHT",
    "tribute": "TRIBUTE_TO",
    "founder": "FOUNDED",
    "supporting musician": "SUPPORTED",
    "subgroup": "SUBGROUP_OF",
    "artist rename": "RENAMED_TO",
}

# ── Canonical media (ADR 0007) ───────────────────────────────────────────
#
# Every ISSUED_ON edge this service writes carries source: 'musicbrainz'. The value is the
# only thing separating our edges from the Discogs enricher's on the same release, so it is
# both the MERGE key (never a property set afterwards) and the filter that scopes the stale
# sweep. Medium and MediaFamily nodes are shared with the Discogs enricher by design: the
# node ids come from the shared vocabulary, so both enrichers MERGE the same nodes.
MEDIA_SOURCE = "musicbrainz"

# Reconciles the release-level media summary and removes this source's stale edges. It runs
# whenever the event carries (or yields) a media block, including a block with no items —
# an empty block is an authoritative statement that MusicBrainz knows no media for the
# release, so it clears both the summary and every musicbrainz-sourced edge.
#
# The DELETE is scoped by `source` and by the new medium id set, so a Discogs-sourced edge
# is never a candidate no matter which media it points at.
RELEASE_MEDIA_SUMMARY_CYPHER = (
    "MATCH (r:Release {id: $discogs_id}) "
    "SET r.mb_media_families = $families, "
    "    r.mb_medium_count = $medium_count "
    "WITH r "
    "MATCH (r)-[stale:ISSUED_ON]->(m:Medium) "
    "WHERE stale.source = $source AND NOT m.id IN $medium_ids "
    "DELETE stale"
)

# Merges one Medium per canonical medium id, files it under its MediaFamily, and attaches it
# to the release.
#
# `source` sits INSIDE the ISSUED_ON pattern rather than in the SET that follows it. A
# release can hold two edges to the same Medium — one from each catalog — and a bare
# `MERGE (r)-[e:ISSUED_ON]->(m)` would match the Discogs edge and overwrite its qty and
# source. Keying the MERGE on the source keeps the two edges distinct and leaves the Discogs
# edge untouched.
#
# Medium properties are ON CREATE only: whichever enricher first sees a medium names it, and
# neither rewrites the other's node on every event.
RELEASE_MEDIA_EDGES_CYPHER = (
    "MATCH (r:Release {id: $discogs_id}) "
    "UNWIND $items AS item "
    "MERGE (m:Medium {id: item.medium}) "
    "    ON CREATE SET m.family = item.family, m.label = item.label "
    "MERGE (f:MediaFamily {name: item.family}) "
    "MERGE (m)-[:IN_FAMILY]->(f) "
    "MERGE (r)-[e:ISSUED_ON {source: $source}]->(m) "
    "SET e.qty = item.qty"
)

# ── Telemetry ────────────────────────────────────────────────────────────
#
# Instruments follow the GrooveMap OpenTelemetry metrics conventions. `get_meter` and every
# instrument created from it are safe no-ops when the 'otel' extra is absent or no collector
# endpoint is configured (see common.telemetry) -- this service behaves identically either way.
INSTRUMENTATION_SCOPE = "groovemap.brainzgraphinator"
PIPELINE_SOURCE = "musicbrainz"
PIPELINE_STORE = "neo4j"

PIPELINE_MESSAGES = "groovemap.pipeline.messages"
PIPELINE_MESSAGE_DURATION = "groovemap.pipeline.message.duration"
PIPELINE_BATCH_SIZE = "groovemap.pipeline.batch.size"
PIPELINE_BATCH_FLUSH_DURATION = "groovemap.pipeline.batch.flush.duration"
PIPELINE_CONSUMERS_ACTIVE = "groovemap.pipeline.consumers.active"
# Recorded locally, matching common.runtime_metrics.record_consumed_message exactly: this
# service registers its handler with aio-pika's queue.consume() directly rather than going
# through common.process_message_with_retry, so the shared wrapper never sees these deliveries.
MESSAGING_CONSUMED_MESSAGES = "messaging.client.consumed.messages"
MESSAGING_OPERATION_DURATION = "messaging.client.operation.duration"

# Instruments are rebuilt whenever the installed MeterProvider changes (tracked by
# provider_generation()), the same seam common.runtime_metrics uses -- so a cache built
# against the no-op provider before setup_telemetry() runs is replaced rather than silently
# discarding every later measurement, and tests can install an in-memory provider mid-run.
_instruments_lock = threading.Lock()
_instruments: dict[str, Any] = {}
_instrument_generation = -1


def _build_instruments() -> dict[str, Any]:
    """Create one instrument per telemetry metric from the current provider."""
    meter = get_meter(INSTRUMENTATION_SCOPE)
    return {
        PIPELINE_MESSAGES: meter.create_counter(
            PIPELINE_MESSAGES,
            description="Catalog pipeline messages handled, by entity and outcome.",
        ),
        PIPELINE_MESSAGE_DURATION: meter.create_histogram(
            PIPELINE_MESSAGE_DURATION,
            unit="s",
            description="Duration of handling one pipeline message.",
        ),
        PIPELINE_BATCH_SIZE: meter.create_histogram(
            PIPELINE_BATCH_SIZE,
            unit="{items}",
            description="Number of records written to the store in one flush.",
        ),
        PIPELINE_BATCH_FLUSH_DURATION: meter.create_histogram(
            PIPELINE_BATCH_FLUSH_DURATION,
            unit="s",
            description="Duration of flushing records to the store.",
        ),
        PIPELINE_CONSUMERS_ACTIVE: meter.create_up_down_counter(
            PIPELINE_CONSUMERS_ACTIVE,
            description="Number of currently active MusicBrainz consumers.",
        ),
        MESSAGING_CONSUMED_MESSAGES: meter.create_counter(
            MESSAGING_CONSUMED_MESSAGES,
            description="Messages consumed from the broker.",
        ),
        MESSAGING_OPERATION_DURATION: meter.create_histogram(
            MESSAGING_OPERATION_DURATION,
            unit="s",
            description="Duration of a messaging client operation.",
        ),
    }


def _instrument(name: str) -> Any:
    """Return one cached instrument, rebuilding the cache when the provider changed."""
    global _instrument_generation

    generation = provider_generation()
    with _instruments_lock:
        if _instrument_generation != generation or not _instruments:
            _instruments.clear()
            _instruments.update(_build_instruments())
            _instrument_generation = generation
        return _instruments[name]


def reset_telemetry_instruments() -> None:
    """Drop the instrument cache. Test seam; production relies on the generation check."""
    global _instrument_generation

    with _instruments_lock:
        _instruments.clear()
        _instrument_generation = -1


def _record_pipeline_message(entity: str, outcome: str) -> None:
    """Count one pipeline message by entity and outcome (processed/skipped/failed)."""
    try:
        _instrument(PIPELINE_MESSAGES).add(1, {"source": PIPELINE_SOURCE, "entity": entity, "outcome": outcome})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_MESSAGES, exc_info=True)


def _record_message_duration(entity: str, duration_s: float) -> None:
    """Record how long handling one pipeline message took."""
    try:
        _instrument(PIPELINE_MESSAGE_DURATION).record(duration_s, {"source": PIPELINE_SOURCE, "entity": entity})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_MESSAGE_DURATION, exc_info=True)


def _record_batch_flush(entity: str, size: int, duration_s: float, outcome: str) -> None:
    """Record one Neo4j write flush's size and duration."""
    try:
        _instrument(PIPELINE_BATCH_SIZE).record(size, {"store": PIPELINE_STORE, "entity": entity})
        _instrument(PIPELINE_BATCH_FLUSH_DURATION).record(duration_s, {"store": PIPELINE_STORE, "entity": entity, "outcome": outcome})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_BATCH_FLUSH_DURATION, exc_info=True)


def _record_consumer_delta(delta: int) -> None:
    """Adjust the active-consumer gauge by delta (+1 on start, -1 on stop)."""
    try:
        _instrument(PIPELINE_CONSUMERS_ACTIVE).add(delta, {"source": PIPELINE_SOURCE})
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record %s", PIPELINE_CONSUMERS_ACTIVE, exc_info=True)


def _record_consumed_message(destination: str, duration_s: float, error_type: str | None) -> None:
    """Record one consumed delivery, matching common.runtime_metrics' shared wrapper shape."""
    attributes: dict[str, str] = {
        "messaging.system": "rabbitmq",
        "messaging.destination.name": destination,
        "messaging.operation.name": "process",
    }
    if error_type is not None:
        attributes["error.type"] = error_type
    try:
        _instrument(MESSAGING_CONSUMED_MESSAGES).add(1, attributes)
        _instrument(MESSAGING_OPERATION_DURATION).record(duration_s, attributes)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not record consumed-message metrics", exc_info=True)


# ── Tracing ──────────────────────────────────────────────────────────────
#
# Spans follow the same GrooveMap OpenTelemetry conventions as the metrics above, and are the
# same no-ops when the 'otel' extra is absent or no collector endpoint is configured.
#
# common.process_message_with_retry opens the CONSUMER span for services that route their
# handler through it. This one registers with aio-pika's queue.consume() directly (see the
# messaging metrics note above), so the identical span is opened here from the library's own
# helpers -- same name, kind, attributes, and extracted parent context -- rather than
# reimplementing the wrapper's ack/nack policy, which this service owns.
MESSAGING_SYSTEM = "rabbitmq"

# A batch flush links the message spans it covers, capped so a large batch cannot carry one
# link per row into the collector. common.tracing enforces the same bound; this service flushes
# one delivery at a time, so the cap is never reached in practice.
MAX_FLUSH_LINKS = 64


def _span_kind(name: str) -> Any:
    """Return a SpanKind member, or None when the OpenTelemetry API is not installed."""
    try:
        from opentelemetry.trace import SpanKind  # noqa: PLC0415 - optional, only with the 'otel' extra
    except ImportError:
        return None
    return getattr(SpanKind, name)


def _mark_span_failed(span: Any, error_type: str) -> None:
    """Fail a span with `error.type` only -- never a message, a stack trace, or a payload."""
    if span is None:
        return
    try:
        from opentelemetry.trace import Status, StatusCode  # noqa: PLC0415 - optional, only with the 'otel' extra

        span.set_attribute("error.type", error_type)
        span.set_status(Status(StatusCode.ERROR))
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not mark a span as failed", exc_info=True)


def _set_span_attribute(span: Any, key: str, value: Any) -> None:
    """Set one closed-set attribute on a span, ignoring a no-op or absent span."""
    if span is None:
        return
    try:
        span.set_attribute(key, value)
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not set the %s span attribute", key, exc_info=True)


def flush_links(*spans: Any) -> list[Any]:
    """Return the span contexts of a flush's member message spans, at most MAX_FLUSH_LINKS."""
    contexts = []
    for span in spans:
        if span is None:
            continue
        try:
            context = span.get_span_context()
        except Exception:  # pragma: no cover - defensive
            logger.debug("Could not read a member span context for a flush span", exc_info=True)
            continue
        if context is not None:
            contexts.append(context)
    return contexts[:MAX_FLUSH_LINKS]


@contextlib.contextmanager
def consume_span(destination: str, headers: Any) -> Iterator[Any]:
    """Open the CONSUMER span `process {destination}` for one delivery.

    The span is a child of the trace context carried in the AMQP headers, which is what puts
    the MusicBrainz extractor's publish and this service's enrichment in one trace. Headers
    carrying no readable context simply start a new trace rather than failing the delivery.

    Exception recording and automatic status are both off: the conventions allow a status and
    an `error.type`, not a stack trace attached as a span event. This handler settles every
    failure itself, so the caller marks the span through `_mark_span_failed`.
    """
    try:
        manager = get_tracer(INSTRUMENTATION_SCOPE).start_as_current_span(
            f"process {destination}",
            context=extract_context(headers) if headers else None,
            kind=_span_kind("CONSUMER"),
            attributes={
                "messaging.system": MESSAGING_SYSTEM,
                "messaging.destination.name": destination,
                "messaging.operation.name": "process",
            },
            record_exception=False,
            set_status_on_exception=False,
        )
    except Exception:  # pragma: no cover - defensive
        logger.debug("Could not start the consumer span", exc_info=True)
        yield None
        return

    with manager as span:
        yield span


def get_health_data() -> dict[str, Any]:
    """Get current health data for monitoring."""
    active_task = None
    current_time = time.time()

    # Check for recent message activity (within last 10 seconds)
    for data_type, last_time in last_message_time.items():
        if last_time > 0 and (current_time - last_time) < 10:
            active_task = f"Enriching {data_type}"
            break

    # If no recent activity but consumers exist, show as idle
    if active_task is None and len(consumer_tags) > 0:
        active_task = "Idle - waiting for messages"

    # Check for stuck state
    no_active_consumers = len(consumer_tags) == 0
    files_incomplete = len(completed_files) < len(MUSICBRAINZ_DATA_TYPES)
    has_processed_messages = any(count > 0 for count in message_counts.values())
    is_stuck = no_active_consumers and files_incomplete and has_processed_messages

    if is_stuck:
        active_task = "STUCK - consumers died, awaiting recovery"

    if graph is None:
        if len(consumer_tags) == 0 and all(c == 0 for c in message_counts.values()):
            status = "starting"
            active_task = "Initializing Neo4j connection"
        else:
            status = "unhealthy"
    elif is_stuck:
        status = "unhealthy"
    else:
        status = "healthy"

    return {
        "status": status,
        "service": SERVICE_NAME,
        "current_task": active_task,
        "message_counts": message_counts.copy(),
        "last_message_time": last_message_time.copy(),
        "active_consumers": list(consumer_tags.keys()),
        "completed_files": list(completed_files),
        "enrichment_stats": _get_enrichment_stats_snapshot(),
        "timestamp": datetime.now(UTC).isoformat(),
    }


def _get_enrichment_stats_snapshot() -> dict[str, int]:
    """Thread-safe snapshot of enrichment stats for health server access."""
    with _stats_thread_lock:
        return enrichment_stats.copy()


def signal_handler(signum: int, _frame: Any) -> None:
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info("🛑 Received signal, initiating graceful shutdown...", signum=signum)
    shutdown_requested = True


async def schedule_consumer_cancellation(data_type: str, queue: Any) -> None:
    """Schedule cancellation of a consumer after a delay."""

    async def cancel_after_delay() -> None:
        try:
            await asyncio.sleep(CONSUMER_CANCEL_DELAY)

            if data_type in consumer_tags:
                consumer_tag = consumer_tags[data_type]
                logger.info(f"🔧 Canceling consumer for {data_type} after {CONSUMER_CANCEL_DELAY}s grace period")
                await queue.cancel(consumer_tag, nowait=True)
                del consumer_tags[data_type]
                _record_consumer_delta(-1)

                logger.info(
                    "✅ Consumer successfully canceled",
                    data_type=data_type,
                )

                if await check_all_consumers_idle():
                    logger.info("🔧 All consumers idle, closing RabbitMQ connection")
                    await close_rabbitmq_connection()
        except Exception as e:
            logger.error(
                "❌ Failed to cancel consumer",
                data_type=data_type,
                error=str(e),
            )
        finally:
            consumer_cancel_tasks.pop(data_type, None)

    # Cancel any existing scheduled cancellation
    if data_type in consumer_cancel_tasks:
        consumer_cancel_tasks[data_type].cancel()

    consumer_cancel_tasks[data_type] = asyncio.create_task(cancel_after_delay())


async def cancel_all_consumers() -> None:
    """Stop new deliveries at shutdown by cancelling every consumer.

    Shutdown previously had no deregistration phase at all: the flag flipped, the
    consumers stayed subscribed, and the per-message guard nacked whatever the
    broker kept pushing. Cancelling here closes the delivery tap BEFORE the
    seconds-long flush/teardown sequence, so nothing is redelivered into a
    service that is on its way out. Best-effort: teardown continues regardless.
    """
    for data_type, consumer_tag in list(consumer_tags.items()):
        queue = queues.get(data_type)
        if queue is None:
            consumer_tags.pop(data_type, None)
            _record_consumer_delta(-1)
            continue
        try:
            await queue.cancel(consumer_tag, nowait=True)
            consumer_tags.pop(data_type, None)
            _record_consumer_delta(-1)
        except Exception as e:
            logger.warning(
                "⚠️ Failed to cancel consumer during shutdown",
                data_type=data_type,
                error=str(e),
            )
    logger.info("✅ Consumers cancelled for shutdown")


async def close_rabbitmq_connection() -> None:
    """Close the RabbitMQ connection and channel when all consumers are idle."""
    global active_connection, active_channel

    try:
        if active_channel:
            try:
                await active_channel.close()
                logger.info("🔧 Closed RabbitMQ channel - all consumers idle")
            except Exception as e:
                logger.warning("⚠️ Error closing channel", error=str(e))
            active_channel = None

        if active_connection:
            try:
                await active_connection.close()
                logger.info("🔧 Closed RabbitMQ connection - all consumers idle")
            except Exception as e:
                logger.warning("⚠️ Error closing connection", error=str(e))
            active_connection = None

        logger.info("✅ RabbitMQ connection closed", check_interval=f"{QUEUE_CHECK_INTERVAL}s")
    except Exception as e:
        logger.error("❌ Error closing RabbitMQ connection", error=str(e))


async def check_all_consumers_idle() -> bool:
    """Check if all consumers are cancelled (idle) AND all files completed."""
    return len(consumer_tags) == 0 and len(MUSICBRAINZ_DATA_TYPES) == len(completed_files)


async def check_file_completion(data: dict[str, Any], data_type: str, message: AbstractIncomingMessage) -> bool:
    """Check if message is a file completion or extraction completion message."""
    if data.get("type") == "file_complete":
        total_processed = data.get("total_processed", 0)
        logger.info(f"✅ File processing complete for {data_type}! Total records processed: {total_processed}")

        if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
            await schedule_consumer_cancellation(data_type, queues[data_type])

        # Mark as completed AFTER scheduling cancellation so the stuck-state
        # checker still fires for any in-flight messages during the delay.
        completed_files.add(data_type)

        await message.ack()
        return True

    if data.get("type") == "extraction_complete":
        logger.info(
            "🏁 Received extraction_complete signal",
            data_type=data_type,
            version=data.get("version"),
        )

        # extraction_complete is this type's terminal signal, so it must also
        # (re-)mark the type complete. completed_files is otherwise written only by
        # file_complete and ERASED by _recover_consumers for any type whose queue
        # still holds messages — and when the only pending message IS this signal,
        # nothing ever restored the flag: the stall check then logged at ERROR
        # every 30s forever and check_all_consumers_idle() could never return True,
        # so the connection and idle consumers were held open until restart. A
        # plain restart between the file_complete ack and this delivery must reach
        # the same terminal state.
        completed_files.add(data_type)
        if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
            await schedule_consumer_cancellation(data_type, queues[data_type])

        await message.ack()
        return True

    return False


async def enrich_artist(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Artist node with MusicBrainz metadata.

    If discogs_artist_id is None, skip — entity has no Discogs match.
    """
    s = stats if stats is not None else enrichment_stats
    discogs_id = record.get("discogs_artist_id")
    if discogs_id is None:
        s["entities_skipped_no_discogs_match"] += 1
        return True  # Deliberately skipped, not an error

    # Discogs nodes store `id` as a String property (graphinator writes it
    # from DataMessage.id, always a str); MusicBrainz emits discogs_*_id as a
    # JSON integer. Coerce to the graph's string convention before matching,
    # or the MATCH silently never finds the node.
    discogs_id = str(discogs_id)

    result = await tx.run(
        "MATCH (a:Artist {id: $discogs_id}) "
        "SET a.mbid = $mbid, "
        "    a.mb_type = $mb_type, "
        "    a.mb_gender = $mb_gender, "
        "    a.mb_begin_date = $mb_begin_date, "
        "    a.mb_end_date = $mb_end_date, "
        "    a.mb_area = $mb_area, "
        "    a.mb_begin_area = $mb_begin_area, "
        "    a.mb_end_area = $mb_end_area, "
        "    a.mb_disambiguation = $mb_disambiguation, "
        "    a.mb_updated_at = $mb_updated_at "
        "RETURN a.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_type=record.get("mb_type", record.get("type")),
        mb_gender=record.get("gender"),
        mb_begin_date=record.get("begin_date", (record.get("life_span") or {}).get("begin")),
        mb_end_date=record.get("end_date", (record.get("life_span") or {}).get("end")),
        mb_area=record.get("area"),
        mb_begin_area=record.get("begin_area"),
        mb_end_area=record.get("end_area"),
        mb_disambiguation=record.get("disambiguation"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    if matched:
        s["entities_enriched"] += 1
    else:
        s["entities_skipped_no_discogs_match"] += 1

    # Create relationship edges if relations are present
    relations = record.get("relations", [])
    if relations and matched:
        await create_relationship_edges(tx, discogs_id, relations, stats=s)

    return True


async def enrich_label(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Label node with MusicBrainz metadata.

    If discogs_label_id is None, skip — entity has no Discogs match.
    """
    s = stats if stats is not None else enrichment_stats
    discogs_id = record.get("discogs_label_id")
    if discogs_id is None:
        s["entities_skipped_no_discogs_match"] += 1
        return True

    # See enrich_artist: coerce to the graph's string `id` convention.
    discogs_id = str(discogs_id)

    result = await tx.run(
        "MATCH (l:Label {id: $discogs_id}) "
        "SET l.mbid = $mbid, "
        "    l.mb_type = $mb_type, "
        "    l.mb_label_code = $mb_label_code, "
        "    l.mb_begin_date = $mb_begin_date, "
        "    l.mb_end_date = $mb_end_date, "
        "    l.mb_area = $mb_area, "
        "    l.mb_updated_at = $mb_updated_at "
        "RETURN l.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_type=record.get("mb_type", record.get("type")),
        mb_label_code=record.get("label_code"),
        mb_begin_date=record.get("begin_date", (record.get("life_span") or {}).get("begin")),
        mb_end_date=record.get("end_date", (record.get("life_span") or {}).get("end")),
        mb_area=record.get("area"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    if matched:
        s["entities_enriched"] += 1
    else:
        s["entities_skipped_no_discogs_match"] += 1

    return True


def release_media_block(record: dict[str, Any]) -> dict[str, Any] | None:
    """Return the canonical media block for a release event, or None when it has none.

    ADR 0007 has the MusicBrainz producer compute the block in its parser, so a current
    event carries `media` ready to use. An event published before that field existed carries
    only the raw medium list in `media_raw`; for those the block is derived here through the
    shared runtime mapper, the same one the producer runs, so a replayed backlog lands the
    same media as a fresh event.

    An event with neither field yields None, and the caller leaves the release's media
    properties and edges exactly as it found them — absence of the field is not evidence that
    the release has no media.
    """
    media = record.get("media")
    if isinstance(media, dict) and isinstance(media.get("items"), list):
        return media

    media_raw = record.get("media_raw")
    if isinstance(media_raw, list):
        # map_musicbrainz_release reads `media`, `status`, `packaging` and `release_group`
        # off one mapping; the event carries the raw list under a different key, so hand the
        # mapper the record with that key restored to the shape it expects.
        return map_musicbrainz_release({**record, "media": media_raw})

    return None


def media_edge_rows(media: dict[str, Any]) -> list[dict[str, Any]]:
    """Collapse a media block's items into one row per canonical medium.

    A MusicBrainz release lists one entry per physical medium, each with qty 1, so a 2xLP
    arrives as two `vinyl_unspecified` items. The graph holds at most one musicbrainz-sourced
    ISSUED_ON edge per (release, medium) pair, so items sharing a medium are summed into one
    row and the total lands on that edge's `qty`. Rows keep the order the mediums first appear
    in, which makes the emitted parameters deterministic for a given event.

    Items missing a medium or family id are dropped rather than raising: an item that cannot
    name its medium cannot become an edge.
    """
    rows: dict[str, dict[str, Any]] = {}
    for item in media.get("items") or []:
        if not isinstance(item, dict):
            continue
        medium_id = item.get("medium")
        family = item.get("family")
        if not isinstance(medium_id, str) or not isinstance(family, str):
            continue
        quantity = item.get("qty")
        quantity = quantity if isinstance(quantity, int) and not isinstance(quantity, bool) and quantity >= 1 else 1
        row = rows.get(medium_id)
        if row is None:
            rows[medium_id] = {
                "medium": medium_id,
                "family": family,
                "label": _medium_label(medium_id),
                "qty": quantity,
            }
        else:
            row["qty"] += quantity
    return list(rows.values())


def _medium_label(medium_id: str) -> str:
    """Return a medium's vocabulary label, falling back to its id.

    A medium id the vendored vocabulary does not know means this service is older than the
    event that reached it. Naming the node after its id keeps the edge — and the provenance
    of an unrecognized medium — rather than failing the delivery over a display string.
    """
    try:
        return medium_label(medium_id)
    except KeyError:
        return medium_id


async def reconcile_release_media(tx: Any, discogs_id: str, record: dict[str, Any]) -> bool:
    """Reconcile one matched release's MusicBrainz media onto the graph.

    Writes the `mb_media_families` and `mb_medium_count` summary, merges a Medium per
    canonical medium id under its MediaFamily, and attaches each to the release with an
    ISSUED_ON edge tagged source: 'musicbrainz'. Edges this source wrote for media the
    release no longer has are deleted, so a corrected upstream medium list does not leave a
    contradiction behind. Discogs-sourced edges are never read, written, or deleted.

    Returns:
        True when the event carried media to reconcile, False when it carried none and the
        release's media was left untouched.
    """
    media = release_media_block(record)
    if media is None:
        return False

    rows = media_edge_rows(media)
    items = [item for item in (media.get("items") or []) if isinstance(item, dict)]

    await tx.run(
        RELEASE_MEDIA_SUMMARY_CYPHER,
        discogs_id=discogs_id,
        families=families_of(media),
        # The count of source mediums, not of distinct media: a 2xLP is two mediums behind
        # one edge of qty 2.
        medium_count=len(items),
        medium_ids=[row["medium"] for row in rows],
        source=MEDIA_SOURCE,
    )

    if rows:
        await tx.run(
            RELEASE_MEDIA_EDGES_CYPHER,
            discogs_id=discogs_id,
            items=rows,
            source=MEDIA_SOURCE,
        )

    return True


async def enrich_release(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Release node with MusicBrainz metadata.

    If discogs_release_id is None, skip — entity has no Discogs match.

    A release that matches also has its canonical media (ADR 0007) reconciled onto the graph;
    see reconcile_release_media. An unmatched release is counted and skipped as before, and
    creates no Medium, MediaFamily, or ISSUED_ON node or edge.
    """
    s = stats if stats is not None else enrichment_stats
    discogs_id = record.get("discogs_release_id")
    if discogs_id is None:
        s["entities_skipped_no_discogs_match"] += 1
        return True

    # See enrich_artist: coerce to the graph's string `id` convention.
    discogs_id = str(discogs_id)

    result = await tx.run(
        "MATCH (r:Release {id: $discogs_id}) "
        "SET r.mbid = $mbid, "
        "    r.mb_barcode = $mb_barcode, "
        "    r.mb_status = $mb_status, "
        "    r.mb_release_group_mbid = $release_group_mbid, "
        "    r.mb_updated_at = $mb_updated_at "
        "RETURN r.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_barcode=record.get("barcode"),
        mb_status=record.get("status"),
        release_group_mbid=record.get("release_group_mbid"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    if matched:
        s["entities_enriched"] += 1
        await reconcile_release_media(tx, discogs_id, record)
    else:
        s["entities_skipped_no_discogs_match"] += 1

    return True


async def create_relationship_edges(
    tx: Any,
    source_discogs_id: str,
    relations: list[dict[str, Any]],
    stats: dict[str, int] | None = None,
) -> None:
    """Create MusicBrainz relationship edges between Artist nodes.

    For each relation:
    - Look up the Neo4j edge type in MB_RELATIONSHIP_MAP
    - Skip unknown relationship types
    - If target_discogs_artist_id is None, skip
    - Swap source/target when the relation's direction is "backward" — the
      current entity is the relationship's target in that case, not its
      source (see MB_RELATIONSHIP_MAP types, all of which are canonically
      oriented, e.g. member->band for "member of band")
    - MERGE the edge with source: 'musicbrainz' property

    Cypher can't parameterize relationship types, so we format the edge type
    into the query string. This is safe because values come from our map, not
    user input.
    """
    s = stats if stats is not None else enrichment_stats
    for relation in relations:
        mb_type = relation.get("type", "")
        edge_type = MB_RELATIONSHIP_MAP.get(mb_type)
        if edge_type is None:
            continue  # Unknown relationship type, skip

        target_discogs_id = relation.get("target_discogs_artist_id")
        if target_discogs_id is None:
            s["relationships_skipped_missing_side"] += 1
            continue

        # Discogs nodes key on a String `id`; MB targets resolve to an int
        # discogs id (see enrich_artist). Coerce both sides consistently.
        edge_source_id = str(source_discogs_id)
        edge_target_id = str(target_discogs_id)

        # MusicBrainz relations are directional and materialized on both
        # endpoints. direction == "backward" means the record being
        # processed is the relation's TARGET, not its source — swap so the
        # edge is created in the canonical orientation (e.g. member->band).
        if relation.get("direction") == "backward":
            edge_source_id, edge_target_id = edge_target_id, edge_source_id

        # Safe: edge_type comes from MB_RELATIONSHIP_MAP, not user input
        result = await tx.run(
            f"MATCH (a:Artist {{id: $source_id}}) MATCH (b:Artist {{id: $target_id}}) MERGE (a)-[r:{edge_type}]->(b) SET r.source = 'musicbrainz'",
            source_id=edge_source_id,
            target_id=edge_target_id,
        )
        summary = await result.consume()
        if summary.counters.relationships_created > 0:
            s["relationships_created"] += 1
        elif summary.counters.contains_updates:
            # MERGE matched an existing relationship and SET updated it
            s["relationships_updated"] = s.get("relationships_updated", 0) + 1
        else:
            # One or both nodes missing — no rows produced
            s["relationships_skipped_missing_side"] += 1


async def enrich_release_group(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Master node with MusicBrainz release-group metadata.

    If discogs_master_id is None, skip — entity has no Discogs match.
    """
    s = stats if stats is not None else enrichment_stats
    discogs_id = record.get("discogs_master_id")
    if discogs_id is None:
        s["entities_skipped_no_discogs_match"] += 1
        return True

    # See enrich_artist: coerce to the graph's string `id` convention.
    discogs_id = str(discogs_id)

    result = await tx.run(
        "MATCH (m:Master {id: $discogs_id}) "
        "SET m.mbid = $mbid, "
        "    m.mb_type = $mb_type, "
        "    m.mb_secondary_types = $mb_secondary_types, "
        "    m.mb_first_release_date = $mb_first_release_date, "
        "    m.mb_disambiguation = $mb_disambiguation, "
        "    m.mb_updated_at = $mb_updated_at "
        "RETURN m.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_type=record.get("mb_type", record.get("type")),
        mb_secondary_types=record.get("secondary_types", []),
        mb_first_release_date=record.get("first_release_date"),
        mb_disambiguation=record.get("disambiguation"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    if matched:
        s["entities_enriched"] += 1
    else:
        s["entities_skipped_no_discogs_match"] += 1

    return True


# Processor lookup by data type
PROCESSORS: dict[str, Any] = {
    "artists": enrich_artist,
    "labels": enrich_label,
    "release-groups": enrich_release_group,
    "releases": enrich_release,
}


def make_message_handler(data_type: str, enrich_fn: Any) -> Any:
    """Create a RabbitMQ message handler for the given data type."""

    destination = catalog_queue_name(WIRE_CONSUMER_NAME, data_type)

    async def handler(message: AbstractIncomingMessage) -> None:
        if shutdown_requested:
            # Leave the delivery UNACKED — never nack(requeue=True) here. The
            # consumer is still subscribed at this point, so a requeue is
            # redelivered within milliseconds and nacked again, burning a quorum
            # x-delivery-count per cycle; at x-delivery-limit=20 valid records
            # are dead-lettered within a second of a routine restart. Returning
            # without settling lets the connection close requeue them exactly once.
            logger.debug("🛑 Shutdown requested, leaving message unacked for redelivery")
            return

        # message.duration / messaging.client.* cover every settled delivery below,
        # control messages included; groovemap.pipeline.messages (set per-branch) is
        # domain-scoped to actual enrichment attempts.
        started = time.perf_counter()
        messaging_error_type: str | None = None

        # Every delivery this service actually processes runs inside the CONSUMER span,
        # joined to the extractor's trace through the message's traceparent header.
        with consume_span(destination, getattr(message, "headers", None)) as consumer_span:
            try:
                logger.debug("🔄 Received MusicBrainz message", data_type=data_type)
                body: dict[str, Any] = loads(message.body)

                if await check_file_completion(body, data_type, message):
                    return

                # Validate required 'id' field — nack with requeue=False to avoid
                # infinite requeue loop for malformed messages (matches brainztableinator).
                if "id" not in body:
                    logger.error("❌ Message missing 'id' field", data_type=data_type)
                    await message.nack(requeue=False)
                    messaging_error_type = "ValidationError"
                    _record_pipeline_message(data_type, "failed")
                    return

                data_id: str = body["id"]
                if not data_id:
                    logger.warning("⚠️ Nacking record with empty mbid/id", data_type=data_type)
                    await message.nack(requeue=False)
                    messaging_error_type = "ValidationError"
                    _record_pipeline_message(data_type, "failed")
                    return

                if graph is None:
                    raise RuntimeError("Neo4j driver not initialized")

                # Use local counters inside the transaction to avoid race conditions
                # with concurrent messages mutating the global enrichment_stats dict.
                # We pass local_stats to the enrich function so it writes to a
                # per-message dict instead of the shared global. This avoids the
                # race condition of swapping/restoring the global reference under
                # concurrent message delivery (prefetch=200).
                local_stats: dict[str, int] = {
                    "entities_enriched": 0,
                    "entities_skipped_no_discogs_match": 0,
                    "relationships_created": 0,
                    "relationships_updated": 0,
                    "relationships_skipped_missing_side": 0,
                }

                # `flush neo4j {entity}` opens OUTSIDE graph.session(...) so the driver wrapper's
                # own `session neo4j` CLIENT span nests inside it rather than around it. The flush
                # *metric* window is unchanged and still measures only the write itself.
                #
                # One delivery is one flush, so the batch has exactly one member span: this
                # delivery's CONSUMER span. A batching consumer would pass one context per member
                # here, and both flush_links and common.tracing cap the list at 64.
                with flush_span(PIPELINE_STORE, data_type, links=flush_links(consumer_span)) as batch_span:
                    async with graph.session(database="neo4j") as session:

                        async def tx_fn(tx: Any) -> bool:
                            # Reset local counters on each retry attempt
                            for key in local_stats:
                                local_stats[key] = 0
                            return bool(await enrich_fn(tx, body, local_stats))

                        # Each message writes exactly one record, so this is a flush of batch size 1 —
                        # still reported through the shared batch metrics so dashboards built against
                        # batching services (graphinator et al.) read this service the same way.
                        flush_started = time.perf_counter()
                        try:
                            await session.execute_write(tx_fn)
                        except Exception:
                            _set_span_attribute(batch_span, "outcome", "failed")
                            _record_batch_flush(data_type, 1, time.perf_counter() - flush_started, "failed")
                            raise
                        _set_span_attribute(batch_span, "outcome", "committed")
                        _record_batch_flush(data_type, 1, time.perf_counter() - flush_started, "committed")

                # Merge local counters into global stats under lock to prevent
                # concurrent handlers from corrupting the shared dict
                global _stats_lock
                if _stats_lock is None:
                    _stats_lock = asyncio.Lock()
                async with _stats_lock:
                    with _stats_thread_lock:
                        for key, value in local_stats.items():
                            enrichment_stats[key] += value

                await message.ack()

                # Neo4j answered — clear the outage backoff.
                outage_backoff.reset()

                # Increment counts only after successful processing and ack
                message_counts[data_type] += 1
                last_message_time[data_type] = time.time()
                if message_counts[data_type] % progress_interval == 0:
                    logger.info(
                        f"📊 Enriched {data_type} in Neo4j",
                        message_counts=message_counts[data_type],
                    )

                # entities_enriched vs. entities_skipped_no_discogs_match is exactly the
                # per-message enrich_fn outcome (see enrich_artist/label/release/release_group):
                # a record with no Discogs match is deliberately skipped, not an error.
                outcome = "processed" if local_stats["entities_enriched"] > 0 else "skipped"
                _record_pipeline_message(data_type, outcome)
            except (ServiceUnavailable, SessionExpired, DatabaseUnavailableError) as e:
                logger.warning(
                    f"⚠️ Neo4j unavailable, will retry {data_type} message",
                    error=str(e),
                )
                # Pause before requeueing. The main queues are quorum queues with
                # x-delivery-limit=20, a budget with no time dimension: requeueing
                # immediately burns all 20 redeliveries in ~3 minutes and RabbitMQ
                # dead-letters a perfectly valid record mid-outage. Prefetch here is
                # 200, so an unthrottled Neo4j outage puts 200 messages on that
                # treadmill at once.
                await outage_backoff.wait()
                try:
                    await message.nack(requeue=True)
                except Exception as nack_error:
                    logger.warning("⚠️ Failed to nack message", error=str(nack_error))
                messaging_error_type = type(e).__name__
                _record_pipeline_message(data_type, "failed")
            except Exception as e:
                logger.error(
                    f"❌ Failed to process {data_type} MusicBrainz message",
                    error=str(e),
                )
                try:
                    await message.nack(requeue=True)
                except Exception as nack_error:
                    logger.warning("⚠️ Failed to nack message", error=str(nack_error))
                messaging_error_type = type(e).__name__
                _record_pipeline_message(data_type, "failed")
            finally:
                duration_s = time.perf_counter() - started
                _record_message_duration(data_type, duration_s)
                _record_consumed_message(destination, duration_s, messaging_error_type)
                # The handler settles every failure itself, so nothing propagates out of
                # the span for it to notice: mark it here instead.
                if messaging_error_type is not None:
                    _mark_span_failed(consumer_span, messaging_error_type)

    return handler


on_artist_message = make_message_handler("artists", enrich_artist)
on_label_message = make_message_handler("labels", enrich_label)
on_release_group_message = make_message_handler("release-groups", enrich_release_group)
on_release_message = make_message_handler("releases", enrich_release)

# Handler lookup by data type for consumer registration
HANDLERS: dict[str, Any] = {
    "artists": on_artist_message,
    "labels": on_label_message,
    "release-groups": on_release_group_message,
    "releases": on_release_message,
}


async def progress_reporter() -> None:
    """Periodically report processing progress and manage idle mode."""
    global idle_mode

    report_count = 0
    startup_time = time.time()
    last_idle_log = 0.0

    while not shutdown_requested:
        if report_count < 3:
            await asyncio.sleep(10)
        else:
            await asyncio.sleep(30)
        report_count += 1

        if len(completed_files) == len(MUSICBRAINZ_DATA_TYPES):
            continue

        total = sum(message_counts.values())
        current_time = time.time()

        if not idle_mode and total == 0 and (current_time - startup_time) >= STARTUP_IDLE_TIMEOUT:
            idle_mode = True
            last_idle_log = current_time
            logger.info(
                "⏳ No MusicBrainz messages received — entering idle mode",
                startup_idle_timeout=STARTUP_IDLE_TIMEOUT,
            )

        if idle_mode:
            if total > 0:
                idle_mode = False
                logger.info("🔄 Messages detected, resuming normal operation")
            elif (current_time - last_idle_log) >= IDLE_LOG_INTERVAL:
                last_idle_log = current_time
                logger.info(
                    "⏳ Still idle, waiting for MusicBrainz messages",
                    active_consumers=list(consumer_tags.keys()),
                    enrichment_stats=_get_enrichment_stats_snapshot(),
                )
            continue

        if total > 0:
            logger.info(
                "📊 MusicBrainz enrichment progress",
                message_counts=message_counts.copy(),
                enrichment_stats=_get_enrichment_stats_snapshot(),
                active_consumers=list(consumer_tags.keys()),
                completed_files=list(completed_files),
            )


async def periodic_queue_checker() -> None:
    """Periodically check queue health and recover from stuck states."""

    last_full_check = 0.0

    while not shutdown_requested:
        try:
            await asyncio.sleep(STUCK_CHECK_INTERVAL)

            current_time = time.time()

            # Check for stuck state
            no_active_consumers = len(consumer_tags) == 0
            files_incomplete = len(completed_files) < len(MUSICBRAINZ_DATA_TYPES)
            has_processed_messages = any(count > 0 for count in message_counts.values())

            if no_active_consumers and files_incomplete and has_processed_messages:
                logger.warning(
                    "⚠️ Detected stuck state: consumers died but files not completed. Attempting recovery...",
                    active_consumers=len(consumer_tags),
                    completed_files=list(completed_files),
                    message_counts=message_counts,
                )
                await _recover_consumers()
                continue

            # Normal idle check
            time_since_last_check = current_time - last_full_check
            if time_since_last_check < QUEUE_CHECK_INTERVAL:
                continue

            if active_connection or len(consumer_tags) > 0:
                continue

            last_full_check = current_time
            logger.info("🔄 Checking all queues for new messages...")
            await _recover_consumers()

        except asyncio.CancelledError:
            logger.info("🛑 Queue checker task cancelled")
            break
        except Exception as e:
            logger.error("❌ Error in periodic queue checker", error=str(e))


async def _recover_consumers() -> None:
    """Recover consumers by reconnecting to RabbitMQ and restarting consumption."""
    global active_connection, active_channel, queues, idle_mode

    if active_connection:
        try:
            await active_connection.close()
        except Exception as e:
            logger.warning("⚠️ Error closing broken connection during recovery", error=str(e))
        active_connection = None
        active_channel = None

    try:
        temp_connection = await rabbitmq_manager.connect()
        temp_channel = await temp_connection.channel()
    except Exception as e:
        logger.error("❌ Failed to connect to RabbitMQ for recovery", error=str(e))
        return

    try:
        queues_with_messages = []
        for data_type in MUSICBRAINZ_DATA_TYPES:
            queue_name = catalog_queue_name(WIRE_CONSUMER_NAME, data_type)

            declared_queue = await temp_channel.declare_queue(name=queue_name, passive=True)

            if declared_queue.declaration_result.message_count > 0:
                queues_with_messages.append((data_type, declared_queue.declaration_result.message_count))

        if queues_with_messages:
            total_messages = sum(count for _, count in queues_with_messages)
            logger.info(f"📬 Found messages in queues, restarting consumers: {queues_with_messages} (total: {total_messages})")

            active_connection = temp_connection
            active_channel = temp_channel

            await active_channel.set_qos(prefetch_count=200)

            queues = {}
            for data_type in MUSICBRAINZ_DATA_TYPES:
                exchange_name = catalog_exchange_name(data_type)
                queue_name = catalog_queue_name(WIRE_CONSUMER_NAME, data_type)
                dlx_name = catalog_dead_letter_exchange_name(WIRE_CONSUMER_NAME, data_type)
                dlq_name = catalog_dead_letter_queue_name(WIRE_CONSUMER_NAME, data_type)

                exchange = await active_channel.declare_exchange(
                    exchange_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                dlx_exchange = await active_channel.declare_exchange(
                    dlx_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                dlq = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=dlq_name,
                    arguments={"x-queue-type": "classic"},
                )
                await dlq.bind(dlx_exchange)

                queue_args = {
                    "x-queue-type": "quorum",
                    "x-dead-letter-exchange": dlx_name,
                    "x-delivery-limit": 20,
                }
                queue = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=queue_name,
                    arguments=queue_args,
                )
                await queue.bind(exchange)
                queues[data_type] = queue

            # Start consumers for ALL data types lacking one — not just those
            # with a current backlog. A type whose queue was empty at the
            # passive-declare instant still needs a consumer; otherwise messages
            # that arrive later are never consumed, because once active_connection
            # is set and consumer_tags is non-empty both periodic recovery routes
            # are permanently gated off, silently starving that data type.
            pending_counts = dict(queues_with_messages)
            for data_type in MUSICBRAINZ_DATA_TYPES:
                if data_type in queues and data_type not in consumer_tags:
                    handler = HANDLERS.get(data_type)
                    if handler:
                        consumer_tag = await queues[data_type].consume(handler, consumer_tag=f"{SERVICE_NAME}-{data_type}")
                        consumer_tags[data_type] = consumer_tag
                        _record_consumer_delta(1)
                        # Only un-complete a type that actually has a backlog, so
                        # genuinely-finished types stay marked complete.
                        if data_type in pending_counts:
                            completed_files.discard(data_type)
                        last_message_time[data_type] = time.time()
                        logger.info(f"✅ Started consumer for {data_type} (pending: {pending_counts.get(data_type, 0)})")

            logger.info(f"✅ Recovery complete - consumers restarted: {list(consumer_tags.keys())}")
            idle_mode = False
        else:
            logger.info("⏳ No messages in any queue, connection remains closed")
            await temp_channel.close()
            await temp_connection.close()

    except Exception as e:
        logger.error("❌ Error during consumer recovery", error=str(e))
        try:
            await temp_channel.close()
            await temp_connection.close()
        except Exception as close_error:
            logger.warning(
                "⚠️ Error closing temporary connection after recovery failure",
                error=str(close_error),
            )
        active_connection = None
        active_channel = None
        queues = {}
        # Clear stale consumer tags: any consumers registered before the error
        # died with the now-closed connection. Leaving them behind would keep
        # len(consumer_tags) > 0 forever, permanently gating off both recovery
        # routes (stuck-check requires 0 tags) while health still reads healthy.
        if consumer_tags:
            _record_consumer_delta(-len(consumer_tags))
        consumer_tags.clear()


async def main() -> None:
    global config, graph, queues, rabbitmq_manager, active_connection, active_channel, connection_check_task

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_logging(SERVICE_NAME, log_file=Path(f"/logs/{SERVICE_NAME}.log"))
    setup_telemetry("brainzgraphinator")
    # Sampled from the consumer's own running loop, which is the loop every delivery is
    # handled on. Returns None with telemetry off, and shutdown_telemetry() cancels it.
    start_event_loop_monitor()
    logger.info("🚀 Starting GrooveMap musicbrainz-graph-enricher service")

    # Add startup delay for dependent services
    startup_delay = int(os.environ.get("STARTUP_DELAY", "5"))
    if startup_delay > 0:
        logger.info(f"⏳ Waiting {startup_delay} seconds for dependent services to start...")
        await asyncio.sleep(startup_delay)

    # Start health server
    health_server = HealthServer(8011, get_health_data)
    health_server.start_background()
    logger.info("🏥 Health server started on port 8011")

    # Initialize configuration
    try:
        config = BrainzgraphinatorConfig.from_env()
    except ValueError as e:
        logger.error("❌ Configuration error", error=str(e))
        shutdown_telemetry()
        return

    # Initialize async resilient Neo4j driver
    graph = AsyncResilientNeo4jDriver(
        uri=config.neo4j_host,
        auth=(config.neo4j_username, config.neo4j_password),
        max_retries=5,
        **neo4j_security_kwargs(),
    )

    # Test Neo4j connectivity
    try:
        async with graph.session(database="neo4j") as session:
            result = await session.run("RETURN 1 as test")
            await result.single()
            logger.info("✅ Neo4j connectivity verified (async)")
    except Exception as e:
        logger.error("❌ Failed to connect to Neo4j", error=str(e))
        shutdown_telemetry()
        return

    print(STARTUP_BANNER)

    # Initialize resilient RabbitMQ connection manager
    rabbitmq_manager = AsyncResilientRabbitMQ(
        connection_url=config.amqp_connection,
        max_retries=10,
        heartbeat=600,
        connection_attempts=10,
        retry_delay=5.0,
    )

    # Try to connect with retry logic
    max_startup_retries = 5
    startup_retry = 0
    amqp_connection = None

    while startup_retry < max_startup_retries and not shutdown_requested:
        try:
            logger.info(f"🐰 Attempting to connect to RabbitMQ (attempt {startup_retry + 1}/{max_startup_retries})")
            amqp_connection = await rabbitmq_manager.connect()
            active_connection = amqp_connection
            break
        except Exception as e:
            startup_retry += 1
            if startup_retry < max_startup_retries:
                wait_time = min(30, 5 * startup_retry)
                logger.warning(f"⚠️ RabbitMQ connection failed: {e}. Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"❌ Failed to connect to AMQP broker after {max_startup_retries} attempts: {e}")
                shutdown_telemetry()
                return

    if amqp_connection is None:
        logger.error("❌ No AMQP connection available")
        shutdown_telemetry()
        return

    async with amqp_connection:
        channel = await amqp_connection.channel()
        active_channel = channel

        await channel.set_qos(prefetch_count=200)
        logger.info("🔧 QoS prefetch configured", prefetch_count=200)

        # Declare per-data-type fanout exchanges and consumer-owned queues
        queues = {}
        for data_type in MUSICBRAINZ_DATA_TYPES:
            exchange_name = catalog_exchange_name(data_type)
            queue_name = catalog_queue_name(WIRE_CONSUMER_NAME, data_type)
            dlx_name = catalog_dead_letter_exchange_name(WIRE_CONSUMER_NAME, data_type)
            dlq_name = catalog_dead_letter_queue_name(WIRE_CONSUMER_NAME, data_type)

            # Declare fanout exchange
            exchange = await channel.declare_exchange(
                exchange_name,
                AMQP_EXCHANGE_TYPE,
                durable=True,
                auto_delete=False,
            )

            # Declare consumer-owned dead-letter exchange
            dlx_exchange = await channel.declare_exchange(
                dlx_name,
                AMQP_EXCHANGE_TYPE,
                durable=True,
                auto_delete=False,
            )

            # Declare DLQ
            dlq = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=dlq_name,
                arguments={"x-queue-type": "classic"},
            )
            await dlq.bind(dlx_exchange)

            # Declare main quorum queue with consumer-owned DLX
            queue_args = {
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": dlx_name,
                "x-delivery-limit": 20,
            }
            queue = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=queue_name,
                arguments=queue_args,
            )
            await queue.bind(exchange)
            queues[data_type] = queue

        # Start consuming from each queue
        for data_type in MUSICBRAINZ_DATA_TYPES:
            handler = HANDLERS[data_type]
            consumer_tag = await queues[data_type].consume(handler, consumer_tag=f"{SERVICE_NAME}-{data_type}")
            consumer_tags[data_type] = consumer_tag
            _record_consumer_delta(1)
            logger.info(f"✅ Started consuming {data_type} MusicBrainz messages")

        logger.info(
            "🚀 musicbrainz-graph-enricher is ready and consuming MusicBrainz messages",
            data_types=MUSICBRAINZ_DATA_TYPES,
        )

        # Start background tasks
        progress_task = asyncio.create_task(progress_reporter())
        connection_check_task = asyncio.create_task(periodic_queue_checker())

        try:
            while not shutdown_requested:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("🛑 Main loop cancelled")
        finally:
            # Stop new deliveries FIRST, before the multi-second flush/teardown
            # below: a still-subscribed consumer keeps being handed messages it
            # can only leave unacked.
            await cancel_all_consumers()

            progress_task.cancel()
            if connection_check_task:
                connection_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await progress_task
            if connection_check_task:
                with contextlib.suppress(asyncio.CancelledError):
                    await connection_check_task

            if graph is not None:
                await graph.close()

            health_server.stop()
            shutdown_telemetry()

            logger.info(
                "✅ musicbrainz-graph-enricher shutdown complete",
                enrichment_stats=enrichment_stats,
            )


def cli() -> None:
    """Run the async service from a console-script entry point."""
    run(main())


if __name__ == "__main__":
    cli()
