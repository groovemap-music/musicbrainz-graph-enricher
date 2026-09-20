from __future__ import annotations

import asyncio
import contextlib
import contextvars
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
    DeliveryResult,
    FailureKind,
    HealthServer,
    OutageBackoff,
    Settlement,
    extract_context,
    flush_span,
    get_tracer,
    neo4j_security_kwargs,
    run_delivery,
    setup_logging,
    setup_telemetry,
    shutdown_telemetry,
    start_event_loop_monitor,
)
from common.telemetry import get_meter, provider_generation
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from orjson import loads

from brainzgraphinator import _projections
from brainzgraphinator._queue_lifecycle import declare_stream_queue
from brainzgraphinator.catalog_contract import ENTITY_TYPES as MUSICBRAINZ_DATA_TYPES
from brainzgraphinator.config import BrainzgraphinatorConfig
from brainzgraphinator.queue_names import (
    queue_name as catalog_queue_name,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator


logger = structlog.get_logger(__name__)

# Compatibility exports: callers historically import projections from this module.
MB_RELATIONSHIP_MAP = _projections.MB_RELATIONSHIP_MAP
MEDIA_SOURCE = _projections.MEDIA_SOURCE
RELEASE_MEDIA_EDGES_CYPHER = _projections.RELEASE_MEDIA_EDGES_CYPHER
RELEASE_MEDIA_SUMMARY_CYPHER = _projections.RELEASE_MEDIA_SUMMARY_CYPHER
media_edge_rows = _projections.media_edge_rows
reconcile_release_media = _projections.reconcile_release_media
release_event_rows = _projections.release_event_rows
release_media_block = _projections.release_media_block

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
active_connection: Any = None
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
# service registers its handler with aio-pika's queue.consume() directly. The shared delivery
# runner delegates those service-specific measurements back to the observer below.
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
# This service registers with aio-pika's queue.consume() directly. Its DeliveryObserver opens
# the CONSUMER span here from the runtime's tracing helpers, while common.run_delivery owns
# transport settlement.
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


_delivery_span: contextvars.ContextVar[Any] = contextvars.ContextVar("delivery_span", default=None)


class MusicBrainzDeliveryObserver:
    """Preserve this consumer's local telemetry around shared delivery settlement."""

    @contextlib.contextmanager
    def consume(self, destination: str, headers: object | None) -> Iterator[Any]:
        with consume_span(destination, headers) as span:
            token = _delivery_span.set(span)
            try:
                yield span
            finally:
                _delivery_span.reset(token)

    def settled(self, *, entity: str, result: DeliveryResult, duration_s: float, span: Any) -> None:
        if result.outcome != "control":
            pipeline_outcome = result.outcome if result.outcome in {"processed", "skipped"} else "failed"
            _record_pipeline_message(entity, pipeline_outcome)
        if result.settlement is Settlement.ACK and result.outcome in {"processed", "skipped"}:
            outage_backoff.reset()
            message_counts[entity] += 1
            last_message_time[entity] = time.time()
            if message_counts[entity] % progress_interval == 0:
                logger.info(
                    f"📊 Enriched {entity} in Neo4j",
                    message_counts=message_counts[entity],
                )
        _record_message_duration(entity, duration_s)
        destination = catalog_queue_name(WIRE_CONSUMER_NAME, entity)
        _record_consumed_message(destination, duration_s, result.error_type)
        if result.error_type is not None:
            _mark_span_failed(span, result.error_type)


delivery_observer = MusicBrainzDeliveryObserver()


def classify_delivery_failure(error: BaseException) -> FailureKind:
    """Classify database outages that require throttled broker redelivery."""
    if isinstance(error, (ServiceUnavailable, SessionExpired, DatabaseUnavailableError)):
        return FailureKind.TRANSIENT
    return FailureKind.DETERMINISTIC


async def wait_before_requeue() -> None:
    """Adapt the local backoff's diagnostic return value to the delivery contract."""
    await outage_backoff.wait()


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
    # Settlement belongs to common.run_delivery; retain the parameter as part of this public
    # helper's introspected compatibility surface.
    _ = message
    if data.get("type") == "file_complete":
        total_processed = data.get("total_processed", 0)
        logger.info(f"✅ File processing complete for {data_type}! Total records processed: {total_processed}")

        if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
            await schedule_consumer_cancellation(data_type, queues[data_type])

        # Mark as completed AFTER scheduling cancellation so the stuck-state
        # checker still fires for any in-flight messages during the delay.
        completed_files.add(data_type)

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

        return True

    return False


async def enrich_artist(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Artist node with MusicBrainz metadata."""
    return await _projections.enrich_artist(tx, record, enrichment_stats if stats is None else stats)


async def enrich_label(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Label node with MusicBrainz metadata."""
    return await _projections.enrich_label(tx, record, enrichment_stats if stats is None else stats)


async def enrich_release(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Release node with MusicBrainz metadata."""
    return await _projections.enrich_release(tx, record, enrichment_stats if stats is None else stats)


async def enrich_release_group(tx: Any, record: dict[str, Any], stats: dict[str, int] | None = None) -> bool:
    """Enrich an existing Master node with MusicBrainz release-group metadata."""
    return await _projections.enrich_release_group(tx, record, enrichment_stats if stats is None else stats)


async def create_relationship_edges(
    tx: Any,
    source_discogs_id: str,
    relations: list[dict[str, Any]],
    stats: dict[str, int] | None = None,
) -> None:
    """Create supported MusicBrainz relationship edges between Artist nodes."""
    await _projections.create_relationship_edges(
        tx,
        source_discogs_id,
        relations,
        enrichment_stats if stats is None else stats,
    )


# Processor lookup by data type
PROCESSORS: dict[str, Any] = {
    "artists": enrich_artist,
    "labels": enrich_label,
    "release-groups": enrich_release_group,
    "releases": enrich_release,
}


def make_message_handler(data_type: str, enrich_fn: _projections.Projection) -> Any:
    """Create a RabbitMQ message handler for the given data type."""

    destination = catalog_queue_name(WIRE_CONSUMER_NAME, data_type)

    async def handler(message: AbstractIncomingMessage) -> DeliveryResult:
        if shutdown_requested:
            # Leave the delivery UNACKED — never nack(requeue=True) here. The
            # consumer is still subscribed at this point, so a requeue is
            # redelivered within milliseconds and nacked again, burning a quorum
            # x-delivery-count per cycle; at x-delivery-limit=20 valid records
            # are dead-lettered within a second of a routine restart. Returning
            # without settling lets the connection close requeue them exactly once.
            logger.debug("🛑 Shutdown requested, leaving message unacked for redelivery")
            return DeliveryResult(Settlement.DEFER, "shutdown")

        async def operation() -> DeliveryResult:
            try:
                logger.debug("🔄 Received MusicBrainz message", data_type=data_type)
                body: dict[str, Any] = loads(message.body)

                if await check_file_completion(body, data_type, message):
                    return DeliveryResult(Settlement.ACK, "control")

                # Validate required 'id' field — nack with requeue=False to avoid
                # infinite requeue loop for malformed messages (matches brainztableinator).
                if "id" not in body:
                    logger.error("❌ Message missing 'id' field", data_type=data_type)
                    return DeliveryResult(Settlement.REJECT, "failed", "ValidationError")

                data_id: str = body["id"]
                if not data_id:
                    logger.warning("⚠️ Nacking record with empty mbid/id", data_type=data_type)
                    return DeliveryResult(Settlement.REJECT, "failed", "ValidationError")

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
                consumer_span = _delivery_span.get()
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

                # entities_enriched vs. entities_skipped_no_discogs_match is exactly the
                # per-message enrich_fn outcome (see enrich_artist/label/release/release_group):
                # a record with no Discogs match is deliberately skipped, not an error.
                outcome = "processed" if local_stats["entities_enriched"] > 0 else "skipped"
                return DeliveryResult(Settlement.ACK, outcome)
            except (ServiceUnavailable, SessionExpired, DatabaseUnavailableError) as e:
                logger.warning(
                    f"⚠️ Neo4j unavailable, will retry {data_type} message",
                    error=str(e),
                )
                raise
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(
                    f"❌ Failed to process {data_type} MusicBrainz message",
                    error=str(e),
                )
                # Preserve the existing immediate requeue default for unknown failures. Known
                # database outages escape to the classifier and wait exactly once.
                return DeliveryResult(Settlement.REQUEUE, "failed", type(e).__name__)

        return await run_delivery(
            message,
            operation,
            classifier=classify_delivery_failure,
            observer=delivery_observer,
            destination=destination,
            entity=data_type,
            headers=getattr(message, "headers", None),
            wait_before_requeue=wait_before_requeue,
        )

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
                queues[data_type] = await declare_stream_queue(active_channel, WIRE_CONSUMER_NAME, data_type)

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

        queues = {}
        for data_type in MUSICBRAINZ_DATA_TYPES:
            queues[data_type] = await declare_stream_queue(channel, WIRE_CONSUMER_NAME, data_type)

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
