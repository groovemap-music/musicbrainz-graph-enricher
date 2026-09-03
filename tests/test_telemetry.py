"""Domain OpenTelemetry metrics recorded by brainzgraphinator.

Every assertion here is about the shape a collector and dashboards depend on: instrument
name, unit, and the closed attribute set from the GrooveMap OpenTelemetry metrics
conventions. Values are checked only where the value carries meaning (the batch size and the
active-consumer gauge).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage
from common import telemetry
from common.telemetry import setup_telemetry, shutdown_telemetry
from neo4j.exceptions import ServiceUnavailable
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from orjson import dumps

import brainzgraphinator.brainzgraphinator as bgmod


if TYPE_CHECKING:
    from collections.abc import Iterator

    from opentelemetry.sdk.metrics.export import Metric


class Collector:
    """An in-memory provider plus helpers for reading what brainzgraphinator recorded."""

    def __init__(self) -> None:
        self.reader = InMemoryMetricReader()
        self.provider = SdkMeterProvider(metric_readers=[self.reader])

    def metrics(self) -> dict[str, Metric]:
        """Collect once and return every recorded metric by name."""
        data = self.reader.get_metrics_data()
        if data is None:
            return {}
        return {
            metric.name: metric
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
        }

    def points(self, name: str) -> list[Any]:
        """Return the data points recorded for one metric name."""
        metric = self.metrics().get(name)
        return [] if metric is None else list(metric.data.data_points)

    def attribute_sets(self, name: str) -> list[dict[str, Any]]:
        """Return the attribute dicts recorded for one metric name."""
        return [dict(point.attributes) for point in self.points(name)]

    def point_for(self, name: str, **attributes: Any) -> Any:
        """Return the single data point matching every given attribute."""
        matching = [point for point in self.points(name) if all(dict(point.attributes).get(k) == v for k, v in attributes.items())]
        assert len(matching) == 1, f"expected exactly one {name} point for {attributes!r}, got {len(matching)}"
        return matching[0]


@pytest.fixture
def collector(monkeypatch: pytest.MonkeyPatch) -> Iterator[Collector]:
    """Install an in-memory provider and make brainzgraphinator build instruments against it."""
    active = Collector()
    monkeypatch.setattr(telemetry, "_provider", active.provider)
    monkeypatch.setattr(telemetry, "_generation", telemetry.provider_generation() + 1)
    bgmod.reset_telemetry_instruments()
    assert telemetry._active_provider() is active.provider
    yield active
    monkeypatch.setattr(telemetry, "_provider", None)
    bgmod.reset_telemetry_instruments()


def _execute_write_calling(mock_tx: Any) -> Any:
    """Build an execute_write side_effect that actually invokes the transaction function."""

    async def run(func: Any) -> Any:
        return await func(mock_tx)

    return run


def _matched_tx() -> AsyncMock:
    """A transaction mock whose MATCH queries always find a node."""
    tx = AsyncMock()
    mock_result = AsyncMock()
    mock_result.single.return_value = {"matched_id": 12345}
    mock_counters = MagicMock()
    mock_counters.relationships_created = 1
    mock_counters.contains_updates = True
    mock_summary = MagicMock()
    mock_summary.counters = mock_counters
    mock_result.consume.return_value = mock_summary
    tx.run.return_value = mock_result
    return tx


# ── groovemap.pipeline.messages / message.duration ──────────────────────────


class TestPipelineMessages:
    """groovemap.pipeline.messages and groovemap.pipeline.message.duration."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_processed_outcome(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A record with a Discogs match records outcome=processed."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(sample_artist_record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = _execute_write_calling(_matched_tx())

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for("groovemap.pipeline.messages", source="musicbrainz", entity="artists", outcome="processed")
        assert point.value == 1

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_skipped_outcome_no_discogs_match(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A record with no Discogs id is deliberately skipped, not failed."""
        record = dict(sample_artist_record)
        del record["discogs_artist_id"]
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = _execute_write_calling(AsyncMock())

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for("groovemap.pipeline.messages", source="musicbrainz", entity="artists", outcome="skipped")
        assert point.value == 1
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_failed_outcome_validation_error(self, collector: Collector) -> None:
        """A message missing the required 'id' field records outcome=failed."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps({"mbid": "abc", "discogs_artist_id": 1})

        with patch("brainzgraphinator.brainzgraphinator.graph", MagicMock()):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for("groovemap.pipeline.messages", source="musicbrainz", entity="artists", outcome="failed")
        assert point.value == 1

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_failed_outcome_neo4j_unavailable(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A transient Neo4j outage requeues the message and records outcome=failed."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(sample_artist_record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = ServiceUnavailable("Neo4j is down")

        with (
            patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver),
            patch.object(bgmod.outage_backoff, "wait", AsyncMock(return_value=0.0)),
        ):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for("groovemap.pipeline.messages", source="musicbrainz", entity="artists", outcome="failed")
        assert point.value == 1
        mock_message.nack.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_message_duration_recorded(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """Handling a message records its duration under {source, entity}."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(sample_artist_record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = _execute_write_calling(_matched_tx())

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for("groovemap.pipeline.message.duration", source="musicbrainz", entity="artists")
        assert point.count == 1
        assert point.sum >= 0

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_control_message_has_no_pipeline_message_outcome(self, collector: Collector) -> None:
        """A file_complete control message is not an enrichment attempt."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps({"type": "file_complete", "total_processed": 100})

        with (
            patch("brainzgraphinator.brainzgraphinator.graph", MagicMock()),
            patch("brainzgraphinator.brainzgraphinator.completed_files", set()),
            patch("brainzgraphinator.brainzgraphinator.queues", {}),
        ):
            await bgmod.on_artist_message(mock_message)

        assert collector.points("groovemap.pipeline.messages") == []


# ── groovemap.pipeline.batch.size / batch.flush.duration ────────────────────


class TestBatchFlush:
    """groovemap.pipeline.batch.size and groovemap.pipeline.batch.flush.duration."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_committed_flush(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A successful write flush records size=1 and outcome=committed."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(sample_artist_record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = _execute_write_calling(_matched_tx())

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await bgmod.on_artist_message(mock_message)

        size_point = collector.point_for("groovemap.pipeline.batch.size", store="neo4j", entity="artists")
        assert size_point.sum == 1
        assert size_point.count == 1

        duration_point = collector.point_for("groovemap.pipeline.batch.flush.duration", store="neo4j", entity="artists", outcome="committed")
        assert duration_point.count == 1

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_failed_flush(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A write that raises records outcome=failed, then re-raises for the outer handler."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(sample_artist_record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = ServiceUnavailable("Neo4j is down")

        with (
            patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver),
            patch.object(bgmod.outage_backoff, "wait", AsyncMock(return_value=0.0)),
        ):
            await bgmod.on_artist_message(mock_message)

        duration_point = collector.point_for("groovemap.pipeline.batch.flush.duration", store="neo4j", entity="artists", outcome="failed")
        assert duration_point.count == 1
        # groovemap.pipeline.batch.size carries no outcome attribute (per the shared
        # convention): it records the attempted flush size regardless of success.
        size_point = collector.point_for("groovemap.pipeline.batch.size", store="neo4j", entity="artists")
        assert size_point.sum == 1


# ── groovemap.pipeline.consumers.active ──────────────────────────────────────


class TestConsumersActiveGauge:
    """groovemap.pipeline.consumers.active tracks consumer start/stop."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.CONSUMER_CANCEL_DELAY", 0.05)
    async def test_start_then_scheduled_stop(self, collector: Collector) -> None:
        """A start followed by a scheduled cancellation nets to zero."""
        mock_queue = AsyncMock()

        with (
            patch("brainzgraphinator.brainzgraphinator.consumer_tags", {"artists": "tag-1"}),
            patch("brainzgraphinator.brainzgraphinator.consumer_cancel_tasks", {}),
        ):
            bgmod._record_consumer_delta(1)
            await bgmod.schedule_consumer_cancellation("artists", mock_queue)
            await asyncio.sleep(0.2)

        point = collector.point_for("groovemap.pipeline.consumers.active", source="musicbrainz")
        assert point.value == 0

    @pytest.mark.asyncio
    async def test_cancel_all_consumers_decrements_by_count(self, collector: Collector) -> None:
        """Shutdown's cancel-all path decrements once per active consumer."""
        mock_queue_a = AsyncMock()
        mock_queue_b = AsyncMock()
        bgmod._record_consumer_delta(1)
        bgmod._record_consumer_delta(1)

        with (
            patch("brainzgraphinator.brainzgraphinator.consumer_tags", {"artists": "tag-1", "labels": "tag-2"}),
            patch("brainzgraphinator.brainzgraphinator.queues", {"artists": mock_queue_a, "labels": mock_queue_b}),
        ):
            await bgmod.cancel_all_consumers()

        point = collector.point_for("groovemap.pipeline.consumers.active", source="musicbrainz")
        assert point.value == 0


# ── messaging.client.consumed.messages / operation.duration (local fallback) ─


class TestMessagingConsumedFallback:
    """This service registers with queue.consume() directly, bypassing
    common.process_message_with_retry, so these are recorded locally with the
    same instrument names and attribute shape the shared wrapper uses.
    """

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_success_has_no_error_type(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A successfully processed message is counted with no error.type attribute."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(sample_artist_record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = _execute_write_calling(_matched_tx())
        expected_destination = bgmod.catalog_queue_name(bgmod.WIRE_CONSUMER_NAME, "artists")

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for(
            "messaging.client.consumed.messages",
            **{"messaging.system": "rabbitmq", "messaging.destination.name": expected_destination, "messaging.operation.name": "process"},
        )
        assert point.value == 1
        assert "error.type" not in dict(point.attributes)

        duration_point = collector.point_for(
            "messaging.client.operation.duration",
            **{"messaging.system": "rabbitmq", "messaging.destination.name": expected_destination, "messaging.operation.name": "process"},
        )
        assert duration_point.count == 1

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_control_message_is_counted(self, collector: Collector) -> None:
        """Control messages are still transport-level deliveries."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps({"type": "file_complete", "total_processed": 100})
        expected_destination = bgmod.catalog_queue_name(bgmod.WIRE_CONSUMER_NAME, "artists")

        with (
            patch("brainzgraphinator.brainzgraphinator.graph", MagicMock()),
            patch("brainzgraphinator.brainzgraphinator.completed_files", set()),
            patch("brainzgraphinator.brainzgraphinator.queues", {}),
        ):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for(
            "messaging.client.consumed.messages",
            **{"messaging.system": "rabbitmq", "messaging.destination.name": expected_destination, "messaging.operation.name": "process"},
        )
        assert point.value == 1

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_failure_carries_error_type(
        self,
        collector: Collector,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A failed delivery's error.type is the exception class name, a closed set."""
        mock_message = AsyncMock(spec=AbstractIncomingMessage)
        mock_message.body = dumps(sample_artist_record)
        mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        mock_session.execute_write.side_effect = ServiceUnavailable("Neo4j is down")
        expected_destination = bgmod.catalog_queue_name(bgmod.WIRE_CONSUMER_NAME, "artists")

        with (
            patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver),
            patch.object(bgmod.outage_backoff, "wait", AsyncMock(return_value=0.0)),
        ):
            await bgmod.on_artist_message(mock_message)

        point = collector.point_for(
            "messaging.client.consumed.messages",
            **{
                "messaging.system": "rabbitmq",
                "messaging.destination.name": expected_destination,
                "messaging.operation.name": "process",
                "error.type": "ServiceUnavailable",
            },
        )
        assert point.value == 1


# ── Regression: telemetry is a safe no-op without a configured endpoint ─────


class TestTelemetryRegression:
    """With OTEL_EXPORTER_OTLP_ENDPOINT unset, the service behaves exactly as before."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_message_processing_unaffected_without_endpoint(
        self,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """setup_telemetry/shutdown_telemetry never raise, and message handling is unchanged."""
        setup_telemetry("brainzgraphinator")
        try:
            mock_message = AsyncMock(spec=AbstractIncomingMessage)
            mock_message.body = dumps(sample_artist_record)
            mock_session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
            mock_session.execute_write.side_effect = _execute_write_calling(_matched_tx())

            with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
                await bgmod.on_artist_message(mock_message)

            mock_message.ack.assert_called_once()
        finally:
            shutdown_telemetry()
