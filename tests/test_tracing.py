"""OpenTelemetry spans emitted by brainzgraphinator.

Every assertion here is about the shape a collector and its span-metrics connector depend on:
span name, kind, the closed attribute set from the GrooveMap OpenTelemetry conventions, and the
parent/link structure that stitches one MusicBrainz record's journey into a single trace.

Deliveries are driven through a fake AMQP channel rather than by calling the handler directly,
so the headers a broker hands a consumer are the ones the CONSUMER span extracts from.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from common import telemetry
from common.telemetry import setup_telemetry, shutdown_telemetry
from neo4j.exceptions import ServiceUnavailable
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import MetricExporter, MetricExportResult
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import NoOpTracerProvider, SpanKind, StatusCode
from orjson import dumps

import brainzgraphinator.brainzgraphinator as bgmod


if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from opentelemetry.sdk.trace import ReadableSpan


METRIC_EXPORTER_IMPORT_PATH = "opentelemetry.exporter.otlp.proto.http.metric_exporter"
SPAN_EXPORTER_IMPORT_PATH = "opentelemetry.exporter.otlp.proto.http.trace_exporter"
ENDPOINT = "http://otel-collector:4318"

# A well-formed W3C parent with the sampled flag set: the wire format an extractor's publish
# span leaves in the AMQP headers. Written out by hand rather than produced by a live span,
# because what is being tested is that the consumer joins a trace it did not start.
UPSTREAM_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
UPSTREAM_SPAN_ID = "00f067aa0ba902b7"
TRACEPARENT = f"00-{UPSTREAM_TRACE_ID}-{UPSTREAM_SPAN_ID}-01"

ARTISTS_QUEUE = bgmod.catalog_queue_name(bgmod.WIRE_CONSUMER_NAME, "artists")


# ── Fake broker ──────────────────────────────────────────────────────────────


class FakeIncomingMessage:
    """The subset of an aio-pika incoming message this service's handlers touch."""

    def __init__(self, body: bytes, headers: dict[str, Any] | None) -> None:
        self.body = body
        self.headers = headers
        self.acked = 0
        self.nacks: list[bool] = []

    async def ack(self) -> None:
        self.acked += 1

    async def nack(self, requeue: bool = True) -> None:
        self.nacks.append(requeue)


class FakeQueue:
    """A queue that hands whatever is published to it straight to its consumer."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.handler: Any = None

    async def consume(self, handler: Any, consumer_tag: str | None = None) -> str:
        self.handler = handler
        return consumer_tag or f"tag-{self.name}"

    async def deliver(self, message: FakeIncomingMessage) -> None:
        assert self.handler is not None, "nothing is consuming this queue"
        await self.handler(message)


class FakeChannel:
    """A channel that declares fake queues and publishes into them synchronously."""

    def __init__(self) -> None:
        self.queues: dict[str, FakeQueue] = {}

    async def declare_queue(self, name: str, **_kwargs: Any) -> FakeQueue:
        return self.queues.setdefault(name, FakeQueue(name))

    async def publish(self, routing_key: str, record: dict[str, Any], headers: dict[str, Any] | None = None) -> FakeIncomingMessage:
        message = FakeIncomingMessage(dumps(record), headers)
        await self.queues[routing_key].deliver(message)
        return message


# ── In-memory exporters ──────────────────────────────────────────────────────


class SpanCollector:
    """An in-memory tracer provider plus helpers for reading what brainzgraphinator opened."""

    def __init__(self) -> None:
        self.exported: list[ReadableSpan] = []
        self.provider = SdkTracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(_ListExporter(self.exported)))

    def names(self) -> list[str]:
        return [span.name for span in self.exported]

    def one(self, name: str) -> ReadableSpan:
        matching = [span for span in self.exported if span.name == name]
        assert len(matching) == 1, f"expected exactly one {name!r} span, got {self.names()!r}"
        return matching[0]


class _ListExporter(SpanExporter):
    """Append every finished span to a list."""

    def __init__(self, sink: list[ReadableSpan]) -> None:
        self.sink = sink

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.sink.extend(spans)
        return SpanExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30_000) -> bool:  # noqa: ARG002
        return True

    def shutdown(self) -> None:
        """Nothing to release."""


def _refuse_to_build_a_span_exporter(**_kwargs: Any) -> SpanExporter:
    """Stand in for the OTLP span exporter and fail loudly if tracing tries to export."""
    raise AssertionError("no span exporter may be built while OTEL_TRACES_EXPORTER=none")


class CapturingMetricExporter(MetricExporter):
    """Stands in for the OTLP metric exporter and records the metric names it was handed."""

    def __init__(self, sink: list[str] | None = None, **_kwargs: Any) -> None:
        super().__init__(preferred_temporality={}, preferred_aggregation={})
        self.sink = sink if sink is not None else []

    def export(self, metrics_data: Any, timeout_millis: float = 10_000, **_kwargs: Any) -> MetricExportResult:  # noqa: ARG002
        for resource_metrics in metrics_data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                self.sink.extend(metric.name for metric in scope_metrics.metrics)
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:  # noqa: ARG002
        return True

    def shutdown(self, timeout_millis: float = 30_000, **_kwargs: Any) -> None:
        """Nothing to release."""


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def isolated_telemetry(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Give every test pristine provider handles; conftest clears the environment."""
    for name in ("_provider", "_sdk_provider", "_tracer_provider", "_sdk_tracer_provider"):
        monkeypatch.setattr(telemetry, name, None)
    bgmod.reset_telemetry_instruments()
    yield
    for name in ("_provider", "_sdk_provider", "_tracer_provider", "_sdk_tracer_provider"):
        monkeypatch.setattr(telemetry, name, None)
    bgmod.reset_telemetry_instruments()


@pytest.fixture
def traces(monkeypatch: pytest.MonkeyPatch) -> SpanCollector:
    """Install an in-memory tracer provider and let get_tracer() find it."""
    collector = SpanCollector()
    monkeypatch.setattr(telemetry, "_tracer_provider", collector.provider)
    return collector


@pytest_asyncio.fixture
async def artists_queue() -> FakeQueue:
    """A fake artists queue with the service's own handler consuming it."""
    channel = FakeChannel()
    queue = await channel.declare_queue(ARTISTS_QUEUE)
    await queue.consume(bgmod.on_artist_message, consumer_tag=f"{bgmod.SERVICE_NAME}-artists")
    return queue


def _writing_session(mock_neo4j_driver: MagicMock, tx: Any) -> None:
    """Make the driver's session actually run the transaction function it is given."""

    async def run(func: Any) -> Any:
        return await func(tx)

    session = mock_neo4j_driver.session.return_value.__aenter__.return_value
    session.execute_write.side_effect = run


def _matched_tx() -> AsyncMock:
    """A transaction mock whose MATCH queries always find a node."""
    tx = AsyncMock()
    result = AsyncMock()
    result.single.return_value = {"matched_id": 12345}
    counters = MagicMock()
    counters.relationships_created = 1
    counters.contains_updates = True
    summary = MagicMock()
    summary.counters = counters
    result.consume.return_value = summary
    tx.run.return_value = result
    return tx


async def _deliver(
    queue: FakeQueue,
    record: dict[str, Any],
    driver: MagicMock,
    headers: dict[str, Any] | None = None,
) -> FakeIncomingMessage:
    """Publish one record into the fake queue with the graph driver patched in."""
    message = FakeIncomingMessage(dumps(record), headers)
    with patch("brainzgraphinator.brainzgraphinator.graph", driver):
        await queue.deliver(message)
    return message


# ── CONSUMER span: process {queue} ───────────────────────────────────────────


class TestConsumerSpan:
    """`process {queue}`, kind CONSUMER, joined to the publisher's trace."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_span_joins_the_traceparent_the_message_carried(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """The delivery is processed inside the extractor's trace, not a new one."""
        _writing_session(mock_neo4j_driver, _matched_tx())

        message = await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": TRACEPARENT})

        span = traces.one(f"process {ARTISTS_QUEUE}")
        assert span.kind is SpanKind.CONSUMER
        assert format(span.context.trace_id, "032x") == UPSTREAM_TRACE_ID
        assert span.parent is not None
        assert format(span.parent.span_id, "016x") == UPSTREAM_SPAN_ID
        assert dict(span.attributes) == {
            "messaging.system": "rabbitmq",
            "messaging.destination.name": ARTISTS_QUEUE,
            "messaging.operation.name": "process",
        }
        assert span.status.status_code is not StatusCode.ERROR
        assert message.acked == 1

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_bytes_headers_round_trip(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """Some brokers hand back AMQP header values as bytes; the trace still joins."""
        _writing_session(mock_neo4j_driver, _matched_tx())

        await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": TRACEPARENT.encode()})

        span = traces.one(f"process {ARTISTS_QUEUE}")
        assert format(span.context.trace_id, "032x") == UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_a_message_without_headers_starts_a_new_trace(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A publisher that never injected a context leaves the consumer to root the trace."""
        _writing_session(mock_neo4j_driver, _matched_tx())

        await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, None)

        span = traces.one(f"process {ARTISTS_QUEUE}")
        assert span.parent is None
        assert format(span.context.trace_id, "032x") != UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_a_malformed_traceparent_starts_a_new_trace(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """An unreadable context must not fail the record that delivered it."""
        _writing_session(mock_neo4j_driver, _matched_tx())

        message = await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": "not-a-traceparent"})

        span = traces.one(f"process {ARTISTS_QUEUE}")
        assert span.parent is None
        assert message.acked == 1

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_a_control_message_is_still_one_consumer_span(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
    ) -> None:
        """A file_complete signal is a delivery too, and it opens no flush span."""
        with (
            patch("brainzgraphinator.brainzgraphinator.completed_files", set()),
            patch("brainzgraphinator.brainzgraphinator.queues", {}),
        ):
            await _deliver(artists_queue, {"type": "file_complete", "total_processed": 100}, MagicMock(), {"traceparent": TRACEPARENT})

        assert traces.names() == [f"process {ARTISTS_QUEUE}"]

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", True)
    async def test_a_delivery_left_for_redelivery_opens_no_span(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """During shutdown a delivery is left unsettled and never processed, so never traced."""
        message = await _deliver(artists_queue, sample_artist_record, MagicMock(), {"traceparent": TRACEPARENT})

        assert traces.exported == []
        assert message.acked == 0
        assert message.nacks == []

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_a_failed_delivery_sets_status_error_and_error_type_only(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A failure carries `error.type` and nothing else -- no message, no span event."""
        session = mock_neo4j_driver.session.return_value.__aenter__.return_value
        session.execute_write.side_effect = ServiceUnavailable("Neo4j is down")

        with patch.object(bgmod.outage_backoff, "wait", AsyncMock(return_value=0.0)):
            await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": TRACEPARENT})

        span = traces.one(f"process {ARTISTS_QUEUE}")
        assert span.status.status_code is StatusCode.ERROR
        assert span.attributes["error.type"] == "ServiceUnavailable"
        assert span.events == ()


# ── INTERNAL span: flush neo4j {entity} ──────────────────────────────────────


class TestFlushSpan:
    """`flush neo4j {entity}`, kind INTERNAL, linked to the messages it covers."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_committed_flush_nests_under_and_links_the_consumer_span(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """One delivery is one flush, so the batch's single member is its own message span."""
        _writing_session(mock_neo4j_driver, _matched_tx())

        await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": TRACEPARENT})

        consumer = traces.one(f"process {ARTISTS_QUEUE}")
        flush = traces.one("flush neo4j artists")
        assert flush.kind is SpanKind.INTERNAL
        assert flush.parent is not None
        assert flush.parent.span_id == consumer.context.span_id
        assert dict(flush.attributes) == {
            "db.system.name": "neo4j",
            "groovemap.entity": "artists",
            "outcome": "committed",
        }
        assert [link.context.span_id for link in flush.links] == [consumer.context.span_id]

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_failed_flush_records_the_outcome_and_the_error_type(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """A write that raises fails its own flush span before the handler settles it."""
        session = mock_neo4j_driver.session.return_value.__aenter__.return_value
        session.execute_write.side_effect = ServiceUnavailable("Neo4j is down")

        with patch.object(bgmod.outage_backoff, "wait", AsyncMock(return_value=0.0)):
            await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": TRACEPARENT})

        flush = traces.one("flush neo4j artists")
        assert flush.attributes["outcome"] == "failed"
        assert flush.attributes["error.type"] == "ServiceUnavailable"
        assert flush.status.status_code is StatusCode.ERROR

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_the_driver_wrappers_db_span_nests_inside_the_flush_span(
        self,
        traces: SpanCollector,
        artists_queue: FakeQueue,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """`session neo4j` comes free from AsyncResilientNeo4jDriver and must nest, not wrap."""
        driver = bgmod.AsyncResilientNeo4jDriver(uri="bolt://neo4j:7687", auth=("neo4j", "password"))
        underlying = MagicMock()
        underlying.session.return_value = _async_context(_recording_session(_matched_tx()))

        with patch.object(driver, "get_connection", AsyncMock(return_value=underlying)):
            await _deliver(artists_queue, sample_artist_record, driver, {"traceparent": TRACEPARENT})

        flush = traces.one("flush neo4j artists")
        db = traces.one("session neo4j")
        assert db.kind is SpanKind.CLIENT
        assert db.parent is not None
        assert db.parent.span_id == flush.context.span_id
        assert dict(db.attributes) == {"db.system.name": "neo4j", "db.operation.name": "session"}

    def test_flush_links_are_capped_at_sixty_four(self, traces: SpanCollector) -> None:
        """A batching consumer must not carry one link per row into the collector."""
        tracer = traces.provider.get_tracer("test")
        spans = [tracer.start_span(f"process {ARTISTS_QUEUE}") for _ in range(100)]

        assert bgmod.MAX_FLUSH_LINKS == 64
        assert len(bgmod.flush_links(*spans)) == 64
        assert bgmod.flush_links(None) == []


def _recording_session(tx: Any) -> AsyncMock:
    """A Neo4j session mock whose execute_write runs the transaction function."""

    async def run(func: Any) -> Any:
        return await func(tx)

    session = AsyncMock()
    session.execute_write.side_effect = run
    return session


def _async_context(value: Any) -> AsyncMock:
    """Wrap a value in an async context manager."""
    manager = AsyncMock()
    manager.__aenter__ = AsyncMock(return_value=value)
    manager.__aexit__ = AsyncMock(return_value=False)
    return manager


# ── Environment contract ─────────────────────────────────────────────────────


class TestTracingEnvironment:
    """The env-var-only contract: either signal is independently switchable."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_endpoint_with_traces_exporter_none_flows_metrics_and_creates_no_spans(
        self,
        monkeypatch: pytest.MonkeyPatch,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """OTEL_TRACES_EXPORTER=none turns tracing off without touching metrics."""
        exported_metrics: list[str] = []
        monkeypatch.setattr(
            f"{METRIC_EXPORTER_IMPORT_PATH}.OTLPMetricExporter",
            lambda **kwargs: CapturingMetricExporter(exported_metrics, **kwargs),
        )
        monkeypatch.setattr(f"{SPAN_EXPORTER_IMPORT_PATH}.OTLPSpanExporter", _refuse_to_build_a_span_exporter)
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT)
        monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")

        meter_provider = setup_telemetry("brainzgraphinator")
        bgmod.reset_telemetry_instruments()
        try:
            assert isinstance(meter_provider, SdkMeterProvider)
            assert isinstance(telemetry.tracer_provider(), NoOpTracerProvider)
            _writing_session(mock_neo4j_driver, _matched_tx())
            message = await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": TRACEPARENT})
        finally:
            shutdown_telemetry()

        assert message.acked == 1
        assert "groovemap.pipeline.messages" in exported_metrics
        assert "groovemap.pipeline.batch.flush.duration" in exported_metrics

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_without_an_endpoint_no_span_is_recorded_and_handling_is_unchanged(
        self,
        artists_queue: FakeQueue,
        mock_neo4j_driver: MagicMock,
        sample_artist_record: dict[str, Any],
    ) -> None:
        """The wave-1 regression, extended to tracing: an unconfigured service pays nothing."""
        setup_telemetry("brainzgraphinator")
        try:
            assert isinstance(telemetry.tracer_provider(), NoOpTracerProvider)
            _writing_session(mock_neo4j_driver, _matched_tx())
            message = await _deliver(artists_queue, sample_artist_record, mock_neo4j_driver, {"traceparent": TRACEPARENT})
        finally:
            shutdown_telemetry()

        assert message.acked == 1

    @pytest.mark.asyncio
    async def test_the_event_loop_monitor_starts_on_the_consumers_loop_after_setup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """start_event_loop_monitor() runs inside main(), after setup_telemetry()."""
        calls: list[str] = []
        monkeypatch.setenv("STARTUP_DELAY", "0")
        monkeypatch.setattr(bgmod, "setup_logging", MagicMock())
        monkeypatch.setattr(bgmod, "setup_telemetry", MagicMock(side_effect=lambda *_a, **_k: calls.append("setup")))
        monkeypatch.setattr(bgmod, "shutdown_telemetry", MagicMock())
        monkeypatch.setattr(bgmod, "HealthServer", MagicMock())
        monkeypatch.setattr(bgmod, "start_event_loop_monitor", MagicMock(side_effect=lambda *_a, **_k: calls.append("monitor")))
        # Stop main() at the first thing that needs a backing service.
        monkeypatch.setattr(bgmod.BrainzgraphinatorConfig, "from_env", MagicMock(side_effect=ValueError("no config")))

        with caplog.at_level(logging.ERROR):
            await bgmod.main()

        assert calls == ["setup", "monitor"]
