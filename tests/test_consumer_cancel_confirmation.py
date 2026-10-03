"""Bounded broker confirmation and restart regressions; no live backing services."""

import asyncio
import signal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika import Queue

import brainzgraphinator.brainzgraphinator as service
from tests.neo4j_doubles import neo4j_driver, neo4j_session, neo4j_transaction


@pytest.fixture(autouse=True)
def isolated_service_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep these lifecycle probes independent of existing global-state tests."""
    for name, value in {
        "consumer_tags": {},
        "connection_consumer_tags": {},
        "consumer_cancel_tasks": {},
        "consumer_watch_started_at": 0.0,
        "completed_files": set(),
        "message_counts": dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, 0),
        "last_message_time": dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, 0.0),
        "active_channel": None,
        "active_connection": None,
        "shutdown_requested": False,
        "queues": {},
        "graph": None,
        "_stats_lock": None,
    }.items():
        monkeypatch.setattr(service, name, value)


@pytest.mark.asyncio
async def test_consecutive_cancels_complete_on_one_serialized_channel() -> None:
    """A no-wait request gets no reply; confirmed requests release the RPC lock."""
    rpc_lock = asyncio.Lock()
    confirmed: list[str] = []

    async def basic_cancel(*, consumer_tag: str, nowait: bool, timeout: float) -> object:
        assert timeout > 0
        async with rpc_lock:
            if nowait:
                # Reproduce the missing CancelOk that left the channel RPC stuck.
                await asyncio.Event().wait()
            confirmed.append(consumer_tag)
            return object()

    underlay = MagicMock()
    underlay.basic_cancel = basic_cancel
    channel = MagicMock()
    channel.get_underlay_channel = AsyncMock(return_value=underlay)
    queue = Queue(channel, "test", durable=True, exclusive=False, auto_delete=False, arguments=None)
    service.consumer_tags = {kind: f"tag-{kind}" for kind in service.MUSICBRAINZ_DATA_TYPES}
    with patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.05):
        for kind, tag in list(service.consumer_tags.items()):
            assert await service._cancel_consumer(kind, tag, queue)
    assert len(confirmed) == len(service.MUSICBRAINZ_DATA_TYPES)
    assert service.consumer_tags == {}


@pytest.mark.asyncio
async def test_tag_remains_until_cancel_confirmation() -> None:
    requested = asyncio.Event()
    confirmed = asyncio.Event()

    async def cancel(*_args: Any, **_kwargs: Any) -> None:
        requested.set()
        await confirmed.wait()

    service.consumer_tags = {"artists": "tag-a"}
    task = asyncio.create_task(service._cancel_consumer("artists", "tag-a", AsyncMock(cancel=cancel)))
    await requested.wait()
    assert service.consumer_tags == {"artists": "tag-a"}
    confirmed.set()
    assert await task
    assert service.consumer_tags == {}


@pytest.mark.asyncio
async def test_completion_cancel_timeout_keeps_tag_and_requests_recovery() -> None:
    queue = AsyncMock()

    async def no_reply(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.Event().wait()

    queue.cancel.side_effect = no_reply
    service.consumer_tags = {"artists": "tag-a"}
    with (
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
        patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01),
        patch.object(service, "logger") as logger,
    ):
        await service.schedule_consumer_cancellation("artists", queue)
        await asyncio.wait_for(service.consumer_cancel_tasks["artists"], timeout=0.5)
    assert service.consumer_tags == {"artists": "tag-a"}
    assert service.consumer_recovery_requested
    assert logger.warning.call_args.kwargs["timed_out"] is True


@pytest.mark.asyncio
async def test_failed_cancel_triggers_periodic_recovery_despite_tags_and_completion() -> None:
    service.consumer_recovery_requested = True
    service.consumer_tags = {"artists": "tag-a"}
    service.completed_files = set(service.MUSICBRAINZ_DATA_TYPES)

    async def recover() -> None:
        service.shutdown_requested = True

    with patch.object(service, "STUCK_CHECK_INTERVAL", 0), patch.object(service, "_recover_consumers", side_effect=recover) as recovery:
        await asyncio.wait_for(service.periodic_queue_checker(), timeout=0.5)
    recovery.assert_awaited_once()


@pytest.mark.asyncio
async def test_sigterm_shutdown_bounds_all_cancel_waits_and_closes_transport() -> None:
    async def no_reply(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.Event().wait()

    service.consumer_tags = {kind: f"tag-{kind}" for kind in service.MUSICBRAINZ_DATA_TYPES}
    service.queues = {kind: AsyncMock(cancel=no_reply) for kind in service.MUSICBRAINZ_DATA_TYPES}
    connection = AsyncMock()
    service.active_connection = connection
    service.active_channel = AsyncMock()
    service.signal_handler(signal.SIGTERM, None)
    with patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01):
        await asyncio.wait_for(service.cancel_all_consumers(), timeout=0.5)
        assert len(service.consumer_tags) == len(service.MUSICBRAINZ_DATA_TYPES)
        await asyncio.wait_for(service.close_rabbitmq_connection(), timeout=0.5)
    connection.close.assert_awaited_once()
    assert service.shutdown_requested
    assert service.consumer_tags == {}
    assert service.active_connection is None


@pytest.mark.asyncio
async def test_next_extraction_record_is_enriched_settled_and_rearms_completion() -> None:
    service.completed_files = set(service.MUSICBRAINZ_DATA_TYPES)
    queue = AsyncMock()
    service.consumer_tags = {"artists": "tag-a"}
    service.graph = neo4j_driver(session=neo4j_session(transaction=neo4j_transaction()))
    enrich = AsyncMock(return_value=True)
    handler = service.make_message_handler("artists", enrich)
    with patch.object(service, "CONSUMER_CANCEL_DELAY", 60):
        await service.schedule_consumer_cancellation("artists", queue)
        timer = service.consumer_cancel_tasks["artists"]
        message = AsyncMock(body=b'{"id":"synthetic-next-run"}', routing_key="artists")
        await handler(message)
        await asyncio.gather(timer, return_exceptions=True)
    enrich.assert_awaited_once()
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    assert "artists" not in service.completed_files
    assert "artists" not in service.consumer_cancel_tasks
    queue.cancel.assert_not_awaited()
    service.message_counts["artists"] = 1
    service.consumer_tags.clear()
    assert "artists" in service._consumer_alarm_types()


@pytest.mark.asyncio
async def test_replaced_timer_cannot_remove_new_timer_reference() -> None:
    with patch.object(service, "CONSUMER_CANCEL_DELAY", 60):
        await service.schedule_consumer_cancellation("artists", AsyncMock())
        old = service.consumer_cancel_tasks["artists"]
        await asyncio.sleep(0)  # Start old task so its finally clause runs on cancel.
        await service.schedule_consumer_cancellation("artists", AsyncMock())
        new = service.consumer_cancel_tasks["artists"]
        await asyncio.gather(old, return_exceptions=True)
        assert service.consumer_cancel_tasks["artists"] is new
        new.cancel()
        await asyncio.gather(new, return_exceptions=True)


@pytest.mark.asyncio
async def test_transport_close_timeout_retains_state_for_retry() -> None:
    async def no_reply() -> None:
        await asyncio.Event().wait()

    channel = AsyncMock(close=no_reply)
    service.active_channel = channel
    service.consumer_tags = {"artists": "tag-a"}
    with patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01):
        await asyncio.wait_for(service.close_rabbitmq_connection(), timeout=0.5)
    assert service.active_channel is channel
    assert service.consumer_tags == {"artists": "tag-a"}
    assert service.consumer_recovery_requested


@pytest.mark.asyncio
async def test_main_sigterm_path_finishes_even_without_cancel_replies() -> None:
    subscribed = asyncio.Event()
    tags: list[str] = []

    async def consume(_handler: Any, *, consumer_tag: str) -> str:
        tags.append(consumer_tag)
        if len(tags) == len(service.MUSICBRAINZ_DATA_TYPES):
            subscribed.set()
        return consumer_tag

    async def no_reply(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.Event().wait()

    queue = AsyncMock(consume=consume, cancel=no_reply)
    channel = AsyncMock()
    channel.declare_queue.return_value = queue
    connection = AsyncMock()
    connection.channel.return_value = channel
    manager = AsyncMock()
    manager.connect.return_value = connection
    graph = MagicMock()
    graph.session.return_value = AsyncMock()
    graph.close = AsyncMock()
    health = MagicMock()
    with (
        patch.dict("os.environ", {"STARTUP_DELAY": "0"}),
        patch.object(service.signal, "signal"),
        patch.object(service, "setup_logging"),
        patch.object(service, "setup_telemetry"),
        patch.object(service, "start_event_loop_monitor"),
        patch.object(service, "shutdown_telemetry"),
        patch.object(service, "HealthServer", return_value=health),
        patch.object(service.BrainzgraphinatorConfig, "from_env", return_value=MagicMock()),
        patch.object(service, "AsyncResilientNeo4jDriver", return_value=graph),
        patch.object(service, "AsyncResilientRabbitMQ", return_value=manager),
        patch.object(service, "CONSUMER_CANCEL_TIMEOUT", 0.01),
    ):
        task = asyncio.create_task(service.main())
        try:
            await asyncio.wait_for(subscribed.wait(), timeout=1)
            service.signal_handler(signal.SIGTERM, None)
            await asyncio.wait_for(task, timeout=2)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    connection.close.assert_awaited_once()
    graph.close.assert_awaited_once()
    health.stop.assert_called_once()
    assert service.consumer_tags == {}


@pytest.mark.asyncio
async def test_recovery_replaces_retained_tags_after_confirmed_transport_close() -> None:
    old_connection = AsyncMock()
    service.active_connection = old_connection
    service.active_channel = AsyncMock()
    service.consumer_tags = {"artists": "old-tag"}
    service.consumer_recovery_requested = True
    channel = AsyncMock()

    def declare_queue(**kwargs: Any) -> Any:
        if kwargs.get("passive"):
            queue = MagicMock()
            queue.declaration_result.message_count = 1 if kwargs["name"].endswith("-artists") else 0
            return queue
        queue = AsyncMock()
        queue.consume.return_value = f"new-{kwargs['name']}"
        return queue

    channel.declare_queue.side_effect = declare_queue
    connection = AsyncMock()
    connection.channel.return_value = channel
    manager = AsyncMock()
    manager.connect.return_value = connection

    async def declare_stream(_channel: Any, _consumer: str, kind: str) -> AsyncMock:
        queue = AsyncMock()
        queue.consume.return_value = f"new-{kind}"
        return queue

    with patch.object(service, "rabbitmq_manager", manager), patch.object(service, "declare_stream_queue", side_effect=declare_stream):
        await service._recover_consumers()
    old_connection.close.assert_awaited_once()
    assert set(service.consumer_tags) == set(service.MUSICBRAINZ_DATA_TYPES)
    assert all(tag.startswith("new-") for tag in service.consumer_tags.values())
    assert not service.consumer_recovery_requested


@pytest.mark.asyncio
async def test_interrupted_inflight_cancel_requests_recovery() -> None:
    requested = asyncio.Event()

    async def no_reply(*_args: Any, **_kwargs: Any) -> None:
        requested.set()
        await asyncio.Event().wait()

    service.consumer_tags = {"artists": "tag-a"}
    task = asyncio.create_task(service._cancel_consumer("artists", "tag-a", AsyncMock(cancel=no_reply)))
    await requested.wait()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert service.consumer_tags == {"artists": "tag-a"}
    assert service.consumer_recovery_requested


@pytest.mark.asyncio
async def test_confirmed_cancel_keeps_registration_history_until_transport_close() -> None:
    service.consumer_tags = {"release-groups": "tag-rg"}
    service.connection_consumer_tags = {"artists": "tag-a", "release-groups": "tag-rg"}
    with patch.object(service, "_record_consumer_delta") as delta:
        assert await service._cancel_consumer("release-groups", "tag-rg", AsyncMock())
        assert service.connection_consumer_tags == {"artists": "tag-a", "release-groups": "tag-rg"}
        delta.assert_called_once_with(-1)
        service.active_connection = AsyncMock()
        await service.close_rabbitmq_connection()
    assert service.connection_consumer_tags == {}
