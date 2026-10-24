"""Process exit status of the cli entry point: startup give-up exits 1, clean shutdown exits 0."""

import signal
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import brainzgraphinator.brainzgraphinator as service
from tests.neo4j_doubles import neo4j_driver, neo4j_session


if TYPE_CHECKING:
    from collections.abc import Iterator

REQUIRED_ENV = {"NEO4J_HOST": "localhost", "NEO4J_USERNAME": "neo4j", "NEO4J_PASSWORD": "pw"}


@pytest.fixture(autouse=True)
def startup_harness(monkeypatch: pytest.MonkeyPatch) -> Iterator[MagicMock]:
    """Stub process-level side effects; yield the shutdown_telemetry mock."""
    monkeypatch.setattr(service, "shutdown_requested", False)
    monkeypatch.setattr(service, "STARTUP_BANNER", "")
    monkeypatch.setenv("STARTUP_DELAY", "0")
    for name in ("NEO4J_HOST", "NEO4J_USERNAME", "NEO4J_PASSWORD", "HEALTH_PORT"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(f"{name}_FILE", raising=False)
    with (
        patch.object(service.signal, "signal"),
        patch.object(service, "setup_logging"),
        patch.object(service, "setup_telemetry"),
        patch.object(service, "start_event_loop_monitor"),
        patch.object(service, "HealthServer", return_value=MagicMock()),
        patch.object(service.asyncio, "sleep", AsyncMock(return_value=None)),
        patch.object(service, "shutdown_telemetry") as shutdown_telemetry,
    ):
        yield shutdown_telemetry


def _run_cli() -> int:
    with pytest.raises(SystemExit) as exit_info:
        service.cli()
    assert isinstance(exit_info.value.code, int)
    return exit_info.value.code


def test_missing_required_env_exits_1(startup_harness: MagicMock) -> None:
    assert _run_cli() == 1
    startup_harness.assert_called_once()


def test_invalid_health_port_exits_1(monkeypatch: pytest.MonkeyPatch, startup_harness: MagicMock) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("HEALTH_PORT", "abc")
    assert _run_cli() == 1
    startup_harness.assert_called_once()


def test_neo4j_unreachable_exits_1(monkeypatch: pytest.MonkeyPatch, startup_harness: MagicMock) -> None:
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    session = MagicMock()
    session.__aenter__ = AsyncMock(side_effect=OSError("neo4j down"))
    session.__aexit__ = AsyncMock(return_value=None)
    driver = MagicMock()
    driver.session.return_value = session
    with patch.object(service, "AsyncResilientNeo4jDriver", return_value=driver):
        assert _run_cli() == 1
    startup_harness.assert_called_once()


def _healthy_config_and_neo4j() -> tuple[MagicMock, Any]:
    return MagicMock(), neo4j_driver(session=neo4j_session(health_record={"test": 1}))


def test_amqp_retries_exhausted_exits_1(startup_harness: MagicMock) -> None:
    config, driver = _healthy_config_and_neo4j()
    manager = MagicMock(connect=AsyncMock(side_effect=OSError("broker down")))
    with (
        patch.object(service.BrainzgraphinatorConfig, "from_env", return_value=config),
        patch.object(service, "AsyncResilientNeo4jDriver", return_value=driver),
        patch.object(service, "AsyncResilientRabbitMQ", return_value=manager),
    ):
        assert _run_cli() == 1
    assert manager.connect.await_count == 5
    startup_harness.assert_called_once()


def test_no_amqp_connection_without_signal_exits_1(startup_harness: MagicMock) -> None:
    config, driver = _healthy_config_and_neo4j()
    manager = MagicMock(connect=AsyncMock(return_value=None))
    with (
        patch.object(service.BrainzgraphinatorConfig, "from_env", return_value=config),
        patch.object(service, "AsyncResilientNeo4jDriver", return_value=driver),
        patch.object(service, "AsyncResilientRabbitMQ", return_value=manager),
    ):
        assert _run_cli() == 1
    startup_harness.assert_called_once()


def test_signal_during_amqp_connect_exits_0(startup_harness: MagicMock) -> None:
    config, driver = _healthy_config_and_neo4j()

    async def connect() -> None:
        service.signal_handler(signal.SIGTERM, None)
        raise OSError("interrupted")

    manager = MagicMock(connect=connect)
    with (
        patch.object(service.BrainzgraphinatorConfig, "from_env", return_value=config),
        patch.object(service, "AsyncResilientNeo4jDriver", return_value=driver),
        patch.object(service, "AsyncResilientRabbitMQ", return_value=manager),
    ):
        assert _run_cli() == 0
    startup_harness.assert_called_once()


def test_clean_signal_shutdown_exits_0(monkeypatch: pytest.MonkeyPatch, startup_harness: MagicMock) -> None:
    for name in ("consumer_tags", "connection_consumer_tags", "queues"):
        monkeypatch.setattr(service, name, {})
    monkeypatch.setattr(service, "consumer_cancel_tasks", {})
    tags: list[str] = []

    async def consume(_handler: Any, *, consumer_tag: str) -> str:
        tags.append(consumer_tag)
        if len(tags) == len(service.MUSICBRAINZ_DATA_TYPES):
            service.signal_handler(signal.SIGTERM, None)
        return consumer_tag

    queue = AsyncMock(consume=consume, cancel=AsyncMock(return_value=None))
    channel = AsyncMock()
    channel.declare_queue.return_value = queue
    connection = AsyncMock()
    connection.channel.return_value = channel
    manager = AsyncMock()
    manager.connect.return_value = connection
    config, driver = _healthy_config_and_neo4j()
    with (
        patch.object(service.BrainzgraphinatorConfig, "from_env", return_value=config),
        patch.object(service, "AsyncResilientNeo4jDriver", return_value=driver),
        patch.object(service, "AsyncResilientRabbitMQ", return_value=manager),
    ):
        assert _run_cli() == 0
    startup_harness.assert_called_once()
