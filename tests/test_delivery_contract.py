"""Contract tests for the shared single-delivery settlement boundary."""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import DeliveryResult, Settlement
from neo4j.exceptions import ServiceUnavailable
from orjson import dumps

import brainzgraphinator.brainzgraphinator as service
from tests.neo4j_doubles import neo4j_transaction


RUNTIME_REVISION = "e372b6a7598ae31ee6578fdff39bc920bedd7136"
ROOT = Path(__file__).parent.parent


class StrictDelivery:
    """A broker fake that makes a second terminal operation impossible to hide."""

    def __init__(self, body: bytes, *, fail_settlement: bool = False) -> None:
        self.body = body
        self.headers: dict[str, Any] = {}
        self.settlements: list[tuple[str, bool | None]] = []
        self.fail_settlement = fail_settlement

    def _record(self, operation: str, requeue: bool | None) -> None:
        if self.settlements:
            raise AssertionError("delivery was settled more than once")
        self.settlements.append((operation, requeue))
        if self.fail_settlement:
            raise RuntimeError("broker settlement failed")

    async def ack(self) -> None:
        self._record("ack", None)

    async def nack(self, *, requeue: bool) -> None:
        self._record("nack", requeue)


def _writing_driver(result: object | BaseException = True) -> MagicMock:
    driver = MagicMock()
    session = AsyncMock()
    driver.session.return_value.__aenter__ = AsyncMock(return_value=session)
    driver.session.return_value.__aexit__ = AsyncMock(return_value=False)

    async def execute_write(function: Any) -> Any:
        if isinstance(result, BaseException):
            raise result
        return await function(neo4j_transaction())

    session.execute_write.side_effect = execute_write
    return driver


@pytest.mark.asyncio
async def test_strict_delivery_ack(sample_artist_record: dict[str, Any]) -> None:
    delivery = StrictDelivery(dumps(sample_artist_record))
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", _writing_driver()),
    ):
        result = await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert result.settlement is Settlement.ACK
    assert delivery.settlements == [("ack", None)]


@pytest.mark.asyncio
async def test_strict_delivery_rejects_missing_id() -> None:
    delivery = StrictDelivery(dumps({"mbid": "missing-id"}))
    with patch.object(service, "shutdown_requested", False):
        result = await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert result == DeliveryResult(Settlement.REJECT, "failed", "ValidationError")
    assert delivery.settlements == [("nack", False)]


@pytest.mark.asyncio
async def test_strict_delivery_requeues_unknown_failure_without_waiting() -> None:
    delivery = StrictDelivery(b"not-json")
    wait = AsyncMock()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service.outage_backoff, "wait", wait),
    ):
        result = await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert result.settlement is Settlement.REQUEUE
    assert result.error_type is not None
    assert delivery.settlements == [("nack", True)]
    wait.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_delivery_throttles_known_outage_once(sample_artist_record: dict[str, Any]) -> None:
    delivery = StrictDelivery(dumps(sample_artist_record))
    wait = AsyncMock()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", _writing_driver(ServiceUnavailable("down"))),
        patch.object(service.outage_backoff, "wait", wait),
    ):
        result = await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert result == DeliveryResult(Settlement.REQUEUE, "transient", "ServiceUnavailable")
    assert delivery.settlements == [("nack", True)]
    wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_strict_delivery_defers_shutdown_without_observation() -> None:
    delivery = StrictDelivery(b"__not_even_read__")
    observer = MagicMock()
    with (
        patch.object(service, "shutdown_requested", True),
        patch.object(service, "delivery_observer", observer),
    ):
        result = await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert result == DeliveryResult(Settlement.DEFER, "shutdown")
    assert delivery.settlements == []
    observer.consume.assert_not_called()
    observer.settled.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_delivery_has_no_settlement_wait_classification_or_failure_observation(
    sample_artist_record: dict[str, Any],
) -> None:
    delivery = StrictDelivery(dumps(sample_artist_record))
    classifier = MagicMock(wraps=service.classify_delivery_failure)
    wait = AsyncMock()
    settled = MagicMock(wraps=service.delivery_observer.settled)
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", _writing_driver(asyncio.CancelledError())),
        patch.object(service, "classify_delivery_failure", classifier),
        patch.object(service.outage_backoff, "wait", wait),
        patch.object(service.delivery_observer, "settled", settled),
        pytest.raises(asyncio.CancelledError),
    ):
        await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert delivery.settlements == []
    classifier.assert_not_called()
    wait.assert_not_awaited()
    settled.assert_not_called()


@pytest.mark.asyncio
async def test_settlement_failure_is_visible_and_never_retried(sample_artist_record: dict[str, Any]) -> None:
    delivery = StrictDelivery(dumps(sample_artist_record), fail_settlement=True)
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", _writing_driver()),
        pytest.raises(RuntimeError, match="broker settlement failed"),
    ):
        await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert delivery.settlements == [("ack", None)]


def test_shared_batch_runtime_is_not_a_musicbrainz_dependency() -> None:
    package_source = "\n".join(path.read_text() for path in (ROOT / "brainzgraphinator").glob("*.py"))
    assert "common.batch" not in package_source


def test_only_shared_runner_performs_terminal_delivery_calls() -> None:
    source = (ROOT / "brainzgraphinator" / "brainzgraphinator.py").read_text()
    assert "message.ack(" not in source
    assert "message.nack(" not in source
    assert "run_delivery(" in source


def test_manifest_and_lock_pin_the_same_reviewed_runtime_revision() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text()
    lock = (ROOT / "uv.lock").read_text()
    manifest_pin = re.search(r'groovemap-runtime = \{ git = "[^"]+", rev = "([0-9a-f]{40})" \}', pyproject)
    lock_pins = set(re.findall(r"python-libraries\.git\?rev=([0-9a-f]{40})", lock))

    assert manifest_pin is not None
    assert manifest_pin.group(1) == RUNTIME_REVISION
    assert lock_pins == {RUNTIME_REVISION}
