"""Live Neo4j checks for the shared single-delivery settlement boundary."""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from common import DeliveryResult, OutageBackoff, Settlement
from neo4j import AsyncDriver, AsyncGraphDatabase
from orjson import dumps

import brainzgraphinator.brainzgraphinator as service


pytestmark = pytest.mark.integration


async def one(driver: AsyncDriver, query: str, **parameters: Any) -> dict[str, Any]:
    """Run a read assertion and return its single record as a plain mapping."""
    async with driver.session(database="neo4j") as session:
        result = await session.run(query, **parameters)
        record = await result.single(strict=True)
        return dict(record)


@pytest_asyncio.fixture
async def neo4j_driver() -> AsyncDriver:
    """Connect to and clean the disposable Neo4j started by the integration recipe."""
    driver = AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
    )
    await driver.verify_connectivity()
    try:
        yield driver
    finally:
        async with driver.session(database="neo4j") as session:
            result = await session.run("MATCH (node) DETACH DELETE node")
            await result.consume()
        await driver.close()


class StrictDelivery:
    """Broker fake that rejects any second terminal operation."""

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.headers: dict[str, Any] = {}
        self.settlements: list[tuple[str, bool | None]] = []

    def record(self, operation: str, requeue: bool | None) -> None:
        if self.settlements:
            raise AssertionError("delivery was settled more than once")
        self.settlements.append((operation, requeue))

    async def ack(self) -> None:
        self.record("ack", None)

    async def nack(self, *, requeue: bool) -> None:
        self.record("nack", requeue)


class CommitCheckingDelivery(StrictDelivery):
    """ACK succeeds only after another session can observe the committed write."""

    def __init__(self, body: bytes, driver: AsyncDriver, discogs_id: str, mbid: str) -> None:
        super().__init__(body)
        self.driver = driver
        self.discogs_id = discogs_id
        self.mbid = mbid

    async def ack(self) -> None:
        visible = await one(
            self.driver,
            "MATCH (artist:Artist {id: $id}) RETURN artist.mbid AS mbid",
            id=self.discogs_id,
        )
        assert visible == {"mbid": self.mbid}, "ACK ran before the Neo4j transaction committed"
        self.record("ack", None)


@pytest.mark.asyncio
async def test_run_delivery_commits_real_graph_write_before_ack(neo4j_driver: AsyncDriver) -> None:
    discogs_id = "integration-discogs-artist"
    mbid = "integration-musicbrainz-artist"
    async with neo4j_driver.session(database="neo4j") as session:
        result = await session.run("CREATE (:Artist {id: $id})", id=discogs_id)
        await result.consume()

    delivery = CommitCheckingDelivery(
        dumps({"id": mbid, "discogs_artist_id": discogs_id, "name": "Integration Artist"}),
        neo4j_driver,
        discogs_id,
        mbid,
    )
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", neo4j_driver),
    ):
        result = await service.on_artist_message(delivery)  # type: ignore[arg-type]

    assert result == DeliveryResult(Settlement.ACK, "processed")
    assert delivery.settlements == [("ack", None)]


@pytest.mark.asyncio
async def test_real_driver_connection_failure_is_throttled_and_requeued(neo4j_driver: AsyncDriver) -> None:
    assert await one(neo4j_driver, "RETURN 1 AS ready") == {"ready": 1}
    unavailable = AsyncGraphDatabase.driver(
        "bolt://127.0.0.1:1",
        auth=("neo4j", "disposable"),
        connection_timeout=0.1,
        max_transaction_retry_time=0,
    )
    delivery = StrictDelivery(dumps({"id": "mb-unavailable", "discogs_artist_id": "missing"}))
    backoff = OutageBackoff("integration", initial_delay=0, max_delay=0)
    try:
        with (
            patch.object(service, "shutdown_requested", False),
            patch.object(service, "graph", unavailable),
            patch.object(service, "outage_backoff", backoff),
        ):
            result = await service.on_artist_message(delivery)  # type: ignore[arg-type]
    finally:
        await unavailable.close()

    assert result.settlement is Settlement.REQUEUE
    assert result.outcome == "transient"
    assert result.error_type == "ServiceUnavailable"
    assert delivery.settlements == [("nack", True)]
    assert backoff.consecutive_failures == 1


@pytest.mark.asyncio
async def test_invalid_payload_is_rejected_without_touching_real_graph(neo4j_driver: AsyncDriver) -> None:
    before = await one(neo4j_driver, "MATCH (node) RETURN count(node) AS count")
    delivery = StrictDelivery(dumps({"mbid": "missing-required-id"}))
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", neo4j_driver),
    ):
        result = await service.on_artist_message(delivery)  # type: ignore[arg-type]

    after = await one(neo4j_driver, "MATCH (node) RETURN count(node) AS count")
    assert result == DeliveryResult(Settlement.REJECT, "failed", "ValidationError")
    assert delivery.settlements == [("nack", False)]
    assert after == before
