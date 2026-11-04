"""Live Neo4j checks for cross-message UNWIND batching.

Batching must leave the graph exactly as per-message delivery would, settle every delivery from
its own result, and isolate a poison record. A mocked transaction can hide a bad UNWIND, so each
check runs the real handler against the disposable Neo4j started by the integration recipe.
"""

from __future__ import annotations

import asyncio
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

COUNT = 40


class Delivery:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = dumps(body)
        self.headers: dict[str, Any] = {}
        self.settlements: list[tuple[str, bool | None]] = []

    async def ack(self) -> None:
        assert not self.settlements, "settled twice"
        self.settlements.append(("ack", None))

    async def nack(self, *, requeue: bool) -> None:
        assert not self.settlements, "settled twice"
        self.settlements.append(("nack", requeue))


async def run(driver: AsyncDriver, query: str, **parameters: Any) -> list[dict[str, Any]]:
    async with driver.session(database="neo4j") as session:
        result = await session.run(query, **parameters)
        return [dict(record) async for record in result]


@pytest_asyncio.fixture
async def neo4j_driver() -> AsyncDriver:
    driver = AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
    )
    await driver.verify_connectivity()
    for statement in (
        "CREATE CONSTRAINT artist_id IF NOT EXISTS FOR (n:Artist) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT release_id IF NOT EXISTS FOR (n:Release) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT label_id IF NOT EXISTS FOR (n:Label) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT master_id IF NOT EXISTS FOR (n:Master) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT medium_id IF NOT EXISTS FOR (n:Medium) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT family_name IF NOT EXISTS FOR (n:MediaFamily) REQUIRE n.name IS UNIQUE",
    ):
        await run(driver, statement)
    try:
        yield driver
    finally:
        await run(driver, "MATCH (node) DETACH DELETE node")
        await driver.close()


async def seed(driver: AsyncDriver) -> None:
    await run(driver, "MATCH (node) DETACH DELETE node")
    await run(
        driver,
        "UNWIND range(0, $n - 1) AS i "
        "CREATE (:Artist {id: 'a' + i}) CREATE (:Label {id: 'l' + i}) "
        "CREATE (:Master {id: 'm' + i}) CREATE (:Release {id: 'r' + i})",
        n=COUNT,
    )
    # A stale media edge that the reconcile must delete.
    await run(
        driver,
        "MATCH (r:Release {id: 'r0'}) CREATE (r)-[:ISSUED_ON {source: 'musicbrainz'}]->(:Medium {id: 'stale-medium'})",
    )


def messages() -> list[tuple[Any, Delivery]]:
    out: list[tuple[Any, Delivery]] = []
    for i in range(COUNT):
        out.append(
            (
                service.on_artist_message,
                Delivery(
                    {
                        "id": f"mb-a{i}",
                        "discogs_artist_id": f"a{i}" if i % 10 else None,
                        "type": "Person",
                        "relations": [
                            {"type": "member of band", "target_discogs_artist_id": f"a{(i + 1) % COUNT}", "direction": "forward"},
                            {"type": "teacher", "target_discogs_artist_id": f"a{(i + 2) % COUNT}", "direction": "backward"},
                            {"type": "collaboration", "target_discogs_artist_id": "missing"},
                            {"type": "unsupported"},
                        ],
                    }
                ),
            )
        )
        out.append((service.on_label_message, Delivery({"id": f"mb-l{i}", "discogs_label_id": f"l{i}", "label_code": i})))
        out.append(
            (
                service.on_release_group_message,
                Delivery(
                    {
                        "id": f"mb-m{i}",
                        "discogs_master_id": f"m{i}" if i % 7 else "unmatched",
                        "secondary_types": ["Live"],
                    }
                ),
            )
        )
        out.append(
            (
                service.on_release_message,
                Delivery(
                    {
                        "id": f"mb-r{i}",
                        "discogs_release_id": f"r{i}",
                        "barcode": str(i),
                        "release_events": [{"date": "1969", "area_name": "UK"}],
                        "media": {
                            "items": [{"medium": f"cd{i % 3}", "family": "optical", "qty": 1}, {"medium": "vinyl-12", "family": "analog", "qty": 2}]
                        },
                    }
                ),
            )
        )
    return out


async def deliver(batched: bool, deliveries: list[tuple[Any, Delivery]], driver: AsyncDriver) -> list[DeliveryResult]:
    for key in service.enrichment_stats:
        service.enrichment_stats[key] = 0
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", driver),
        patch.object(service, "BATCH_MODE", batched),
    ):
        return list(await asyncio.gather(*(handler(delivery) for handler, delivery in deliveries)))


async def graph_state(driver: AsyncDriver) -> dict[str, Any]:
    nodes = await run(driver, "MATCH (n) RETURN labels(n) AS labels, properties(n) AS p")
    normalised = sorted((str(row["labels"]), str(sorted((k, str(v)) for k, v in row["p"].items() if k != "mb_updated_at"))) for row in nodes)
    edges = await run(
        driver,
        "MATCH (a)-[r]->(b) RETURN labels(a) AS al, a.id AS a, type(r) AS t, properties(r) AS p, labels(b) AS bl, coalesce(b.id, b.name) AS b",
    )
    return {
        "nodes": normalised,
        "edges": sorted(str((r["al"], r["a"], r["t"], sorted(r["p"].items()), r["bl"], r["b"])) for r in edges),
    }


@pytest.mark.asyncio
async def test_batched_delivery_leaves_the_same_graph_and_stats_as_per_message(neo4j_driver: AsyncDriver) -> None:
    await seed(neo4j_driver)
    per_message = await deliver(False, messages(), neo4j_driver)
    expected_graph = await graph_state(neo4j_driver)
    expected_stats = dict(service.enrichment_stats)

    await seed(neo4j_driver)
    batch_deliveries = messages()
    batched = await deliver(True, batch_deliveries, neo4j_driver)

    assert batched == per_message
    assert await graph_state(neo4j_driver) == expected_graph
    assert dict(service.enrichment_stats) == expected_stats
    assert all(d.settlements == [("ack", None)] for _, d in batch_deliveries)
    # The stale ISSUED_ON edge on r0 was reconciled away in the batch as well.
    assert await run(neo4j_driver, "MATCH (:Release {id: 'r0'})-[e:ISSUED_ON]->(:Medium {id: 'stale-medium'}) RETURN e") == []


@pytest.mark.asyncio
async def test_concurrent_deliveries_share_transactions(neo4j_driver: AsyncDriver) -> None:
    await seed(neo4j_driver)
    sizes: list[int] = []

    def record(_entity: str, size: int, _duration: float, _outcome: str) -> None:
        sizes.append(size)

    with patch.object(service, "_record_batch_flush", record):
        await deliver(True, messages(), neo4j_driver)
    assert sum(sizes) == COUNT * 4
    assert len(sizes) <= 8, f"expected a few flushes, got {sizes}"


@pytest.mark.asyncio
async def test_poison_record_fails_alone(neo4j_driver: AsyncDriver) -> None:
    await seed(neo4j_driver)
    good = [Delivery({"id": f"mb-l{i}", "discogs_label_id": f"l{i}", "label_code": i}) for i in range(5)]
    # Neo4j rejects a map as a property value, which fails the whole UNWIND statement.
    poison = Delivery({"id": "mb-bad", "discogs_label_id": "l5", "area": {"nested": "map"}})
    results = await deliver(True, [(service.on_label_message, d) for d in [*good[:2], poison, *good[2:]]], neo4j_driver)

    assert [r.settlement for r in results].count(Settlement.ACK) == 5
    assert results[2].settlement is Settlement.REQUEUE
    assert [d.settlements for d in good] == [[("ack", None)]] * 5
    assert poison.settlements == [("nack", True)]
    stored = await run(neo4j_driver, "MATCH (l:Label) WHERE l.mbid IS NOT NULL RETURN l.id AS id ORDER BY id")
    assert [r["id"] for r in stored] == [f"l{i}" for i in range(5)]


@pytest.mark.asyncio
async def test_transient_outage_requeues_every_member() -> None:
    unavailable = AsyncGraphDatabase.driver("bolt://127.0.0.1:1", auth=("neo4j", "disposable"), connection_timeout=0.1, max_transaction_retry_time=0)
    deliveries = [Delivery({"id": f"mb-a{i}", "discogs_artist_id": f"a{i}"}) for i in range(5)]
    try:
        with patch.object(service, "outage_backoff", OutageBackoff("integration", initial_delay=0, max_delay=0)):
            results = await deliver(True, [(service.on_artist_message, d) for d in deliveries], unavailable)  # type: ignore[arg-type]
    finally:
        await unavailable.close()
    assert all(r.settlement is Settlement.REQUEUE for r in results)
    assert all(d.settlements == [("nack", True)] for d in deliveries)
