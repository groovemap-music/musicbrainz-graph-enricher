"""Measure enrichment throughput, per-message versus batched, against a real Neo4j.

Reads NEO4J_URI / NEO4J_INTEGRATION_USER / NEO4J_INTEGRATION_PASSWORD (set by bench-batching.sh)
and drives the real message handlers with at most PREFETCH deliveries in flight per data type,
as RabbitMQ does with the service's per-consumer prefetch of 200. The graph is reseeded identically before each mode.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any
from unittest.mock import patch

from neo4j import AsyncGraphDatabase
from orjson import dumps

import brainzgraphinator.brainzgraphinator as service


NODES = int(os.environ.get("BENCH_NODES", "20000"))
MESSAGES = int(os.environ.get("BENCH_MESSAGES", "4000"))
PREFETCH = 200


class Delivery:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = dumps(body)
        self.headers: dict[str, Any] = {}

    async def ack(self) -> None: ...

    async def nack(self, *, requeue: bool) -> None:
        raise AssertionError(f"unexpected nack requeue={requeue}")


def workload() -> list[tuple[Any, Delivery]]:
    """A mix of the four entity kinds; artists carry relations, releases carry media."""
    work: list[tuple[Any, Delivery]] = []
    for i in range(MESSAGES):
        kind = i % 4
        n = i % NODES
        if kind == 0:
            relations = [
                {"type": "member of band", "target_discogs_artist_id": str((n + 1) % NODES)},
                {"type": "collaboration", "target_discogs_artist_id": str((n + 7) % NODES)},
                {"type": "teacher", "target_discogs_artist_id": str((n + 13) % NODES), "direction": "backward"},
            ]
            body = {"id": f"mb-a{i}", "discogs_artist_id": str(n), "type": "Group", "relations": relations}
            work.append((service.on_artist_message, Delivery(body)))
        elif kind == 1:
            work.append((service.on_label_message, Delivery({"id": f"mb-l{i}", "discogs_label_id": str(n), "label_code": n})))
        elif kind == 2:
            body = {"id": f"mb-m{i}", "discogs_master_id": str(n), "secondary_types": ["Live"]}
            work.append((service.on_release_group_message, Delivery(body)))
        else:
            body = {
                "id": f"mb-r{i}",
                "discogs_release_id": str(n),
                "barcode": str(n),
                "release_events": [{"date": "1969", "area_name": "UK"}],
                "media": {"items": [{"medium": "cd", "family": "optical", "qty": 1}, {"medium": "vinyl-12", "family": "analog", "qty": 1}]},
            }
            work.append((service.on_release_message, Delivery(body)))
    return work


async def seed(driver: Any) -> None:
    async with driver.session(database="neo4j") as session:
        await (await session.run("MATCH (n) DETACH DELETE n")).consume()
        for label in ("Artist", "Label", "Master", "Release"):
            await (await session.run(f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")).consume()
        for label in ("Medium",):
            await (await session.run(f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")).consume()
        await (await session.run("CREATE CONSTRAINT family_name IF NOT EXISTS FOR (n:MediaFamily) REQUIRE n.name IS UNIQUE")).consume()
        for label in ("Artist", "Label", "Master", "Release"):
            await (await session.run(f"UNWIND range(0, $n - 1) AS i CREATE (:{label} {{id: toString(i)}})", n=NODES)).consume()


async def run_mode(driver: Any, batched: bool) -> dict[str, float]:
    await seed(driver)
    work = workload()
    for key in service.enrichment_stats:
        service.enrichment_stats[key] = 0
    # RabbitMQ prefetch applies per consumer, and the service runs one consumer per data type.
    gates: dict[Any, asyncio.Semaphore] = {}

    async def one(handler: Any, delivery: Delivery) -> None:
        async with gates.setdefault(handler, asyncio.Semaphore(PREFETCH)):
            result = await handler(delivery)
            assert result.settlement.name == "ACK", result

    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", driver),
        patch.object(service, "BATCH_MODE", batched),
    ):
        started = time.perf_counter()
        await asyncio.gather(*(one(handler, delivery) for handler, delivery in work))
        elapsed = time.perf_counter() - started
    async with driver.session(database="neo4j") as session:
        record = await (await session.run("MATCH (n) WHERE n.mbid IS NOT NULL RETURN count(n) AS enriched")).single()
        edges = await (await session.run("MATCH ()-[r {source: 'musicbrainz'}]->() RETURN count(r) AS edges")).single()
    return {
        "seconds": elapsed,
        "msgs_per_s": MESSAGES / elapsed,
        "ms_per_msg": 1000 * elapsed / MESSAGES,
        "enriched_nodes": float(record["enriched"]),
        "mb_edges": float(edges["edges"]),
    }


async def main() -> int:
    driver = AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
    )
    await driver.verify_connectivity()
    try:
        print(f"nodes per label={NODES} messages={MESSAGES} prefetch={PREFETCH}")
        before = await run_mode(driver, batched=False)
        after = await run_mode(driver, batched=True)
        await asyncio.sleep(0)
    finally:
        await driver.close()
    for name, result in (("per-message", before), ("batched", after)):
        print(name, {key: round(value, 2) for key, value in result.items()})
    print(
        f"speedup {before['seconds'] / after['seconds']:.1f}x; same graph: {before['enriched_nodes'] == after['enriched_nodes'] and before['mb_edges'] == after['mb_edges']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
