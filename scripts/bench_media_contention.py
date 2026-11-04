"""Measure lock contention on shared Medium / MediaFamily nodes under concurrent release flushes.

Releases write MERGEs onto a small shared vocabulary (a few dozen Medium nodes filed under 12
MediaFamily nodes), so concurrent flushes queue on those nodes' locks and can deadlock. This
drives the real release handler with a realistic skewed medium mix at several flush
concurrencies and counts what the Neo4j driver had to retry (lock timeouts and deadlocks).

Reads NEO4J_URI / NEO4J_INTEGRATION_USER / NEO4J_INTEGRATION_PASSWORD (set by bench-contention.sh).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import sys
import time
from typing import Any
from unittest.mock import patch

from neo4j import AsyncGraphDatabase
from orjson import dumps

import brainzgraphinator.brainzgraphinator as service


RELEASES = int(os.environ.get("BENCH_RELEASES", "3000"))
FAMILIES = 12
# (medium id, family, weight): a realistic skew where a few formats dominate.
VOCAB = [(f"medium_{i:02d}", f"family_{i % FAMILIES:02d}", 1.0 / (i + 1) ** 1.2) for i in range(36)]


class Delivery:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = dumps(body)
        self.headers: dict[str, Any] = {}

    async def ack(self) -> None: ...

    async def nack(self, *, requeue: bool) -> None:
        raise AssertionError(f"unexpected nack requeue={requeue}")


class RetryCounter(logging.Handler):
    """Counts driver-level transaction retries by Neo4j error code."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.codes: dict[str, int] = {}

    def emit(self, record: logging.LogRecord) -> None:
        text = record.getMessage()
        if "will be retried" not in text:
            return
        code = "Deadlock" if "DeadlockDetected" in text else "LockClient" if "LockClient" in text else "other"
        self.codes[code] = self.codes.get(code, 0) + 1


def workload(seed: int) -> list[Delivery]:
    rng = random.Random(seed)  # noqa: S311 - reproducible benchmark data, not security
    weights = [weight for _, _, weight in VOCAB]
    deliveries = []
    for i in range(RELEASES):
        picked = {rng.choices(range(len(VOCAB)), weights)[0] for _ in range(rng.choice([1, 1, 2, 3]))}
        items = [{"medium": VOCAB[p][0], "family": VOCAB[p][1], "qty": 1} for p in sorted(picked)]
        deliveries.append(Delivery({"id": f"mb-r{i}", "discogs_release_id": str(i), "media": {"items": items}}))
    return deliveries


async def run_case(driver: Any, *, window: int, batch_size: int, parallel: bool) -> dict[str, Any]:
    async with driver.session(database="neo4j") as session:
        await (await session.run("MATCH (n) DETACH DELETE n")).consume()
        for statement in (
            "CREATE CONSTRAINT release_id IF NOT EXISTS FOR (n:Release) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT medium_id IF NOT EXISTS FOR (n:Medium) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT family_name IF NOT EXISTS FOR (n:MediaFamily) REQUIRE n.name IS UNIQUE",
        ):
            await (await session.run(statement)).consume()
        await (await session.run("UNWIND range(0, $n - 1) AS i CREATE (:Release {id: toString(i)})", n=RELEASES)).consume()

    deliveries = workload(seed=7)
    counter = RetryCounter()
    neo4j_logger = logging.getLogger("neo4j")
    neo4j_logger.addHandler(counter)
    gate = asyncio.Semaphore(window)

    async def one(delivery: Delivery) -> None:
        async with gate:
            result = await service.on_release_message(delivery)
            assert result.settlement.name == "ACK", result

    service._write_batchers.clear()

    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", driver),
        patch.object(service, "BATCH_MODE", True),
        patch.object(service, "BATCH_SIZE", batch_size),
        patch.object(service, "SERIALIZE_FLUSHES", not parallel),
    ):
        started = time.perf_counter()
        await asyncio.gather(*(one(d) for d in deliveries))
        elapsed = time.perf_counter() - started
    neo4j_logger.removeHandler(counter)

    async with driver.session(database="neo4j") as session:
        edges = (await (await session.run("MATCH ()-[e:ISSUED_ON]->() RETURN count(e) AS n")).single())["n"]
        families = (await (await session.run("MATCH (f:MediaFamily) RETURN count(f) AS n")).single())["n"]
        dupes = (await (await session.run("MATCH (m:Medium) WITH m.id AS id, count(*) AS c WHERE c > 1 RETURN count(*) AS n")).single())["n"]
    return {
        "seconds": round(elapsed, 2),
        "rel_per_s": round(RELEASES / elapsed),
        "retries": dict(counter.codes),
        "edges": edges,
        "families": families,
        "duplicate_mediums": dupes,
    }


async def main() -> int:
    driver = AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
    )
    await driver.verify_connectivity()
    shapes = [("prod (prefetch 200, batch 100)", 200, 100), ("stress (prefetch 1000, batch 20)", 1000, 20)]
    cases = [
        (f"{name}, {'parallel flushes' if parallel else 'one flush at a time'}", {"window": w, "batch_size": b, "parallel": parallel})
        for _ in range(2)
        for name, w, b in shapes
        for parallel in (True, False)
    ]
    try:
        print(f"releases={RELEASES} mediums={len(VOCAB)} families={FAMILIES}")
        await run_case(driver, window=200, batch_size=100, parallel=False)  # warm-up, discarded
        for name, options in cases:
            print(name, await run_case(driver, **options))
    finally:
        await driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
