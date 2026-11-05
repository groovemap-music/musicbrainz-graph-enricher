"""Live Neo4j checks that updating a release's media prunes orphan Medium nodes, and only those.

Medium is a vocabulary node shared with the Discogs enricher, so the prune must never remove a
medium any catalog still uses. Whether a DELETE is visible to the same statement's later
clauses is engine behaviour a mocked transaction cannot show, hence a real Neo4j.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest
import pytest_asyncio
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.exceptions import ClientError

from brainzgraphinator import _projections


pytestmark = pytest.mark.integration


def release_event(*mediums: str) -> dict[str, Any]:
    return {
        "id": "mb-release",
        "discogs_release_id": "r1",
        "media": {"items": [{"medium": m, "family": "optical", "qty": 1} for m in mediums]},
    }


async def run(driver: AsyncDriver, query: str, **parameters: Any) -> list[dict[str, Any]]:
    async with driver.session(database="neo4j") as session:
        result = await session.run(query, **parameters)
        return [dict(record) async for record in result]


@pytest.fixture(params=["single", "batch"])
def path(request: pytest.FixtureRequest) -> str:
    """Both write paths must prune: the per-message statement and the default UNWIND batch."""
    return str(request.param)


async def reconcile(driver: AsyncDriver, record: dict[str, Any], path: str = "single") -> None:
    async with driver.session(database="neo4j") as session:

        async def work(tx: Any) -> None:
            if path == "batch":
                stats = dict.fromkeys(("entities_enriched", "entities_skipped_no_discogs_match"), 0)
                assert await _projections.enrich_releases_batch(tx, [record], stats) == [True]
            else:
                await _projections.reconcile_release_media(tx, "r1", record)

        await session.execute_write(work)


async def media(driver: AsyncDriver) -> list[str]:
    return [row["id"] for row in await run(driver, "MATCH (m:Medium) RETURN m.id AS id ORDER BY id")]


@pytest_asyncio.fixture
async def neo4j_driver() -> AsyncDriver:
    driver = AsyncGraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), os.environ["NEO4J_INTEGRATION_PASSWORD"]),
    )
    await driver.verify_connectivity()
    await run(driver, "MATCH (n) DETACH DELETE n")
    await run(driver, "CREATE CONSTRAINT medium_id IF NOT EXISTS FOR (n:Medium) REQUIRE n.id IS UNIQUE")
    await run(driver, "CREATE CONSTRAINT family_name IF NOT EXISTS FOR (n:MediaFamily) REQUIRE n.name IS UNIQUE")
    await run(driver, "CREATE (:Release {id: 'r1'}), (:Release {id: 'r2'}), (:Release {id: 'r3'})")
    try:
        yield driver
    finally:
        await run(driver, "MATCH (n) DETACH DELETE n")
        await driver.close()


@pytest.mark.asyncio
async def test_a_replaced_medium_is_pruned_and_the_new_one_kept(neo4j_driver: AsyncDriver, path: str) -> None:
    await reconcile(neo4j_driver, release_event("cd", "dvd"), path)
    assert await media(neo4j_driver) == ["cd", "dvd"]

    await reconcile(neo4j_driver, release_event("cd"), path)

    assert await media(neo4j_driver) == ["cd"]
    # The family is a small shared vocabulary node and is never pruned; the pruned medium's
    # IN_FAMILY edge goes with it, so no dangling relationship is left behind.
    assert await run(neo4j_driver, "MATCH (f:MediaFamily) RETURN f.name AS name") == [{"name": "optical"}]
    assert await run(neo4j_driver, "MATCH ()-[e:IN_FAMILY]->() RETURN count(e) AS n") == [{"n": 1}]


@pytest.mark.asyncio
async def test_a_block_with_no_items_clears_the_release_and_its_mediums(neo4j_driver: AsyncDriver, path: str) -> None:
    await reconcile(neo4j_driver, release_event("cd", "dvd"), path)

    await reconcile(neo4j_driver, release_event(), path)

    assert await media(neo4j_driver) == []
    assert await run(neo4j_driver, "MATCH (r:Release {id: 'r1'}) RETURN r.mb_medium_count AS n") == [{"n": 0}]


@pytest.mark.asyncio
async def test_a_medium_another_release_uses_survives(neo4j_driver: AsyncDriver, path: str) -> None:
    await run(neo4j_driver, "MATCH (r:Release {id: 'r2'}) CREATE (r)-[:ISSUED_ON {source: 'musicbrainz'}]->(:Medium {id: 'dvd'})")
    await reconcile(neo4j_driver, release_event("dvd"), path)

    await reconcile(neo4j_driver, release_event(), path)

    assert await media(neo4j_driver) == ["dvd"]


@pytest.mark.asyncio
async def test_a_medium_discogs_still_uses_survives_and_its_edge_is_untouched(neo4j_driver: AsyncDriver, path: str) -> None:
    # Discogs edges to the same medium: from this very release, and from another one.
    await run(
        neo4j_driver,
        "MATCH (a:Release {id: 'r1'}), (b:Release {id: 'r3'}) "
        "CREATE (a)-[:ISSUED_ON {source: 'discogs', qty: 1}]->(m:Medium {id: 'cd'}), "
        "       (b)-[:ISSUED_ON {source: 'discogs', qty: 2}]->(m)",
    )
    await reconcile(neo4j_driver, release_event("cd"), path)
    await reconcile(neo4j_driver, release_event(), path)

    assert await media(neo4j_driver) == ["cd"]
    edges = await run(neo4j_driver, "MATCH (r)-[e:ISSUED_ON]->(:Medium {id: 'cd'}) RETURN r.id AS r, e.source AS s ORDER BY r")
    assert edges == [{"r": "r1", "s": "discogs"}, {"r": "r3", "s": "discogs"}]


@pytest.mark.asyncio
async def test_a_medium_with_any_other_relationship_survives(neo4j_driver: AsyncDriver, path: str) -> None:
    await reconcile(neo4j_driver, release_event("cd"), path)
    await run(neo4j_driver, "MATCH (m:Medium {id: 'cd'}) CREATE (:Referrer)-[:REFERS_TO]->(m)")

    await reconcile(neo4j_driver, release_event(), path)

    assert await media(neo4j_driver) == ["cd"]


@pytest.mark.asyncio
async def test_replaying_the_same_event_changes_nothing(neo4j_driver: AsyncDriver, path: str) -> None:
    await reconcile(neo4j_driver, release_event("cd", "dvd"), path)
    await reconcile(neo4j_driver, release_event("cd", "dvd"), path)

    assert await media(neo4j_driver) == ["cd", "dvd"]


@pytest.mark.asyncio
async def test_one_batch_prunes_for_several_releases_and_spares_shared_mediums(neo4j_driver: AsyncDriver) -> None:
    async with neo4j_driver.session(database="neo4j") as session:

        async def work(tx: Any) -> None:
            await _projections.enrich_releases_batch(
                tx,
                [
                    {**release_event("cd", "dvd"), "discogs_release_id": "r1"},
                    {**release_event("dvd", "vinyl"), "discogs_release_id": "r2"},
                ],
                dict.fromkeys(("entities_enriched", "entities_skipped_no_discogs_match"), 0),
            )

        await session.execute_write(work)
        assert await media(neo4j_driver) == ["cd", "dvd", "vinyl"]

        async def replace(tx: Any) -> None:
            await _projections.enrich_releases_batch(
                tx,
                [
                    {**release_event("cd"), "discogs_release_id": "r1"},
                    {**release_event("vinyl"), "discogs_release_id": "r2"},
                ],
                dict.fromkeys(("entities_enriched", "entities_skipped_no_discogs_match"), 0),
            )

        await session.execute_write(replace)

    # dvd lost both users and is pruned; cd and vinyl are still referenced.
    assert await media(neo4j_driver) == ["cd", "vinyl"]


@pytest.mark.asyncio
async def test_an_edge_attached_while_pruning_is_never_silently_detached(neo4j_driver: AsyncDriver) -> None:
    for attempt in range(15):
        await run(neo4j_driver, "MATCH (m:Medium) DETACH DELETE m")
        await reconcile(neo4j_driver, release_event("cd"), "batch")

        async def attach(driver: AsyncDriver = neo4j_driver) -> None:
            async with driver.session(database="neo4j") as session:

                async def work(tx: Any) -> None:
                    await (
                        await tx.run("MATCH (r:Release {id: 'r3'}) MERGE (m:Medium {id: 'cd'}) MERGE (r)-[:ISSUED_ON {source: 'discogs'}]->(m)")
                    ).consume()

                await session.execute_write(work)

        _, attached = await asyncio.gather(reconcile(neo4j_driver, release_event(), "batch"), attach(), return_exceptions=True)

        # The attacher either wins (its edge and the Medium both exist) or, when the prune
        # committed between its MERGE read and its relationship write, aborts with a clean
        # EntityNotFound that the delivery's requeue retries. It is never a silent loss.
        edges = await run(neo4j_driver, "MATCH (:Release {id: 'r3'})-[e:ISSUED_ON]->(m:Medium {id: 'cd'}) RETURN count(e) AS n")
        if attached is None:
            assert edges == [{"n": 1}], f"attempt {attempt}: attached edge was lost"
        else:
            assert isinstance(attached, ClientError), attached
            assert edges == [{"n": 0}], f"attempt {attempt}: aborted attach left an edge"
        await run(neo4j_driver, "MATCH (:Release {id: 'r3'})-[e:ISSUED_ON]->() DELETE e")


@pytest.mark.asyncio
async def test_concurrent_batches_sharing_vocabulary_never_deadlock(neo4j_driver: AsyncDriver) -> None:
    """Many small concurrent flushes over one shared vocabulary take their locks in one order.

    Each worker runs a raw transaction (no driver retry, so a DeadlockDetected surfaces) that
    rewrites its releases' media, detaching what the previous round attached so the prune path
    contends too. Releases are disjoint per worker; only the Medium/MediaFamily nodes are shared.
    """
    mediums = [(f"medium_{i:02d}", f"family_{i % 4}") for i in range(12)]
    workers = 12
    await run(neo4j_driver, "UNWIND range(0, $n - 1) AS i CREATE (:Release {id: 'w' + toString(i)})", n=workers)

    def event(worker: int, round_: int) -> dict[str, Any]:
        # Each worker takes a different slice, walked in a different direction per worker, so
        # unordered locking would cross; every round shifts the slice so stale edges are pruned.
        picked = [mediums[(worker * 3 + round_ * 5 + k) % len(mediums)] for k in range(4)]
        if worker % 2:
            picked.reverse()
        return {
            "id": f"mb-w{worker}",
            "discogs_release_id": f"w{worker}",
            "media": {"items": [{"medium": m, "family": f, "qty": 1} for m, f in picked]},
        }

    async def worker_run(worker: int) -> None:
        for round_ in range(8):
            async with neo4j_driver.session(database="neo4j") as session:
                tx = await session.begin_transaction()
                stats = dict.fromkeys(("entities_enriched", "entities_skipped_no_discogs_match"), 0)
                await _projections.enrich_releases_batch(tx, [event(worker, round_)], stats)
                await tx.commit()

    results = await asyncio.gather(*(worker_run(w) for w in range(workers)), return_exceptions=True)

    deadlocks = [r for r in results if isinstance(r, ClientError) or (isinstance(r, Exception) and "DeadlockDetected" in str(r))]
    assert not deadlocks, deadlocks
    assert all(not isinstance(r, Exception) for r in results), results
    # Every worker's final slice is intact and nothing is attached to a missing family.
    edges = await run(neo4j_driver, "MATCH (:Release)-[e:ISSUED_ON]->(:Medium) RETURN count(e) AS n")
    assert edges == [{"n": workers * 4}]
    assert await run(neo4j_driver, "MATCH (m:Medium) WHERE NOT (m)-[:IN_FAMILY]->() RETURN count(m) AS n") == [{"n": 0}]
