"""Live Neo4j checks that updating a release's media prunes orphan Medium nodes, and only those.

Medium is a vocabulary node shared with the Discogs enricher, so the prune must never remove a
medium any catalog still uses. Whether a DELETE is visible to the same statement's later
clauses is engine behaviour a mocked transaction cannot show, hence a real Neo4j.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
import pytest_asyncio
from neo4j import AsyncDriver, AsyncGraphDatabase

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


async def reconcile(driver: AsyncDriver, record: dict[str, Any]) -> None:
    async with driver.session(database="neo4j") as session:

        async def work(tx: Any) -> None:
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
async def test_a_replaced_medium_is_pruned_and_the_new_one_kept(neo4j_driver: AsyncDriver) -> None:
    await reconcile(neo4j_driver, release_event("cd", "dvd"))
    assert await media(neo4j_driver) == ["cd", "dvd"]

    await reconcile(neo4j_driver, release_event("cd"))

    assert await media(neo4j_driver) == ["cd"]
    # The family is a small shared vocabulary node and is never pruned; the pruned medium's
    # IN_FAMILY edge goes with it, so no dangling relationship is left behind.
    assert await run(neo4j_driver, "MATCH (f:MediaFamily) RETURN f.name AS name") == [{"name": "optical"}]
    assert await run(neo4j_driver, "MATCH ()-[e:IN_FAMILY]->() RETURN count(e) AS n") == [{"n": 1}]


@pytest.mark.asyncio
async def test_a_block_with_no_items_clears_the_release_and_its_mediums(neo4j_driver: AsyncDriver) -> None:
    await reconcile(neo4j_driver, release_event("cd", "dvd"))

    await reconcile(neo4j_driver, release_event())

    assert await media(neo4j_driver) == []
    assert await run(neo4j_driver, "MATCH (r:Release {id: 'r1'}) RETURN r.mb_medium_count AS n") == [{"n": 0}]


@pytest.mark.asyncio
async def test_a_medium_another_release_uses_survives(neo4j_driver: AsyncDriver) -> None:
    await run(neo4j_driver, "MATCH (r:Release {id: 'r2'}) CREATE (r)-[:ISSUED_ON {source: 'musicbrainz'}]->(:Medium {id: 'dvd'})")
    await reconcile(neo4j_driver, release_event("dvd"))

    await reconcile(neo4j_driver, release_event())

    assert await media(neo4j_driver) == ["dvd"]


@pytest.mark.asyncio
async def test_a_medium_discogs_still_uses_survives_and_its_edge_is_untouched(neo4j_driver: AsyncDriver) -> None:
    # Discogs edges to the same medium: from this very release, and from another one.
    await run(
        neo4j_driver,
        "MATCH (a:Release {id: 'r1'}), (b:Release {id: 'r3'}) "
        "CREATE (a)-[:ISSUED_ON {source: 'discogs', qty: 1}]->(m:Medium {id: 'cd'}), "
        "       (b)-[:ISSUED_ON {source: 'discogs', qty: 2}]->(m)",
    )
    await reconcile(neo4j_driver, release_event("cd"))
    await reconcile(neo4j_driver, release_event())

    assert await media(neo4j_driver) == ["cd"]
    edges = await run(neo4j_driver, "MATCH (r)-[e:ISSUED_ON]->(:Medium {id: 'cd'}) RETURN r.id AS r, e.source AS s ORDER BY r")
    assert edges == [{"r": "r1", "s": "discogs"}, {"r": "r3", "s": "discogs"}]


@pytest.mark.asyncio
async def test_a_medium_with_any_other_relationship_survives(neo4j_driver: AsyncDriver) -> None:
    await reconcile(neo4j_driver, release_event("cd"))
    await run(neo4j_driver, "MATCH (m:Medium {id: 'cd'}) CREATE (:Referrer)-[:REFERS_TO]->(m)")

    await reconcile(neo4j_driver, release_event())

    assert await media(neo4j_driver) == ["cd"]


@pytest.mark.asyncio
async def test_replaying_the_same_event_changes_nothing(neo4j_driver: AsyncDriver) -> None:
    await reconcile(neo4j_driver, release_event("cd", "dvd"))
    await reconcile(neo4j_driver, release_event("cd", "dvd"))

    assert await media(neo4j_driver) == ["cd", "dvd"]
