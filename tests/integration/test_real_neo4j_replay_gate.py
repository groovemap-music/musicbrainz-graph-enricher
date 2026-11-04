"""Live Neo4j checks that replaying unchanged MusicBrainz data writes nothing.

The gate is a stored digest compared inside the statement itself, so only a real Neo4j can show
that a matching digest really skips the SET (and the relationship and media writes behind it).
"""

from __future__ import annotations

import asyncio
import os
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from common import Settlement
from neo4j import AsyncDriver, AsyncGraphDatabase
from orjson import dumps

import brainzgraphinator.brainzgraphinator as service


pytestmark = pytest.mark.integration


class Delivery:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = dumps(body)
        self.headers: dict[str, Any] = {}

    async def ack(self) -> None: ...

    async def nack(self, *, requeue: bool) -> None:
        raise AssertionError(f"unexpected nack requeue={requeue}")


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
    await run(driver, "MATCH (n) DETACH DELETE n")
    for label in ("Artist", "Label", "Master", "Release", "Medium"):
        await run(driver, f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")
    await run(driver, "CREATE CONSTRAINT family_name IF NOT EXISTS FOR (n:MediaFamily) REQUIRE n.name IS UNIQUE")
    await run(
        driver,
        "CREATE (:Artist {id: 'a1'}), (:Artist {id: 'a2'}), (:Label {id: 'l1'}), (:Master {id: 'm1'}), (:Release {id: 'r1'})",
    )
    try:
        yield driver
    finally:
        await run(driver, "MATCH (n) DETACH DELETE n")
        await driver.close()


@pytest.fixture(params=[True, False], ids=["batched", "per-message"])
def batched(request: pytest.FixtureRequest) -> bool:
    return bool(request.param)


def messages(**overrides: Any) -> list[tuple[Any, Delivery]]:
    artist = {
        "id": "mb-a1",
        "discogs_artist_id": "a1",
        "type": "Group",
        "relations": [{"type": "member of band", "target_discogs_artist_id": "a2"}],
    }
    release = {
        "id": "mb-r1",
        "discogs_release_id": "r1",
        "barcode": "123",
        "media": {"items": [{"medium": "cd", "family": "optical", "qty": 1}]},
    }
    artist.update(overrides.get("artist", {}))
    release.update(overrides.get("release", {}))
    return [
        (service.on_artist_message, Delivery(artist)),
        (service.on_label_message, Delivery({"id": "mb-l1", "discogs_label_id": "l1", "label_code": 7})),
        (service.on_release_group_message, Delivery({"id": "mb-m1", "discogs_master_id": "m1"})),
        (service.on_release_message, Delivery(release)),
        (service.on_artist_message, Delivery({"id": "mb-x", "discogs_artist_id": "unmatched"})),
    ]


async def deliver(driver: AsyncDriver, batched: bool, work: list[tuple[Any, Delivery]]) -> list[Any]:
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "graph", driver),
        patch.object(service, "BATCH_MODE", batched),
    ):
        return list(await asyncio.gather(*(handler(delivery) for handler, delivery in work)))


async def stamps(driver: AsyncDriver) -> dict[str, Any]:
    rows = await run(driver, "MATCH (n) WHERE n.mb_updated_at IS NOT NULL RETURN n.id AS id, n.mb_updated_at AS ts, n.mb_hash AS hash")
    return {row["id"]: (row["ts"], row["hash"]) for row in rows}


@pytest.mark.asyncio
async def test_replay_of_unchanged_data_rewrites_no_node(neo4j_driver: AsyncDriver, batched: bool) -> None:
    await deliver(neo4j_driver, batched, messages())
    first = await stamps(neo4j_driver)
    assert set(first) == {"a1", "l1", "m1", "r1"}
    assert all(hash_.startswith("v1:") for _, hash_ in first.values())

    await asyncio.sleep(0.01)  # a rewrite would take a later wall-clock stamp
    service.enrichment_stats["replays_skipped_unchanged"] = 0
    results = await deliver(neo4j_driver, batched, messages())

    assert await stamps(neo4j_driver) == first
    assert service.enrichment_stats["replays_skipped_unchanged"] == 4
    assert [r.outcome for r in results] == ["processed", "processed", "processed", "processed", "skipped"]
    assert all(r.settlement is Settlement.ACK for r in results)


@pytest.mark.asyncio
async def test_a_changed_payload_is_written_with_its_relationships_and_media(neo4j_driver: AsyncDriver, batched: bool) -> None:
    await deliver(neo4j_driver, batched, messages())
    first = await stamps(neo4j_driver)
    await asyncio.sleep(0.01)

    changed = messages(
        artist={"type": "Person", "relations": [{"type": "teacher", "target_discogs_artist_id": "a2"}]},
        release={"media": {"items": [{"medium": "dvd", "family": "optical", "qty": 2}]}},
    )
    await deliver(neo4j_driver, batched, changed)
    second = await stamps(neo4j_driver)

    assert second["a1"] != first["a1"]
    assert second["r1"] != first["r1"]
    assert second["l1"] == first["l1"]
    assert second["m1"] == first["m1"]
    assert await run(neo4j_driver, "MATCH (:Artist {id: 'a1'})-[e:TAUGHT]->(:Artist {id: 'a2'}) RETURN count(e) AS n") == [{"n": 1}]
    assert await run(neo4j_driver, "MATCH (a:Artist {id: 'a1'}) RETURN a.mb_type AS t") == [{"t": "Person"}]
    assert await run(neo4j_driver, "MATCH (:Release {id: 'r1'})-[e:ISSUED_ON {source: 'musicbrainz'}]->(m) RETURN m.id AS m, e.qty AS q") == [
        {"m": "dvd", "q": 2}
    ]


@pytest.mark.asyncio
async def test_a_node_enriched_before_the_gate_is_rewritten_once(neo4j_driver: AsyncDriver, batched: bool) -> None:
    await run(neo4j_driver, "MATCH (a:Artist {id: 'a1'}) SET a.mbid = 'mb-a1', a.mb_updated_at = 'legacy'")

    await deliver(neo4j_driver, batched, messages())
    migrated = (await stamps(neo4j_driver))["a1"]
    assert migrated[0] != "legacy"
    assert migrated[1].startswith("v1:")

    await asyncio.sleep(0.01)
    await deliver(neo4j_driver, batched, messages())
    assert (await stamps(neo4j_driver))["a1"] == migrated
