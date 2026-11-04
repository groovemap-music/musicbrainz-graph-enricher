"""Unit tests for cross-message write batching (mocked driver; real Neo4j is in tests/integration)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import Settlement
from neo4j.exceptions import ServiceUnavailable
from orjson import dumps

import brainzgraphinator.brainzgraphinator as service
from brainzgraphinator import _projections
from tests.neo4j_doubles import execute_write_with, neo4j_driver, neo4j_session


pytestmark = pytest.mark.asyncio


class Rows:
    """An awaitable-iterable query result, like neo4j.AsyncResult."""

    def __init__(self, rows: list[dict[str, Any]], *, created: int = 0) -> None:
        self.rows = rows
        summary = MagicMock()
        summary.counters.relationships_created = created
        self.consume = AsyncMock(return_value=summary)
        self.single = AsyncMock(return_value=rows[0] if rows else None)

    def __aiter__(self) -> Any:
        async def gen() -> Any:
            for row in self.rows:
                yield row

        return gen()


class Delivery:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = dumps(body)
        self.headers: dict[str, Any] = {}
        self.settlements: list[tuple[str, bool | None]] = []

    async def ack(self) -> None:
        self.settlements.append(("ack", None))

    async def nack(self, *, requeue: bool) -> None:
        self.settlements.append(("nack", requeue))


@pytest.fixture(autouse=True)
def batching(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(service, "BATCH_MODE", True)
    monkeypatch.setattr(service, "shutdown_requested", False)
    service._write_batchers.clear()
    yield
    service._write_batchers.clear()


def graph_with(tx: AsyncMock) -> Any:
    return neo4j_driver(session=neo4j_session(transaction=tx))


async def test_concurrent_deliveries_share_one_unwind_transaction() -> None:
    tx = AsyncMock()
    tx.run.return_value = Rows([{"idx": 0, "changed": True}, {"idx": 1, "changed": True}])
    graph = graph_with(tx)
    deliveries = [Delivery({"id": f"mb{i}", "discogs_label_id": str(i)}) for i in (1, 2, 3)]

    with patch.object(service, "graph", graph):
        results = await asyncio.gather(*(service.on_label_message(d) for d in deliveries))

    assert graph.session.return_value.execute_write.await_count == 1
    assert tx.run.await_count == 1
    assert [r.outcome for r in results] == ["processed", "processed", "skipped"]
    assert all(r.settlement is Settlement.ACK for r in results)
    assert [d.settlements for d in deliveries] == [[("ack", None)]] * 3


async def test_full_batch_flushes_without_waiting_for_linger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service, "BATCH_SIZE", 2)
    monkeypatch.setattr(service, "BATCH_LINGER", 60.0)
    tx = AsyncMock()
    tx.run.return_value = Rows([{"idx": 0, "changed": True}, {"idx": 1, "changed": True}])
    deliveries = [Delivery({"id": f"mb{i}", "discogs_label_id": str(i)}) for i in (1, 2)]

    with patch.object(service, "graph", graph_with(tx)):
        results = await asyncio.wait_for(asyncio.gather(*(service.on_label_message(d) for d in deliveries)), 5)

    assert [r.outcome for r in results] == ["processed", "processed"]


async def test_transient_outage_fails_every_member_without_replay() -> None:
    session = neo4j_session()
    session.execute_write.side_effect = ServiceUnavailable("down")
    deliveries = [Delivery({"id": f"mb{i}", "discogs_label_id": str(i)}) for i in (1, 2, 3)]

    with (
        patch.object(service, "graph", neo4j_driver(session=session)),
        patch.object(service, "wait_before_requeue", AsyncMock()),
    ):
        results = await asyncio.gather(*(service.on_label_message(d) for d in deliveries))

    assert session.execute_write.await_count == 1
    assert all(r.settlement is Settlement.REQUEUE and r.outcome == "transient" for r in results)


async def test_poison_record_is_replayed_alone() -> None:
    calls: list[list[dict[str, Any]]] = []

    async def run(_query: str, **parameters: Any) -> Rows:
        # The batch statement takes `rows`; the per-record replay statement takes `discogs_id`.
        if "rows" in parameters:
            calls.append(parameters["rows"])
            raise ValueError("bad row")
        calls.append([parameters])
        if parameters["discogs_id"] == "2":
            raise ValueError("bad row")
        return Rows([{"matched_id": parameters["discogs_id"]}])

    tx = AsyncMock()
    tx.run.side_effect = run
    deliveries = [Delivery({"id": f"mb{i}", "discogs_label_id": str(i)}) for i in (1, 2, 3)]

    with patch.object(service, "graph", graph_with(tx)):
        results = await asyncio.gather(*(service.on_label_message(d) for d in deliveries))

    assert [r.settlement for r in results] == [Settlement.ACK, Settlement.REQUEUE, Settlement.ACK]
    assert [len(rows) for rows in calls] == [3, 1, 1, 1]
    assert deliveries[1].settlements == [("nack", True)]


async def test_releases_batch_projects_media_once_for_the_batch() -> None:
    tx = AsyncMock()
    tx.run.side_effect = [Rows([{"idx": 0, "changed": True}, {"idx": 1, "changed": True}]), Rows([]), Rows([])]
    media = {"items": [{"medium": "cd", "family": "optical", "qty": 1}]}
    records = [
        {"id": "a", "discogs_release_id": 10, "media": media},
        {"id": "b", "discogs_release_id": 11, "media": media},
        {"id": "c", "discogs_release_id": 12, "media": media},
    ]
    stats = dict.fromkeys(service.enrichment_stats, 0)

    flags = await _projections.enrich_releases_batch(tx, records, stats)

    assert flags == [True, True, False]
    assert tx.run.await_count == 3
    summary_rows = tx.run.await_args_list[1].kwargs["rows"]
    assert [row["discogs_id"] for row in summary_rows] == ["10", "11"]
    assert len(tx.run.await_args_list[2].kwargs["items"]) == 2
    assert stats["entities_enriched"] == 2
    assert stats["entities_skipped_no_discogs_match"] == 1


async def test_relationship_batch_groups_by_type_and_counts_outcomes() -> None:
    tx = AsyncMock()
    tx.run.return_value = Rows([{"matched": 3}], created=2)
    stats = dict.fromkeys(service.enrichment_stats, 0)
    relations = [
        ("1", {"type": "member of band", "target_discogs_artist_id": 2}),
        ("1", {"type": "member of band", "target_discogs_artist_id": 3}),
        ("1", {"type": "member of band", "target_discogs_artist_id": 4, "direction": "backward"}),
        ("1", {"type": "member of band", "target_discogs_artist_id": None}),
        ("1", {"type": "no such type", "target_discogs_artist_id": 9}),
    ]

    await _projections.create_relationship_edges_batch(tx, relations, stats)

    assert tx.run.await_count == 1
    assert tx.run.await_args.kwargs["rows"][2] == {"source_id": "4", "target_id": "1"}
    assert stats["relationships_created"] == 2
    assert stats["relationships_updated"] == 1
    assert stats["relationships_skipped_missing_side"] == 1


async def test_single_mode_still_writes_one_transaction_per_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service, "BATCH_MODE", False)
    session = neo4j_session()
    tx = AsyncMock()
    tx.run.return_value = Rows([{"matched_id": "1"}])
    session.execute_write.side_effect = execute_write_with(tx)
    deliveries = [Delivery({"id": f"mb{i}", "discogs_label_id": str(i)}) for i in (1, 2)]

    with patch.object(service, "graph", neo4j_driver(session=session)):
        await asyncio.gather(*(service.on_label_message(d) for d in deliveries))

    assert session.execute_write.await_count == 2


@pytest.mark.parametrize(("serialize", "expected_peak"), [(True, 1), (False, 5)])
async def test_serialize_flushes_limits_commits_in_flight(monkeypatch: pytest.MonkeyPatch, serialize: bool, expected_peak: int) -> None:
    """NEO4J_SERIALIZE_FLUSHES keeps one commit in flight per data type; off, flushes overlap."""
    monkeypatch.setattr(service, "BATCH_SIZE", 1)
    monkeypatch.setattr(service, "SERIALIZE_FLUSHES", serialize)
    in_flight = 0
    peak = 0

    async def execute(function: Any) -> Any:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        try:
            tx = AsyncMock()
            tx.run.return_value = Rows([{"matched_id": "0", "idx": 0, "changed": True}])
            return await function(tx)
        finally:
            in_flight -= 1

    session = neo4j_session()
    session.execute_write.side_effect = execute
    deliveries = [Delivery({"id": f"mb{i}", "discogs_label_id": str(i)}) for i in range(5)]

    with patch.object(service, "graph", neo4j_driver(session=session)):
        results = await asyncio.gather(*(service.on_label_message(d) for d in deliveries))

    assert all(r.settlement is Settlement.ACK for r in results)
    assert session.execute_write.await_count == 5
    assert peak == expected_peak


async def test_unchanged_replay_writes_no_relationships_or_media() -> None:
    tx = AsyncMock()
    tx.run.side_effect = [
        Rows([{"idx": 0, "changed": False}]),
        Rows([{"idx": 0, "changed": False}]),
    ]
    stats = dict.fromkeys(service.enrichment_stats, 0)
    artist = {"id": "a", "discogs_artist_id": 1, "relations": [{"type": "teacher", "target_discogs_artist_id": 2}]}
    release = {"id": "r", "discogs_release_id": 3, "media": {"items": [{"medium": "cd", "family": "optical", "qty": 1}]}}

    assert await _projections.enrich_artists_batch(tx, [artist], stats) == [True]
    assert await _projections.enrich_releases_batch(tx, [release], stats) == [True]

    assert tx.run.await_count == 2  # the gated SETs only: no relationship or media statements
    assert stats["replays_skipped_unchanged"] == 2
    assert stats["entities_enriched"] == 2


async def test_payload_hash_is_stable_and_sensitive() -> None:
    row = {"b": 1, "a": [1, 2]}
    assert _projections.payload_hash(row, None) == _projections.payload_hash({"a": [1, 2], "b": 1}, None)
    assert _projections.payload_hash(row, None) != _projections.payload_hash({**row, "b": 2}, None)
    assert _projections.payload_hash(row, [1]) != _projections.payload_hash(row, [2])


async def test_both_write_paths_compute_the_same_digest_for_every_entity() -> None:
    """A node written by one path must not be rewritten when the other replays it."""
    media = {"items": [{"medium": "cd", "family": "optical", "qty": 1}]}
    cases = [
        (_projections.enrich_artist, _projections.enrich_artists_batch, {"id": "a", "discogs_artist_id": 1, "relations": [{"type": "teacher"}]}),
        (_projections.enrich_label, _projections.enrich_labels_batch, {"id": "l", "discogs_label_id": 1, "label_code": 3}),
        (_projections.enrich_release_group, _projections.enrich_release_groups_batch, {"id": "g", "discogs_master_id": 1}),
        (_projections.enrich_release, _projections.enrich_releases_batch, {"id": "r", "discogs_release_id": 1, "media": media}),
    ]
    for single, batch, record in cases:
        single_tx, batch_tx = AsyncMock(), AsyncMock()
        single_tx.run.return_value = Rows([{"matched_id": "1", "changed": False}])
        batch_tx.run.return_value = Rows([{"idx": 0, "changed": False}])
        stats = dict.fromkeys(service.enrichment_stats, 0)
        await single(single_tx, record, stats)
        await batch(batch_tx, [record], stats)
        single_hash = single_tx.run.await_args_list[0].kwargs["mb_hash"]
        batch_hash = batch_tx.run.await_args_list[0].kwargs["rows"][0]["mb_hash"]
        assert single_hash == batch_hash, single.__name__
