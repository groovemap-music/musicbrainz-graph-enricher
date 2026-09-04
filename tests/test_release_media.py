"""Tests for reconciling canonical MusicBrainz media (ADR 0007) onto matched releases."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika.abc import AbstractIncomingMessage
from orjson import dumps

import brainzgraphinator.brainzgraphinator as bgmod
from brainzgraphinator.brainzgraphinator import (
    MEDIA_SOURCE,
    enrich_release,
    media_edge_rows,
    on_release_message,
    reconcile_release_media,
    release_media_block,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "media" / "v1"

CLEAN_STATS = {
    "entities_enriched": 0,
    "entities_skipped_no_discogs_match": 0,
    "relationships_created": 0,
    "relationships_updated": 0,
    "relationships_skipped_missing_side": 0,
}


def load_fixture(name: str) -> dict[str, Any]:
    """Load one vendored conformance fixture by its file stem."""
    return json.loads((FIXTURE_DIR / f"{name}.json").read_text())


def fixture_names() -> list[str]:
    """Every vendored MusicBrainz fixture, so a new one is covered without editing a list."""
    return sorted(path.stem for path in FIXTURE_DIR.glob("musicbrainz-*.json"))


def release_event(name: str, *, discogs_release_id: int | None = 99999, canonical: bool = True) -> dict[str, Any]:
    """Build a releases event around a fixture.

    A canonical event carries the block the producer computed under `media` alongside the raw
    medium list under `media_raw`, exactly as the promoted contract describes it. A
    non-canonical event predates the block and carries only `media_raw`.
    """
    fixture = load_fixture(name)
    source = fixture["input"]
    event: dict[str, Any] = {
        "id": "a5d5abbc-fb46-427c-9e5f-8da2f0bdbb18",
        "mbid": "a5d5abbc-fb46-427c-9e5f-8da2f0bdbb18",
        "discogs_release_id": discogs_release_id,
        "barcode": "724384952051",
        "status": source.get("status"),
        "packaging": source.get("packaging"),
        "release_group": source.get("release_group"),
        "media_raw": source.get("media"),
    }
    if canonical:
        event["media"] = fixture["expected"]
    return event


def media_calls(tx: AsyncMock) -> list[tuple[str, dict[str, Any]]]:
    """Return the (cypher, parameters) pairs of every media query the release enrichment ran.

    The first tx.run is the release property SET that predates this feature; everything after
    it belongs to media reconciliation.
    """
    return [(call.args[0], call.kwargs) for call in tx.run.call_args_list[1:]]


def matching_tx() -> AsyncMock:
    """A transaction whose MATCH resolves the release, like conftest's mock_tx."""
    tx = AsyncMock()
    result = AsyncMock()
    result.single.return_value = {"matched_id": 99999}
    tx.run.return_value = result
    return tx


# ── Block resolution ──────────────────────────────────────────────────────


class TestReleaseMediaBlock:
    """Tests for release_media_block."""

    @pytest.mark.parametrize("name", fixture_names())
    def test_canonical_block_is_used_verbatim(self, name: str) -> None:
        """An event carrying `media` is trusted as-is; the mapper is not re-run."""
        event = release_event(name)
        assert release_media_block(event) is event["media"]

    @pytest.mark.parametrize("name", fixture_names())
    def test_legacy_event_derives_the_expected_block(self, name: str) -> None:
        """An event predating the block derives it from media_raw, matching the fixture."""
        block = release_media_block(release_event(name, canonical=False))
        assert block == load_fixture(name)["expected"]

    def test_event_with_neither_field_has_no_block(self) -> None:
        """No media and no media_raw means the release's media is unknown, not empty."""
        assert release_media_block({"discogs_release_id": 1, "mbid": "abc"}) is None

    def test_media_without_items_is_not_a_block(self) -> None:
        """A `media` value that is not a block shape falls through to the derive path."""
        event = release_event("musicbrainz-12-inch-vinyl", canonical=False)
        event["media"] = '12" Vinyl'
        assert release_media_block(event) == load_fixture("musicbrainz-12-inch-vinyl")["expected"]


# ── Edge rows ─────────────────────────────────────────────────────────────


class TestMediaEdgeRows:
    """Tests for media_edge_rows."""

    def test_one_row_per_medium_with_family_and_label(self) -> None:
        """Each row names the medium, its family, and its vocabulary label."""
        rows = media_edge_rows(load_fixture("musicbrainz-multi-medium-cd-dvd")["expected"])
        assert rows == [
            {"medium": "optical_sacd", "family": "optical", "label": "SACD", "qty": 1},
            {"medium": "video_dvd", "family": "video", "label": "DVD-Video", "qty": 1},
        ]

    def test_repeated_medium_collapses_and_sums_quantity(self) -> None:
        """A 2xLP is two source mediums but one edge carrying qty 2."""
        rows = media_edge_rows(load_fixture("musicbrainz-parent-vinyl-unresolved")["expected"])
        assert rows == [{"medium": "vinyl_unspecified", "family": "vinyl", "label": "Vinyl", "qty": 2}]

    def test_block_without_items_has_no_rows(self) -> None:
        """An unknown format produces no item, so it produces no edge."""
        assert media_edge_rows(load_fixture("musicbrainz-unknown-format")["expected"]) == []

    def test_items_missing_ids_are_dropped(self) -> None:
        """An item that cannot name its medium or family cannot become an edge."""
        block = {"items": [{"qty": 1}, {"medium": "vinyl_12"}, {"family": "vinyl"}, "not-a-mapping"]}
        assert media_edge_rows(block) == []

    def test_unknown_medium_id_is_labelled_by_its_id(self) -> None:
        """A medium the vendored vocabulary lacks keeps its edge and is named after its id."""
        block = {"items": [{"medium": "quantum_crystal", "family": "other", "qty": 1}]}
        assert media_edge_rows(block) == [{"medium": "quantum_crystal", "family": "other", "label": "quantum_crystal", "qty": 1}]

    def test_absent_or_invalid_quantity_defaults_to_one(self) -> None:
        """Every medium counts at least once, whatever the event says."""
        block = {"items": [{"medium": "vinyl_12", "family": "vinyl"}, {"medium": "optical_cd", "family": "optical", "qty": 0}]}
        assert [row["qty"] for row in media_edge_rows(block)] == [1, 1]


# ── Reconciliation onto a matched release ────────────────────────────────


class TestMatchedReleaseGainsMedia:
    """Tests for the media a matched release gains."""

    @pytest.mark.asyncio
    async def test_summary_properties_are_written(self) -> None:
        """A matched release carries its MusicBrainz families and medium count."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-multi-medium-cd-dvd"))

        cypher, parameters = media_calls(tx)[0]
        assert "SET r.mb_media_families = $families" in cypher
        assert "r.mb_medium_count = $medium_count" in cypher
        assert parameters["families"] == ["optical", "video"]
        assert parameters["medium_count"] == 2

    @pytest.mark.asyncio
    async def test_medium_count_counts_source_mediums_not_edges(self) -> None:
        """Two vinyl mediums behind one edge still count as two mediums."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-parent-vinyl-unresolved"))

        summary_parameters = media_calls(tx)[0][1]
        edge_parameters = media_calls(tx)[1][1]
        assert summary_parameters["medium_count"] == 2
        assert [item["qty"] for item in edge_parameters["items"]] == [2]

    @pytest.mark.asyncio
    async def test_medium_family_and_edge_are_merged(self) -> None:
        """The edge query merges the Medium, its MediaFamily, and the ISSUED_ON edge."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-12-inch-vinyl"))

        cypher, parameters = media_calls(tx)[1]
        assert "MERGE (m:Medium {id: item.medium})" in cypher
        assert "ON CREATE SET m.family = item.family, m.label = item.label" in cypher
        assert "MERGE (f:MediaFamily {name: item.family})" in cypher
        assert "MERGE (m)-[:IN_FAMILY]->(f)" in cypher
        assert "SET e.qty = item.qty" in cypher
        assert parameters["items"] == [{"medium": "vinyl_12", "family": "vinyl", "label": '12" vinyl', "qty": 1}]
        assert parameters["discogs_id"] == "99999"

    @pytest.mark.asyncio
    async def test_every_fixture_reaches_the_graph_as_its_block_describes(self) -> None:
        """Each fixture's families and media survive the trip into the emitted parameters."""
        for name in fixture_names():
            tx = matching_tx()
            expected = load_fixture(name)["expected"]
            with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
                await enrich_release(tx, release_event(name))

            calls = media_calls(tx)
            assert calls[0][1]["families"] == expected["families"], name
            emitted = [item["medium"] for item in calls[1][1]["items"]] if len(calls) > 1 else []
            assert emitted == sorted(set(emitted), key=emitted.index), name
            assert set(emitted) == {item["medium"] for item in expected["items"]}, name

    @pytest.mark.asyncio
    async def test_legacy_event_gains_the_same_media(self) -> None:
        """An event predating the block reaches the graph with the derived media."""
        canonical_tx = matching_tx()
        legacy_tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(canonical_tx, release_event("musicbrainz-digital-media"))
            await enrich_release(legacy_tx, release_event("musicbrainz-digital-media", canonical=False))

        assert media_calls(legacy_tx) == media_calls(canonical_tx)


# ── Discogs media is never touched ───────────────────────────────────────


class TestDiscogsMediaIsUntouched:
    """Tests that this enricher only ever reads and writes its own edges."""

    @pytest.mark.asyncio
    async def test_the_edge_merge_is_keyed_on_this_source(self) -> None:
        """MERGE matches on source, so a Discogs edge to the same medium is a different edge."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-12-inch-vinyl"))

        cypher, parameters = media_calls(tx)[1]
        assert "MERGE (r)-[e:ISSUED_ON {source: $source}]->(m)" in cypher
        assert "MERGE (r)-[e:ISSUED_ON]->(m)" not in cypher
        assert "SET e.source" not in cypher
        assert parameters["source"] == MEDIA_SOURCE == "musicbrainz"

    @pytest.mark.asyncio
    async def test_the_stale_sweep_is_scoped_to_this_source(self) -> None:
        """DELETE only ever considers edges this service wrote."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-12-inch-vinyl"))

        cypher, parameters = media_calls(tx)[0]
        assert "WHERE stale.source = $source AND NOT m.id IN $medium_ids" in cypher
        assert "DELETE stale" in cypher
        assert parameters["source"] == "musicbrainz"

    @pytest.mark.asyncio
    async def test_no_media_query_writes_a_discogs_property(self) -> None:
        """Nothing this enricher emits names the Discogs media summary."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-multi-medium-cd-dvd"))

        for cypher, _ in media_calls(tx):
            assert "media_families" not in cypher.replace("mb_media_families", "")
            assert "DETACH DELETE" not in cypher


# ── Stale edge cleanup ───────────────────────────────────────────────────


class TestStaleEdgeCleanup:
    """Tests for removing this source's edges when a release's media changes."""

    @pytest.mark.asyncio
    async def test_medium_ids_name_the_surviving_edges(self) -> None:
        """The sweep spares exactly the media the new block lists."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-multi-medium-cd-dvd"))

        assert media_calls(tx)[0][1]["medium_ids"] == ["optical_sacd", "video_dvd"]

    @pytest.mark.asyncio
    async def test_changed_media_leaves_the_previous_medium_unprotected(self) -> None:
        """Re-issuing a release on a different medium drops the edge to the old one."""
        first = matching_tx()
        second = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(first, release_event("musicbrainz-12-inch-vinyl"))
            await enrich_release(second, release_event("musicbrainz-digital-media"))

        assert media_calls(first)[0][1]["medium_ids"] == ["vinyl_12"]
        assert "vinyl_12" not in media_calls(second)[0][1]["medium_ids"]

    @pytest.mark.asyncio
    async def test_block_with_no_items_clears_the_release(self) -> None:
        """An authoritative empty block removes every edge and empties the summary."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-unknown-format"))

        calls = media_calls(tx)
        assert len(calls) == 1
        assert calls[0][1] == {
            "discogs_id": "99999",
            "families": [],
            "medium_count": 0,
            "medium_ids": [],
            "source": "musicbrainz",
        }


# ── Idempotency ──────────────────────────────────────────────────────────


class TestIdempotency:
    """Tests that a redelivered event is a no-op against the graph."""

    @pytest.mark.asyncio
    async def test_replaying_the_same_event_emits_the_same_queries(self) -> None:
        """Every media query is a MERGE or a scoped DELETE, so a replay converges."""
        first = matching_tx()
        second = matching_tx()
        event = release_event("musicbrainz-multi-medium-cd-dvd")
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(first, event)
            await enrich_release(second, event)

        assert media_calls(second) == media_calls(first)

    @pytest.mark.asyncio
    async def test_media_queries_never_create_a_node_outright(self) -> None:
        """No CREATE anywhere: a second delivery cannot duplicate a Medium or an edge."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            await enrich_release(tx, release_event("musicbrainz-multi-medium-cd-dvd"))

        for cypher, _ in media_calls(tx):
            assert "CREATE (" not in cypher
            assert cypher.startswith("MATCH (r:Release {id: $discogs_id})")


# ── Releases with no Discogs match ───────────────────────────────────────


class TestUnmatchedReleasesAreSkipped:
    """Tests that the no-Discogs-identifier rule is unchanged."""

    @pytest.mark.asyncio
    async def test_release_without_discogs_id_writes_nothing(self) -> None:
        """A release with no Discogs identifier still runs no query at all."""
        tx = matching_tx()
        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            result = await enrich_release(tx, release_event("musicbrainz-12-inch-vinyl", discogs_release_id=None))

            assert result is True
            tx.run.assert_not_called()
            assert bgmod.enrichment_stats["entities_skipped_no_discogs_match"] == 1
            assert bgmod.enrichment_stats["entities_enriched"] == 0

    @pytest.mark.asyncio
    async def test_release_with_no_graph_node_writes_no_media(self) -> None:
        """A release whose node does not exist is counted as skipped and gains no media."""
        tx = AsyncMock()
        result = AsyncMock()
        result.single = AsyncMock(return_value=None)
        tx.run = AsyncMock(return_value=result)

        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            assert await enrich_release(tx, release_event("musicbrainz-12-inch-vinyl")) is True

            assert media_calls(tx) == []
            assert bgmod.enrichment_stats["entities_skipped_no_discogs_match"] == 1
            assert bgmod.enrichment_stats["entities_enriched"] == 0

    @pytest.mark.asyncio
    async def test_event_without_media_leaves_media_untouched(self) -> None:
        """A matched release whose event carries no media keeps the media it already has."""
        tx = matching_tx()
        event = release_event("musicbrainz-12-inch-vinyl", canonical=False)
        del event["media_raw"]

        with patch.dict(bgmod.enrichment_stats, CLEAN_STATS):
            assert await enrich_release(tx, event) is True

            assert media_calls(tx) == []
            assert bgmod.enrichment_stats["entities_enriched"] == 1

    @pytest.mark.asyncio
    async def test_reconcile_reports_whether_it_had_media(self) -> None:
        """reconcile_release_media answers False when the event carried nothing to write."""
        tx = matching_tx()
        assert await reconcile_release_media(tx, "99999", {"mbid": "abc"}) is False
        assert await reconcile_release_media(tx, "99999", release_event("musicbrainz-digital-media")) is True


# ── Delivery behaviour ───────────────────────────────────────────────────


class TestDeliveryBehaviourUnchanged:
    """Tests that media reconciliation did not change how deliveries are settled."""

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_release_with_media_is_acked(self, mock_neo4j_driver: MagicMock) -> None:
        """A release event carrying media is processed and acked exactly as before."""
        message = AsyncMock(spec=AbstractIncomingMessage)
        message.body = dumps(release_event("musicbrainz-multi-medium-cd-dvd"))
        session = await mock_neo4j_driver.session(database="neo4j").__aenter__()

        async def run_transaction(function: Any) -> Any:
            return await function(matching_tx())

        session.execute_write.side_effect = run_transaction

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await on_release_message(message)

        message.ack.assert_called_once()
        message.nack.assert_not_called()

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_release_missing_id_still_dead_letters(self, mock_neo4j_driver: MagicMock) -> None:
        """A malformed release event is nacked without requeue, media or not."""
        message = AsyncMock(spec=AbstractIncomingMessage)
        event = release_event("musicbrainz-12-inch-vinyl")
        del event["id"]
        message.body = dumps(event)

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await on_release_message(message)

        message.nack.assert_called_once_with(requeue=False)
        message.ack.assert_not_called()

    @pytest.mark.asyncio
    @patch("brainzgraphinator.brainzgraphinator.shutdown_requested", False)
    async def test_a_failing_media_query_requeues_rather_than_dead_letters(self, mock_neo4j_driver: MagicMock) -> None:
        """A media write that fails is retried, not discarded: the delivery is requeued."""
        message = AsyncMock(spec=AbstractIncomingMessage)
        message.body = dumps(release_event("musicbrainz-12-inch-vinyl"))
        session = await mock_neo4j_driver.session(database="neo4j").__aenter__()
        session.execute_write.side_effect = RuntimeError("media write failed")

        with patch("brainzgraphinator.brainzgraphinator.graph", mock_neo4j_driver):
            await on_release_message(message)

        message.nack.assert_called_once_with(requeue=True)
        message.ack.assert_not_called()
