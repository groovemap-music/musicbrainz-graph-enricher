"""MusicBrainz-specific projections onto the shared catalog graph."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from common.media import families_of, map_musicbrainz_release, medium_label


class Projection(Protocol):
    """A transaction-scoped graph projection used by the delivery lifecycle."""

    async def __call__(self, tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool: ...


MB_RELATIONSHIP_MAP: dict[str, str] = {
    "member of band": "MEMBER_OF",
    "collaboration": "COLLABORATED_WITH",
    "teacher": "TAUGHT",
    "tribute": "TRIBUTE_TO",
    "founder": "FOUNDED",
    "supporting musician": "SUPPORTED",
    "subgroup": "SUBGROUP_OF",
    "artist rename": "RENAMED_TO",
}

MEDIA_SOURCE = "musicbrainz"

RELEASE_MEDIA_SUMMARY_CYPHER = (
    "MATCH (r:Release {id: $discogs_id}) "
    "SET r.mb_media_families = $families, "
    "    r.mb_medium_count = $medium_count "
    "WITH r "
    "MATCH (r)-[stale:ISSUED_ON]->(m:Medium) "
    "WHERE stale.source = $source AND NOT m.id IN $medium_ids "
    "DELETE stale"
)

RELEASE_MEDIA_EDGES_CYPHER = (
    "MATCH (r:Release {id: $discogs_id}) "
    "UNWIND $items AS item "
    "MERGE (m:Medium {id: item.medium}) "
    "    ON CREATE SET m.family = item.family, m.label = item.label "
    "MERGE (f:MediaFamily {name: item.family}) "
    "MERGE (m)-[:IN_FAMILY]->(f) "
    "MERGE (r)-[e:ISSUED_ON {source: $source}]->(m) "
    "SET e.qty = item.qty"
)


def release_media_block(record: dict[str, Any]) -> dict[str, Any] | None:
    """Return canonical release media, deriving legacy raw events when needed."""
    media = record.get("media")
    if isinstance(media, dict) and isinstance(media.get("items"), list):
        return media

    media_raw = record.get("media_raw")
    if isinstance(media_raw, list):
        return map_musicbrainz_release({**record, "media": media_raw})

    return None


def media_edge_rows(media: dict[str, Any]) -> list[dict[str, Any]]:
    """Collapse media items into one deterministic row per canonical medium."""
    rows: dict[str, dict[str, Any]] = {}
    for item in media.get("items") or []:
        if not isinstance(item, dict):
            continue
        medium_id = item.get("medium")
        family = item.get("family")
        if not isinstance(medium_id, str) or not isinstance(family, str):
            continue
        quantity = item.get("qty")
        quantity = quantity if isinstance(quantity, int) and not isinstance(quantity, bool) and quantity >= 1 else 1
        row = rows.get(medium_id)
        if row is None:
            rows[medium_id] = {
                "medium": medium_id,
                "family": family,
                "label": _medium_label(medium_id),
                "qty": quantity,
            }
        else:
            row["qty"] += quantity
    return list(rows.values())


def _medium_label(medium_id: str) -> str:
    try:
        return medium_label(medium_id)
    except KeyError:
        return medium_id


async def reconcile_release_media(tx: Any, discogs_id: str, record: dict[str, Any]) -> bool:
    """Replace this source's media summary and edges when media is present."""
    media = release_media_block(record)
    if media is None:
        return False

    rows = media_edge_rows(media)
    items = [item for item in (media.get("items") or []) if isinstance(item, dict)]
    await tx.run(
        RELEASE_MEDIA_SUMMARY_CYPHER,
        discogs_id=discogs_id,
        families=families_of(media),
        medium_count=len(items),
        medium_ids=[row["medium"] for row in rows],
        source=MEDIA_SOURCE,
    )
    if rows:
        await tx.run(
            RELEASE_MEDIA_EDGES_CYPHER,
            discogs_id=discogs_id,
            items=rows,
            source=MEDIA_SOURCE,
        )
    return True


async def create_relationship_edges(
    tx: Any,
    source_discogs_id: str,
    relations: list[dict[str, Any]],
    stats: dict[str, int],
) -> None:
    """Project supported MusicBrainz artist relationships."""
    for relation in relations:
        edge_type = MB_RELATIONSHIP_MAP.get(relation.get("type", ""))
        if edge_type is None:
            continue

        target_discogs_id = relation.get("target_discogs_artist_id")
        if target_discogs_id is None:
            stats["relationships_skipped_missing_side"] += 1
            continue

        edge_source_id = str(source_discogs_id)
        edge_target_id = str(target_discogs_id)
        if relation.get("direction") == "backward":
            edge_source_id, edge_target_id = edge_target_id, edge_source_id

        # Cypher relationship types cannot be parameters; edge_type is restricted to the map.
        result = await tx.run(
            f"MATCH (a:Artist {{id: $source_id}}) MATCH (b:Artist {{id: $target_id}}) MERGE (a)-[r:{edge_type}]->(b) SET r.source = 'musicbrainz'",
            source_id=edge_source_id,
            target_id=edge_target_id,
        )
        summary = await result.consume()
        if summary.counters.relationships_created > 0:
            stats["relationships_created"] += 1
        elif summary.counters.contains_updates:
            stats["relationships_updated"] = stats.get("relationships_updated", 0) + 1
        else:
            stats["relationships_skipped_missing_side"] += 1


async def enrich_artist(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Artist and project its supported relationships."""
    discogs_id = record.get("discogs_artist_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    discogs_id = str(discogs_id)
    result = await tx.run(
        "MATCH (a:Artist {id: $discogs_id}) "
        "SET a.mbid = $mbid, "
        "    a.mb_type = $mb_type, "
        "    a.mb_gender = $mb_gender, "
        "    a.mb_begin_date = $mb_begin_date, "
        "    a.mb_end_date = $mb_end_date, "
        "    a.mb_area = $mb_area, "
        "    a.mb_begin_area = $mb_begin_area, "
        "    a.mb_end_area = $mb_end_area, "
        "    a.mb_disambiguation = $mb_disambiguation, "
        "    a.mb_updated_at = $mb_updated_at "
        "RETURN a.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_type=record.get("mb_type", record.get("type")),
        mb_gender=record.get("gender"),
        mb_begin_date=record.get("begin_date", (record.get("life_span") or {}).get("begin")),
        mb_end_date=record.get("end_date", (record.get("life_span") or {}).get("end")),
        mb_area=record.get("area"),
        mb_begin_area=record.get("begin_area"),
        mb_end_area=record.get("end_area"),
        mb_disambiguation=record.get("disambiguation"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    if matched:
        stats["entities_enriched"] += 1
    else:
        stats["entities_skipped_no_discogs_match"] += 1

    relations = record.get("relations", [])
    if relations and matched:
        await create_relationship_edges(tx, discogs_id, relations, stats)
    return True


async def enrich_label(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Label with MusicBrainz metadata."""
    discogs_id = record.get("discogs_label_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    discogs_id = str(discogs_id)
    result = await tx.run(
        "MATCH (l:Label {id: $discogs_id}) "
        "SET l.mbid = $mbid, "
        "    l.mb_type = $mb_type, "
        "    l.mb_label_code = $mb_label_code, "
        "    l.mb_begin_date = $mb_begin_date, "
        "    l.mb_end_date = $mb_end_date, "
        "    l.mb_area = $mb_area, "
        "    l.mb_updated_at = $mb_updated_at "
        "RETURN l.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_type=record.get("mb_type", record.get("type")),
        mb_label_code=record.get("label_code"),
        mb_begin_date=record.get("begin_date", (record.get("life_span") or {}).get("begin")),
        mb_end_date=record.get("end_date", (record.get("life_span") or {}).get("end")),
        mb_area=record.get("area"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    stats["entities_enriched" if matched else "entities_skipped_no_discogs_match"] += 1
    return True


async def enrich_release(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Release and reconcile its canonical media projection."""
    discogs_id = record.get("discogs_release_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    discogs_id = str(discogs_id)
    result = await tx.run(
        "MATCH (r:Release {id: $discogs_id}) "
        "SET r.mbid = $mbid, "
        "    r.mb_barcode = $mb_barcode, "
        "    r.mb_status = $mb_status, "
        "    r.mb_release_group_mbid = $release_group_mbid, "
        "    r.mb_updated_at = $mb_updated_at "
        "RETURN r.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_barcode=record.get("barcode"),
        mb_status=record.get("status"),
        release_group_mbid=record.get("release_group_mbid"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    if matched:
        stats["entities_enriched"] += 1
        await reconcile_release_media(tx, discogs_id, record)
    else:
        stats["entities_skipped_no_discogs_match"] += 1
    return True


async def enrich_release_group(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Master with MusicBrainz release-group metadata."""
    discogs_id = record.get("discogs_master_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    discogs_id = str(discogs_id)
    result = await tx.run(
        "MATCH (m:Master {id: $discogs_id}) "
        "SET m.mbid = $mbid, "
        "    m.mb_type = $mb_type, "
        "    m.mb_secondary_types = $mb_secondary_types, "
        "    m.mb_first_release_date = $mb_first_release_date, "
        "    m.mb_disambiguation = $mb_disambiguation, "
        "    m.mb_updated_at = $mb_updated_at "
        "RETURN m.id AS matched_id",
        discogs_id=discogs_id,
        mbid=record.get("mbid", record.get("id")),
        mb_type=record.get("mb_type", record.get("type")),
        mb_secondary_types=record.get("secondary_types", []),
        mb_first_release_date=record.get("first_release_date"),
        mb_disambiguation=record.get("disambiguation"),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    stats["entities_enriched" if matched else "entities_skipped_no_discogs_match"] += 1
    return True
