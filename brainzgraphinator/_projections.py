"""MusicBrainz-specific projections onto the shared catalog graph."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, Protocol

import orjson
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

# Medium is a vocabulary node shared with the Discogs enricher, so only a medium the stale sweep
# just left without any ISSUED_ON edge, from any catalog, is a candidate; and only when nothing
# but its own outgoing IN_FAMILY edge touches it. MediaFamily nodes are never pruned.
#
# The candidate is write-locked (a no-op property write) BEFORE the orphan check, so a concurrent
# transaction cannot attach an edge between the check and the delete: it waits for this one and
# then re-creates the medium, rather than having its new edge silently detached. The delete is
# plain DELETE of the node and its own IN_FAMILY edge, never DETACH, so an unexpected remaining
# relationship aborts the transaction and the delivery is retried instead of losing data.
PRUNE_ORPHAN_MEDIUM = (
    "WITH DISTINCT m "
    "SET m._prune_lock = true "
    "REMOVE m._prune_lock "
    "WITH m "
    "WHERE NOT (m)<-[:ISSUED_ON]-() "
    "  AND size([(m)-[other]-() WHERE type(other) <> 'IN_FAMILY' OR startNode(other) <> m | other]) = 0 "
    "OPTIONAL MATCH (m)-[family_edge:IN_FAMILY]->() "
    "DELETE family_edge, m"
)

RELEASE_MEDIA_SUMMARY_CYPHER = (
    "MATCH (r:Release {id: $discogs_id}) "
    "SET r.mb_media_families = $families, "
    "    r.mb_medium_count = $medium_count "
    "WITH r "
    "MATCH (r)-[stale:ISSUED_ON]->(m:Medium) "
    "WHERE stale.source = $source AND NOT m.id IN $medium_ids "
    "DELETE stale " + PRUNE_ORPHAN_MEDIUM
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


# ── Payload-hash replay gate ─────────────────────────────────────────────
#
# Every enrichment SET is gated on `mb_hash`, a digest of exactly what the event projects onto
# the graph (properties, supported relations, media). A replay of unchanged data matches the
# node, finds the same digest and writes nothing; `mb_updated_at` therefore records when the
# MusicBrainz data last actually changed, not when the message was last delivered. The events
# carry no source timestamp of their own, so the time of the change is the best available.
# A node enriched before the gate existed has no digest and is rewritten once.

HASH_VERSION = "v1"

# (graph property, row key). The row key is the parameter name of both statement shapes.
ARTIST_FIELDS = [
    (key, key)
    for key in (
        "mbid",
        "mb_type",
        "mb_gender",
        "mb_begin_date",
        "mb_end_date",
        "mb_area",
        "mb_begin_area",
        "mb_end_area",
        "mb_disambiguation",
    )
]
LABEL_FIELDS = [(key, key) for key in ("mbid", "mb_type", "mb_label_code", "mb_begin_date", "mb_end_date", "mb_area")]
RELEASE_FIELDS = [
    ("mbid", "mbid"),
    ("mb_barcode", "mb_barcode"),
    ("mb_status", "mb_status"),
    ("mb_release_group_mbid", "release_group_mbid"),
    ("mb_country", "mb_country"),
    ("mb_release_events", "mb_release_events"),
]
RELEASE_GROUP_FIELDS = [(key, key) for key in ("mbid", "mb_type", "mb_secondary_types", "mb_first_release_date", "mb_disambiguation")]


def payload_hash(*parts: Any) -> str:
    """Return a versioned, order-independent digest of the projected payload."""
    digest = hashlib.sha256(orjson.dumps(parts, option=orjson.OPT_SORT_KEYS, default=str)).hexdigest()
    return f"{HASH_VERSION}:{digest}"


def _gated_cypher(label: str, var: str, fields: list[tuple[str, str]], *, batch: bool) -> str:
    """Build the MATCH-then-conditional-SET statement for one entity kind.

    The SET runs only when the stored digest differs. The statement still returns the matched
    node (and whether it changed) so callers can tell "no Discogs match" from "unchanged".
    """
    source = "row." if batch else "$"
    match_id = "row.discogs_id" if batch else "$discogs_id"
    assignments = ", ".join(f"{var}.{prop} = {source}{key}" for prop, key in fields)
    prefix = "UNWIND $rows AS row " if batch else ""
    with_row = f"{var}, row" if batch else var
    returned = "row.idx AS idx" if batch else f"{var}.id AS matched_id"
    return (
        f"{prefix}MATCH ({var}:{label} {{id: {match_id}}}) "
        f"WITH {with_row}, ({var}.mb_hash IS NULL OR {var}.mb_hash <> {source}mb_hash) AS changed "
        "FOREACH (_ IN CASE WHEN changed THEN [1] ELSE [] END | "
        f"SET {assignments}, {var}.mb_hash = {source}mb_hash, {var}.mb_updated_at = $mb_updated_at) "
        f"RETURN {returned}, changed"
    )


def _record_outcome(matched: Any, stats: dict[str, int]) -> bool:
    """Count a single-record result and return whether the node was written."""
    if not matched:
        stats["entities_skipped_no_discogs_match"] += 1
        return False
    stats["entities_enriched"] += 1
    changed = bool(matched.get("changed", True))
    if not changed:
        stats["replays_skipped_unchanged"] = stats.get("replays_skipped_unchanged", 0) + 1
    return changed


def _media_digest(record: dict[str, Any]) -> Any:
    """Return what the release's media block projects, for the payload hash."""
    media = release_media_block(record)
    if media is None:
        return None
    items = [item for item in (media.get("items") or []) if isinstance(item, dict)]
    return [families_of(media), len(items), media_edge_rows(media)]


async def enrich_artist(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Artist and project its supported relationships."""
    discogs_id = record.get("discogs_artist_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    discogs_id = str(discogs_id)
    relations = record.get("relations") or []
    row = _artist_row(record)
    result = await tx.run(
        _gated_cypher("Artist", "a", ARTIST_FIELDS, batch=False),
        discogs_id=discogs_id,
        **row,
        mb_hash=payload_hash(row, relations),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    changed = _record_outcome(matched, stats)
    if relations and matched and changed:
        await create_relationship_edges(tx, discogs_id, relations, stats)
    return True


async def enrich_label(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Label with MusicBrainz metadata."""
    discogs_id = record.get("discogs_label_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    row = _label_row(record)
    result = await tx.run(
        _gated_cypher("Label", "l", LABEL_FIELDS, batch=False),
        discogs_id=str(discogs_id),
        **row,
        mb_hash=payload_hash(row, None),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    _record_outcome(await result.single(), stats)
    return True


def release_event_rows(release_events: Any) -> list[str]:
    """Collapse MusicBrainz release events into compact "date|area_name" rows.

    Null-safe: a missing/non-list `release_events` yields an empty list, and an
    entry missing both fields is skipped. An entry with only one of the two
    fields keeps the other side empty (e.g. "1969-09-26|" or "|United Kingdom").
    """
    rows = []
    for event in release_events or []:
        if not isinstance(event, dict):
            continue
        date = event.get("date")
        area_name = event.get("area_name")
        if date is None and area_name is None:
            continue
        rows.append(f"{date or ''}|{area_name or ''}")
    return rows


async def enrich_release(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Release and reconcile its canonical media projection."""
    discogs_id = record.get("discogs_release_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    discogs_id = str(discogs_id)
    row = _release_row(record)
    result = await tx.run(
        _gated_cypher("Release", "r", RELEASE_FIELDS, batch=False),
        discogs_id=discogs_id,
        **row,
        mb_hash=payload_hash(row, _media_digest(record)),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    matched = await result.single()
    if _record_outcome(matched, stats):
        await reconcile_release_media(tx, discogs_id, record)
    return True


async def enrich_release_group(tx: Any, record: dict[str, Any], stats: dict[str, int]) -> bool:
    """Enrich a matched Master with MusicBrainz release-group metadata."""
    discogs_id = record.get("discogs_master_id")
    if discogs_id is None:
        stats["entities_skipped_no_discogs_match"] += 1
        return True

    row = _release_group_row(record)
    result = await tx.run(
        _gated_cypher("Master", "m", RELEASE_GROUP_FIELDS, batch=False),
        discogs_id=str(discogs_id),
        **row,
        mb_hash=payload_hash(row, None),
        mb_updated_at=datetime.now(UTC).isoformat(),
    )
    _record_outcome(await result.single(), stats)
    return True


# ── Cross-message UNWIND batches ─────────────────────────────────────────
#
# One transaction and one statement per entity kind per flush, instead of one transaction per
# message. The per-record functions above stay as the isolation fallback: when a batch fails
# for a non-transient reason, the flusher replays its members one by one so a single poison
# record cannot fail its neighbours.

RELEASE_MEDIA_SUMMARY_BATCH_CYPHER = (
    "UNWIND $rows AS row "
    "MATCH (r:Release {id: row.discogs_id}) "
    "SET r.mb_media_families = row.families, "
    "    r.mb_medium_count = row.medium_count "
    "WITH r, row "
    "MATCH (r)-[stale:ISSUED_ON]->(m:Medium) "
    "WHERE stale.source = $source AND NOT m.id IN row.medium_ids "
    "DELETE stale " + PRUNE_ORPHAN_MEDIUM
)

RELEASE_MEDIA_EDGES_BATCH_CYPHER = (
    "UNWIND $items AS item "
    "MATCH (r:Release {id: item.discogs_id}) "
    "MERGE (m:Medium {id: item.medium}) "
    "    ON CREATE SET m.family = item.family, m.label = item.label "
    "MERGE (f:MediaFamily {name: item.family}) "
    "MERGE (m)-[:IN_FAMILY]->(f) "
    "MERGE (r)-[e:ISSUED_ON {source: $source}]->(m) "
    "SET e.qty = item.qty"
)


class BatchProjection(Protocol):
    """A transaction-scoped projection of many records; returns one matched flag per record."""

    async def __call__(self, tx: Any, records: list[dict[str, Any]], stats: dict[str, int]) -> list[bool]: ...


def _life_span(record: dict[str, Any], key: str) -> Any:
    return record.get(f"{key}_date", (record.get("life_span") or {}).get(key))


async def _run_entity_batch(
    tx: Any,
    spec: tuple[str, str, list[tuple[str, str]]],
    id_key: str,
    records: list[dict[str, Any]],
    build_row: Any,
    extra_hash: Any,
    stats: dict[str, int],
) -> tuple[list[bool], list[bool]]:
    """Write the changed records with one UNWIND; return per-record (matched, changed) flags."""
    label, var, fields = spec
    now = datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        discogs_id = record.get(id_key)
        if discogs_id is not None:
            row = build_row(record)
            rows.append({"idx": index, "discogs_id": str(discogs_id), **row, "mb_hash": payload_hash(row, extra_hash(record))})

    outcomes: dict[int, bool] = {}
    if rows:
        result = await tx.run(_gated_cypher(label, var, fields, batch=True), rows=rows, mb_updated_at=now)
        outcomes = {int(row["idx"]): bool(row["changed"]) async for row in result}

    matched = [index in outcomes for index in range(len(records))]
    changed = [outcomes.get(index, False) for index in range(len(records))]
    stats["entities_enriched"] += sum(matched)
    stats["entities_skipped_no_discogs_match"] += len(matched) - sum(matched)
    stats["replays_skipped_unchanged"] = stats.get("replays_skipped_unchanged", 0) + sum(m and not c for m, c in zip(matched, changed, strict=True))
    return matched, changed


def _artist_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "mbid": record.get("mbid", record.get("id")),
        "mb_type": record.get("mb_type", record.get("type")),
        "mb_gender": record.get("gender"),
        "mb_begin_date": _life_span(record, "begin"),
        "mb_end_date": _life_span(record, "end"),
        "mb_area": record.get("area"),
        "mb_begin_area": record.get("begin_area"),
        "mb_end_area": record.get("end_area"),
        "mb_disambiguation": record.get("disambiguation"),
    }


async def create_relationship_edges_batch(
    tx: Any,
    relations: list[tuple[str, dict[str, Any]]],
    stats: dict[str, int],
) -> None:
    """Project supported relationships for many source artists, one UNWIND per edge type."""
    by_type: dict[str, list[dict[str, str]]] = {}
    for source_discogs_id, relation in relations:
        edge_type = MB_RELATIONSHIP_MAP.get(relation.get("type", ""))
        if edge_type is None:
            continue
        target_discogs_id = relation.get("target_discogs_artist_id")
        if target_discogs_id is None:
            stats["relationships_skipped_missing_side"] += 1
            continue
        edge_source_id, edge_target_id = str(source_discogs_id), str(target_discogs_id)
        if relation.get("direction") == "backward":
            edge_source_id, edge_target_id = edge_target_id, edge_source_id
        by_type.setdefault(edge_type, []).append({"source_id": edge_source_id, "target_id": edge_target_id})

    for edge_type, rows in by_type.items():
        # Cypher relationship types cannot be parameters; edge_type is restricted to the map.
        result = await tx.run(
            "UNWIND $rows AS row "
            "MATCH (a:Artist {id: row.source_id}) MATCH (b:Artist {id: row.target_id}) "
            f"MERGE (a)-[r:{edge_type}]->(b) SET r.source = 'musicbrainz' "
            "RETURN count(*) AS matched",
            rows=rows,
        )
        record = await result.single()
        matched = int(record["matched"]) if record else 0
        summary = await result.consume()
        created = summary.counters.relationships_created
        stats["relationships_created"] += created
        stats["relationships_updated"] += max(matched - created, 0)
        stats["relationships_skipped_missing_side"] += len(rows) - matched


async def enrich_artists_batch(tx: Any, records: list[dict[str, Any]], stats: dict[str, int]) -> list[bool]:
    """Enrich many Artists and project their supported relationships in one transaction."""
    flags, changed = await _run_entity_batch(
        tx,
        ("Artist", "a", ARTIST_FIELDS),
        "discogs_artist_id",
        records,
        _artist_row,
        lambda record: record.get("relations") or [],
        stats,
    )
    # An unchanged replay already projected its relationships; only changed records rewrite them.
    relations = [
        (str(record["discogs_artist_id"]), relation)
        for record, was_changed in zip(records, changed, strict=True)
        if was_changed
        for relation in record.get("relations") or []
    ]
    if relations:
        await create_relationship_edges_batch(tx, relations, stats)
    return flags


def _label_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "mbid": record.get("mbid", record.get("id")),
        "mb_type": record.get("mb_type", record.get("type")),
        "mb_label_code": record.get("label_code"),
        "mb_begin_date": _life_span(record, "begin"),
        "mb_end_date": _life_span(record, "end"),
        "mb_area": record.get("area"),
    }


async def enrich_labels_batch(tx: Any, records: list[dict[str, Any]], stats: dict[str, int]) -> list[bool]:
    """Enrich many Labels in one transaction."""
    flags, _ = await _run_entity_batch(tx, ("Label", "l", LABEL_FIELDS), "discogs_label_id", records, _label_row, lambda _record: None, stats)
    return flags


def _release_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "mbid": record.get("mbid", record.get("id")),
        "mb_barcode": record.get("barcode"),
        "mb_status": record.get("status"),
        "release_group_mbid": record.get("release_group_mbid"),
        "mb_country": record.get("country"),
        "mb_release_events": release_event_rows(record.get("release_events")),
    }


async def enrich_releases_batch(tx: Any, records: list[dict[str, Any]], stats: dict[str, int]) -> list[bool]:
    """Enrich many Releases and reconcile their media projections in one transaction."""
    flags, changed = await _run_entity_batch(tx, ("Release", "r", RELEASE_FIELDS), "discogs_release_id", records, _release_row, _media_digest, stats)

    # A later message for the same release replaces an earlier one, as sequential delivery would.
    summaries: dict[str, dict[str, Any]] = {}
    edges: dict[str, list[dict[str, Any]]] = {}
    for record, was_changed in zip(records, changed, strict=True):
        if not was_changed:
            continue
        media = release_media_block(record)
        if media is None:
            continue
        discogs_id = str(record["discogs_release_id"])
        media_rows = media_edge_rows(media)
        items = [item for item in (media.get("items") or []) if isinstance(item, dict)]
        summaries[discogs_id] = {
            "discogs_id": discogs_id,
            "families": families_of(media),
            "medium_count": len(items),
            "medium_ids": [row["medium"] for row in media_rows],
        }
        edges[discogs_id] = [{"discogs_id": discogs_id, **row} for row in media_rows]

    if summaries:
        await tx.run(RELEASE_MEDIA_SUMMARY_BATCH_CYPHER, rows=list(summaries.values()), source=MEDIA_SOURCE)
        items_flat = [row for rows in edges.values() for row in rows]
        if items_flat:
            await tx.run(RELEASE_MEDIA_EDGES_BATCH_CYPHER, items=items_flat, source=MEDIA_SOURCE)
    return flags


def _release_group_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "mbid": record.get("mbid", record.get("id")),
        "mb_type": record.get("mb_type", record.get("type")),
        "mb_secondary_types": record.get("secondary_types", []),
        "mb_first_release_date": record.get("first_release_date"),
        "mb_disambiguation": record.get("disambiguation"),
    }


async def enrich_release_groups_batch(tx: Any, records: list[dict[str, Any]], stats: dict[str, int]) -> list[bool]:
    """Enrich many Masters with release-group metadata in one transaction."""
    flags, _ = await _run_entity_batch(
        tx,
        ("Master", "m", RELEASE_GROUP_FIELDS),
        "discogs_master_id",
        records,
        _release_group_row,
        lambda _record: None,
        stats,
    )
    return flags


BATCH_PROJECTIONS: dict[str, BatchProjection] = {
    "artists": enrich_artists_batch,
    "labels": enrich_labels_batch,
    "release-groups": enrich_release_groups_batch,
    "releases": enrich_releases_batch,
}
