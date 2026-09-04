# MusicBrainz media conformance fixtures

Copied verbatim from `design/taxonomy/media/v1/fixtures/` in the GrooveMap design
repository, which owns the canonical media vocabulary described by ADR 0007. Only the
`musicbrainz-*` pairs are vendored here; the Discogs pairs belong to the Discogs services.

Each file holds a MusicBrainz release under `input` and the canonical media block the shared
mapper must produce under `expected`. `tests/test_release_media.py` proves the block this
service reads and writes against these pairs, so a vocabulary change that alters a medium id,
family, or label is caught here rather than in the graph.

Do not edit these files by hand. Re-copy them from the design repository when the vocabulary
version changes.
