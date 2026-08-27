# History-preserving extraction

The source was migration branch `wt/bead/issue/discogsography-2kpm.16` at
`69d90758` in the unchanged monorepo. A disposable clone retained
`brainzgraphinator/`, `tests/brainzgraphinator/`, the applicable MusicBrainz/resilience
design documents, and `LICENSE`; the owned tests were promoted to `tests/`.

The exact `git filter-repo` arguments were:

```text
--path brainzgraphinator/
--path tests/brainzgraphinator/
--path LICENSE
--path docs/consumer-cancellation.md
--path docs/database-resilience.md
--path docs/file-completion-tracking.md
--path docs/musicbrainz-sync.md
--path docs/superpowers/plans/2026-05-21-neo4j-bolt-tls.md
--path docs/superpowers/specs/2026-05-21-neo4j-bolt-tls-design.md
--path-rename tests/brainzgraphinator/:tests/
```

The filter retained 68 relevant commits and no tags. The current tree is MIT licensed by
owner decision; earlier license revisions remain in history. The original monorepo and its
refs were not rewritten or deleted.
