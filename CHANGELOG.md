# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.4.0 (2026-10-31)

### Feat

- **health**: move health port to role port 8001 and read HEALTH_PORT

### Fix

- **deps**: upgrade msgpack past CVE-2026-114613
- **cli**: exit 1 when startup gives up so Docker restarts the service
- **health**: reject set-but-empty HEALTH_PORT
- **deps**: upgrade filelock to 4.2.1 past CVE-2026-112940
- **deps**: bump certifi and opentelemetry stack to fix pip-audit advisories
- **deps**: bump multidict to fix CVE-2026-104874 (#10)
- **deps**: bump multidict to fix CVE-2026-104874 flagged by pip-audit

## v0.3.1 (2026-10-04)

### Fix

- **consumers**: confirm bounded cancellation and rearm MusicBrainz runs
- **contracts**: promote persistence compatibility from database-schema@525cbc1
- **consumers**: detect silent stream starvation

## v0.3.0 (2026-09-15)

### Feat

- **graph**: project mb_country and mb_release_events onto matched releases
- **telemetry**: trace consumed messages and neo4j batch flushes

### Fix

- **deps**: bump python to 3.14.7-slim and dockerfile frontend to 1.27 (#1)
- **ci**: accept the no-bumpable-commits bump-preview state

### Refactor

- **delivery**: centralize MusicBrainz settlement
- extract MusicBrainz graph seams

## v0.2.0 (2026-09-04)

### Feat

- **graph**: reconcile MusicBrainz media onto matched releases
- **telemetry**: adopt common.telemetry and record pipeline metrics

### Fix

- **ci**: use public python libraries

## v0.1.1 (2026-08-31)

### Fix

- **ci**: accept release-boundary bump states and use Commitizen's supported files-only option

## v0.1.0 (2026-08-31)

The v0.1.0 workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.
