# Release compliance

The repository gate is credential-free and does not contact a deployment. `just check`
verifies formatting, linting, types, tests and coverage, promoted catalog and schema contracts,
immutable automation, package construction and installation, MIT metadata, complete Git and
worktree secret scans, and version consistency. `just audit` adds the current network-backed
Python vulnerability audit.

The reusable CI graph also runs `just test-integration`, the explicit service-backed lane
defined by `contracts/integration-testing/v1/contract.json`. It needs no repository secret: the
runner starts pinned image
`neo4j:2026-community@sha256:dbc377fb9cd8fe8dabc19d3041b197d5ca0ef8bae514cea175b8df265e5b7a76`
with disposable local credentials, publishes only a random loopback Bolt port, and removes the
container after the tests. The default `just check` remains network- and service-free.

`just image` builds the repository-named `musicbrainz-graph-enricher:local` image, verifies the
installed service import, and checks its numeric non-root runtime identity. `just
release-dry-run` produces local checksums, an SBOM, third-party notices, and provenance without
creating a tag, upload, release, or repository setting.

The CI caller pins the organization Automation workflows to an immutable commit.
**Public-library cutover: complete.** The `python-libraries` dependency is public, pinned to an
immutable commit, and fetched without private-package credentials. Ordinary and
Dependabot-authored pull requests run the same required job graph. The CI caller retains the
supported explicit `CODECOV_TOKEN`, and the release caller retains the supported
`prepare-image-command`; neither caller inherits secrets. Releases remain tag-only, and no
Renovate workflow is active.

The first-party package is MIT licensed. Dependency rights and vulnerabilities are checked from
the Python 3.14 lock before release approval. Publication additionally requires a sanitized
reachable history, a reviewed green commit, successful hosted CI, and explicit operator
approval. Visibility, tags, packages, images, and releases remain separate gates.
