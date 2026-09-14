# Integration testing

Run the real-database delivery checks with:

```bash
just test-integration
```

The recipe starts a unique disposable Neo4j container from the immutable image
`neo4j:2026-community@sha256:dbc377fb9cd8fe8dabc19d3041b197d5ca0ef8bae514cea175b8df265e5b7a76`.
It publishes Bolt on a random loopback-only port, uses a test-local password, waits for
`cypher-shell` readiness, and removes the container even when a test fails. No deployed
database, secret, or operator credential is read.

The lane exercises behavior mocks cannot prove:

- the MusicBrainz projection commits in Neo4j before `common.run_delivery` calls ACK;
- an actual Neo4j driver connection failure is classified as transient, throttled once, and
  settled as one broker requeue;
- deterministic validation failures reject once without modifying the graph.

`just check` explicitly excludes the `integration` marker so local and required unit checks stay
credential-free and never start infrastructure. The reusable CI workflow invokes the integration
recipe separately. `contracts/integration-testing/v1/contract.json` is the machine-readable
source for the engine, pinned image, network boundary, command, and behavioral coverage.
