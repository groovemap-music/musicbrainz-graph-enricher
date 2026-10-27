# Completion signals and health state

The `musicbrainz-ingestion` catalog-event contract carries two terminal control messages. They
coordinate consumer lifecycle; they are not graph records and do not run an enrichment
transaction.

## `file_complete`

A `file_complete` event names the entity stream and may include `total_processed`. The service:

1. logs the producer's processed count;
2. schedules cancellation after `CONSUMER_CANCEL_DELAY` when enabled;
3. records the stream in `completed_files`; and
4. acknowledges the event.

The completion flag is set after scheduling so stuck-state detection still observes work that is
in flight during the grace period.

## `extraction_complete`

An `extraction_complete` event is also terminal for the stream. The service records the stream as
complete even if a restart or consumer recovery removed an earlier `file_complete` marker, then
schedules cancellation and acknowledges the event. This prevents a completed stream from being
reported as permanently stuck.

Unlike other graph consumers, this service does not delete stub nodes on
`extraction_complete`; it only enriches already matched nodes.

```mermaid
stateDiagram-v2
    [*] --> Consuming
    Consuming --> Complete: file_complete or extraction_complete
    Complete --> GracePeriod: cancellation enabled
    Complete --> Idle: cancellation disabled
    GracePeriod --> Idle: consumer cancelled
    Idle --> Consuming: periodic check finds messages
    Consuming --> Draining: shutdown signal
    GracePeriod --> Draining: shutdown signal
    Draining --> [*]: consumers cancelled before connection close
```

## Health interpretation

The health payload contains:

- `status`: `starting`, `healthy`, or `unhealthy`;
- `service`: `musicbrainz-graph-enricher`;
- `message_counts` and `last_message_time` by entity stream;
- `active_consumers` and `completed_files`;
- `consumer_alarm_types`: incomplete streams whose consumer is missing or never delivered;
- `enrichment_stats`;
- `current_task`, including idle, startup, or stuck status; and
- `timestamp`.

A service with no Neo4j driver is `starting` before any work and `unhealthy` after activity. A
connected service is `unhealthy` while `consumer_alarm_types` is non-empty or a consumer recovery
is pending. An incomplete stream is alarmed when its consumer is missing after messages have
flowed or after the `STARTUP_IDLE_TIMEOUT` registration grace period, or when it never received a
delivery after every sibling stream completed; see
[stuck-state recovery](consumer-cancellation.md#stuck-state-recovery). Otherwise a connected
service is healthy, including a completed idle run.
