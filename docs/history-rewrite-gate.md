# History rewrite approval gate

History sanitation runs only in separate backup and sanitized clones. It removes raw private
planning paths from every reachable commit while the private `planning-archive` repository at
commit `4d0ecef0a798aab2f769cb5eb2e93982236f4f91` preserves the source records.

```mermaid
flowchart TD
    Candidate[Reviewed private candidate] --> Backup[Bundle and mirror backup]
    Backup --> Filter[Independent sanitized mirror]
    Filter --> Map[Commit, ref, and removed-object maps]
    Map --> Scan[Object-graph and secret scans]
    Scan --> Validate[Fresh sanitized checkout validation]
    Validate --> Gate{Explicit operator approval?}
    Gate -- No --> Stop[Private remote remains unchanged]
    Gate -- Yes --> Cutover[Force-with-lease private-remote cutover]
    Cutover --> Verify[Fresh-clone verification]
```

The operator must approve the exact old-to-new map, expected remote values, rollback backup,
and cutover before any remote rewrite. Remote drift invalidates the rehearsal. Repository
visibility, tags, releases, packages, and container publication are separate approvals.
